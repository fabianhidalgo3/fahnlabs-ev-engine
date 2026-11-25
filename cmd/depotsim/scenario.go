package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/fhidalgo/emob/motor/internal/engine"
)

// Scenario is one declared simulation input (a JSON file in scenarios/).
//
// Clock times are "HH:MM". A time earlier than Start belongs to the next day (overnight depot).
type Scenario struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Start       string `json:"start"` // clock time of minute 0 of the simulation

	SiteLimitKW float64    `json:"site_limit_kw"`
	MarginKW    float64    `json:"margin_kw"` // reserve subtracted from the available power (0 = none)
	AuxKW       float64    `json:"aux_kw"`    // constant auxiliary (building) load
	AuxProfile  []AuxPoint `json:"aux_profile,omitempty"`

	Chargers        int     `json:"chargers"`
	ChargerKW       float64 `json:"charger_kw"`
	ReleaseWhenFull bool    `json:"release_when_full"` // a bus at target SoC frees its charger for a waiting bus

	Efficiency   float64        `json:"efficiency"`    // η
	IntervalMin  int            `json:"interval_min"`  // Δt
	PeriodicOnly bool           `json:"periodic_only"` // true: setpoints only every Δt (a new bus waits for the next instant)
	Taper        Taper          `json:"taper"`
	Weights      engine.Weights `json:"weights"`

	DemandChargeCLPPerKWMonth float64 `json:"demand_charge_clp_per_kw_month"` // placeholder tariff
	UpgradeCLPPerKW           float64 `json:"connection_upgrade_clp_per_kw"`  // placeholder investment

	Seed      uint64     `json:"seed"`
	Buses     []Bus      `json:"buses,omitempty"`
	Generator *Generator `json:"generator,omitempty"`
	Sweeps    *Sweeps    `json:"sweeps,omitempty"`
}

// AuxPoint: from this clock time on, the auxiliary load is KW.
type AuxPoint struct {
	From string  `json:"from"`
	KW   float64 `json:"kw"`
}

// Taper is the battery acceptance curve: constant MaxKW up to TransitionSoC, then a linear reduction down to
// EndFraction × MaxKW at 100 % SoC.
type Taper struct {
	TransitionSoC float64 `json:"transition_soc"`
	EndFraction   float64 `json:"end_fraction"`
}

// Bus is one vehicle of the fleet.
type Bus struct {
	ID         string  `json:"id"`
	BatteryKWh float64 `json:"battery_kwh"`
	MaxKW      float64 `json:"max_kw"`
	Arrival    string  `json:"arrival"`
	Departure  string  `json:"departure"`
	ArrivalSoC float64 `json:"arrival_soc"`
	TargetSoC  float64 `json:"target_soc"`
	MinSoC     float64 `json:"min_soc"`
	Importance int     `json:"importance"`
}

// Generator draws a fleet with seeded uniform random values inside declared ranges.
type Generator struct {
	N             int        `json:"n"`
	BatteryKWh    []float64  `json:"battery_kwh"` // chosen uniformly from this list
	MaxKW         float64    `json:"max_kw"`
	ArrivalFrom   string     `json:"arrival_from"`
	ArrivalTo     string     `json:"arrival_to"`
	DepartureFrom string     `json:"departure_from"`
	DepartureTo   string     `json:"departure_to"`
	ArrivalSoC    [2]float64 `json:"arrival_soc"`
	TargetSoC     float64    `json:"target_soc"`
	MinSoC        float64    `json:"min_soc"`
	Importance    [2]int     `json:"importance"`
	StepMin       int        `json:"step_min"` // times rounded to this step (default 5)
}

// Sweeps declares the sensitivity runs made from this scenario.
type Sweeps struct {
	LimitKW        []float64 `json:"limit_kw,omitempty"`
	FleetSizes     []int     `json:"fleet_sizes,omitempty"` // needs a generator; chargers = fleet size
	UrgencyWeights []float64 `json:"urgency_weights,omitempty"`
	DepartureShift []int     `json:"departure_shift_min,omitempty"`
	IntervalMin    []int     `json:"interval_min,omitempty"`
}

// LoadScenario reads and validates a scenario file, filling defaults and generating the fleet if needed.
func LoadScenario(path string) (*Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Scenario
	b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf")) // tolerate a UTF-8 BOM (Windows editors)
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := s.Prepare(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &s, nil
}

// Prepare fills defaults, generates the fleet when a generator is declared and validates the scenario.
func (s *Scenario) Prepare() error {
	if s.Start == "" {
		s.Start = "20:00"
	}
	if s.Efficiency <= 0 || s.Efficiency > 1 {
		s.Efficiency = 0.95
	}
	if s.IntervalMin <= 0 {
		s.IntervalMin = 10
	}
	if s.Taper.TransitionSoC <= 0 {
		s.Taper = Taper{TransitionSoC: 80, EndFraction: 0.2}
	}
	if s.Weights == (engine.Weights{}) {
		s.Weights = engine.DefaultParams.Weights
	}
	if len(s.Buses) == 0 && s.Generator != nil {
		buses, err := s.Generator.Generate(s.Seed, s.Start)
		if err != nil {
			return err
		}
		s.Buses = buses
	}
	if s.Chargers <= 0 {
		s.Chargers = len(s.Buses)
	}
	if s.ChargerKW <= 0 || s.SiteLimitKW <= 0 || len(s.Buses) == 0 {
		return fmt.Errorf("charger_kw, site_limit_kw and buses are required")
	}
	if _, err := parseClock(s.Start); err != nil {
		return err
	}
	for _, a := range s.AuxProfile {
		if _, err := parseClock(a.From); err != nil {
			return err
		}
	}
	for i, b := range s.Buses {
		if b.BatteryKWh <= 0 || b.MaxKW <= 0 {
			return fmt.Errorf("bus %s: battery_kwh and max_kw must be > 0", b.ID)
		}
		arr, err1 := s.minute(b.Arrival)
		dep, err2 := s.minute(b.Departure)
		if err1 != nil || err2 != nil || dep <= arr {
			return fmt.Errorf("bus %s: invalid arrival/departure", b.ID)
		}
		if b.ID == "" {
			s.Buses[i].ID = fmt.Sprintf("B%02d", i+1)
		}
	}
	return nil
}

func parseClock(c string) (int, error) {
	h, m, ok := strings.Cut(strings.TrimSpace(c), ":")
	hh, e1 := strconv.Atoi(h)
	mm, e2 := strconv.Atoi(m)
	if !ok || e1 != nil || e2 != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 {
		return 0, fmt.Errorf("invalid clock time %q (want HH:MM)", c)
	}
	return hh*60 + mm, nil
}

// clockToMinute converts a clock time to minutes since start (next day when earlier than start).
func clockToMinute(start, c string) (int, error) {
	s, err := parseClock(start)
	if err != nil {
		return 0, err
	}
	t, err := parseClock(c)
	if err != nil {
		return 0, err
	}
	if t < s {
		t += 24 * 60
	}
	return t - s, nil
}

func (s *Scenario) minute(c string) (int, error) { return clockToMinute(s.Start, c) }

// clock formats minutes since start as HH:MM.
func (s *Scenario) clock(min int) string {
	st, _ := parseClock(s.Start)
	t := ((st+min)%(24*60) + 24*60) % (24 * 60)
	return fmt.Sprintf("%02d:%02d", t/60, t%60)
}

func minuteToClock(start string, min int) string { return (&Scenario{Start: start}).clock(min) }

// auxAt is the auxiliary load (kW) at a minute since start.
func (s *Scenario) auxAt(min int) float64 {
	if len(s.AuxProfile) == 0 {
		return s.AuxKW
	}
	// the profile repeats daily: take the latest point whose clock time is ≤ the current clock time
	st, _ := parseClock(s.Start)
	now := (st + min) % (24 * 60)
	pts := append([]AuxPoint(nil), s.AuxProfile...)
	sort.Slice(pts, func(i, j int) bool { a, _ := parseClock(pts[i].From); b, _ := parseClock(pts[j].From); return a < b })
	kw := pts[len(pts)-1].KW // wraps from the previous day
	for _, p := range pts {
		if t, _ := parseClock(p.From); t <= now {
			kw = p.KW
		}
	}
	return kw
}

// Generate draws the fleet. The same seed always gives the same fleet.
func (g *Generator) Generate(seed uint64, start string) ([]Bus, error) {
	if g.N <= 0 || len(g.BatteryKWh) == 0 || g.MaxKW <= 0 {
		return nil, fmt.Errorf("generator: n, battery_kwh and max_kw are required")
	}
	step := g.StepMin
	if step <= 0 {
		step = 5
	}
	a0, e1 := clockToMinute(start, g.ArrivalFrom)
	a1, e2 := clockToMinute(start, g.ArrivalTo)
	d0, e3 := clockToMinute(start, g.DepartureFrom)
	d1, e4 := clockToMinute(start, g.DepartureTo)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || a1 < a0 || d1 < d0 || d0 <= a1 {
		return nil, fmt.Errorf("generator: invalid time windows")
	}
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	pick := func(lo, hi int) int { return lo + int(math.Round(r.Float64()*float64(hi-lo)/float64(step)))*step }
	buses := make([]Bus, g.N)
	for i := range buses {
		impLo, impHi := g.Importance[0], g.Importance[1]
		if impLo <= 0 {
			impLo, impHi = 3, 3
		}
		buses[i] = Bus{
			ID:         fmt.Sprintf("G%02d", i+1),
			BatteryKWh: g.BatteryKWh[r.IntN(len(g.BatteryKWh))],
			MaxKW:      g.MaxKW,
			Arrival:    minuteToClock(start, pick(a0, a1)),
			Departure:  minuteToClock(start, pick(d0, d1)),
			ArrivalSoC: math.Round(g.ArrivalSoC[0] + r.Float64()*(g.ArrivalSoC[1]-g.ArrivalSoC[0])),
			TargetSoC:  g.TargetSoC,
			MinSoC:     g.MinSoC,
			Importance: impLo + r.IntN(impHi-impLo+1),
		}
	}
	return buses, nil
}

// clone returns a deep copy (for sensitivity variants).
func (s *Scenario) clone() *Scenario {
	c := *s
	c.Buses = append([]Bus(nil), s.Buses...)
	c.AuxProfile = append([]AuxPoint(nil), s.AuxProfile...)
	return &c
}
