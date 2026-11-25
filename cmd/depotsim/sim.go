package main

import (
	"math"

	"github.com/fhidalgo/emob/motor/internal/engine"
)

// Uncoordinated is the baseline: every connected bus charges at min(charger rating, taper(SoC)), ignoring the limit.
const Uncoordinated = "uncoordinated"

// Strategies compared by default (the coordinated ones go through engine.AllocateWith).
var Strategies = []string{Uncoordinated, "equal", "need", "weighted"}

const eps = 1e-9

// Metrics are the indicators of one scenario + strategy run.
type Metrics struct {
	Scenario string
	Strategy string

	PeakKW        float64 // maximum 1-minute site power (EV + auxiliary)
	Peak15KW      float64 // maximum 15-minute average site power (clock blocks), basis of the demand charge
	PeakEVKW      float64 // maximum 1-minute EV power
	EnergyKWh     float64 // energy delivered by the chargers (grid side)
	BatteryKWh    float64 // energy stored in the batteries (η × grid)
	Departures    int
	MetMin        int // departures with SoC ≥ minimum
	MetTarget     int // departures with SoC ≥ target (0.05 % tolerance)
	MinDepSoC     float64
	MeanDepSoC    float64
	MeanConnectH  float64 // mean time connected to a charger
	MeanChargingH float64 // mean time actually drawing power
	Utilization   float64 // energy / (chargers × rating × horizon)
	SetpointChg   int     // setpoint changes sent (coordinated strategies; the baseline sends none)
	LimitViol     int     // control intervals with assigned or measured site power above the limit
	ContingencyBI int     // bus-intervals in which the contingency rule fired (formula 7, evaluated for all strategies)
	ContingencyB  int     // buses that triggered the contingency rule at least once
	NeverCharged  int     // buses that never got a charger
	HorizonH      float64 // first arrival → last departure
	PerBus        []BusResult
}

// BusResult is the outcome for one bus.
type BusResult struct {
	ID          string
	ArrivalSoC  float64
	DepSoC      float64
	MinSoC      float64
	TargetSoC   float64
	EnergyKWh   float64
	Contingency int
}

// IntervalRow is one control interval of the time series.
type IntervalRow struct {
	Min        int
	AuxMaxKW   float64   // highest auxiliary load inside the interval (used for the available power)
	AvailKW    float64   // power available to the engine: limit − margin − AuxMaxKW
	AssignedKW float64   // sum of setpoints (baseline: highest 1-minute EV power, nothing is assigned)
	SiteAvgKW  float64   // mean measured site power
	SiteMaxKW  float64   // highest 1-minute site power
	BusKW      []float64 // mean power of each bus over the interval
	BusSoC     []float64 // SoC at the end of the interval
}

// Result of one run.
type Result struct {
	Metrics Metrics
	Series  []IntervalRow
}

// acceptKW is the power the battery accepts at a SoC (taper curve), in kW.
func acceptKW(maxKW, soc float64, t Taper) float64 {
	if soc <= t.TransitionSoC {
		return maxKW
	}
	if soc >= 100 {
		return maxKW * t.EndFraction
	}
	frac := (soc - t.TransitionSoC) / (100 - t.TransitionSoC)
	return maxKW * (1 - (1-t.EndFraction)*frac)
}

// nextSoC is formula (9): SoC' = SoC + 100·η·P·Δt / capacity (P in kW, Δt in hours).
func nextSoC(soc, kw, hours, eta, capKWh float64) float64 {
	return soc + 100*eta*kw*hours/capKWh
}

// gridKWhToTarget is formula (1): energy the charger still has to deliver to reach the target.
func gridKWhToTarget(soc, target, capKWh, eta float64) float64 {
	return math.Max(0, (target-soc)/100*capKWh/eta)
}

type busState struct {
	Bus
	idx          int
	arr, dep     int
	soc          float64
	connected    bool
	waiting      bool
	everConnect  bool
	connectAt    int
	connectedMin int
	chargingMin  int
	energy       float64
	setpointKW   float64
	spChanges    int
	contingency  int
}

// Simulate runs one strategy over the scenario with a 1-minute physical step; setpoints are recomputed every Δt
// and held in between (a bus that connects mid-interval waits for the next control instant).
func Simulate(s *Scenario, strategy string) Result {
	n := len(s.Buses)
	bs := make([]*busState, n)
	first, end := math.MaxInt, 0
	for i, b := range s.Buses {
		arr, _ := s.minute(b.Arrival)
		dep, _ := s.minute(b.Departure)
		bs[i] = &busState{Bus: b, idx: i, arr: arr, dep: dep, soc: b.ArrivalSoC}
		first = min(first, arr)
		end = max(end, dep)
	}
	dt := s.IntervalMin
	params := engine.Params{Weights: s.Weights, Efficiency: s.Efficiency, IntervalH: float64(dt) / 60, DefaultH: 4}
	freeChargers := s.Chargers
	var queue []*busState

	m := Metrics{Scenario: s.ID, Strategy: strategy, MinDepSoC: math.Inf(1)}
	var series []IntervalRow
	var row *IntervalRow
	siteMinute := make([]float64, end)
	coordinated := strategy != Uncoordinated

	for t := 0; t < end; t++ {
		event := false
		// 1) departures, and full buses releasing their charger when the scenario allows it
		for _, b := range bs {
			full := b.soc >= b.TargetSoC-eps
			if b.connected && (t >= b.dep || (s.ReleaseWhenFull && full)) {
				b.connected = false
				b.setpointKW = 0
				b.connectedMin += t - b.connectAt
				freeChargers++
				event = true
			}
			if b.waiting && t >= b.dep {
				b.waiting = false
			}
		}
		// 2) arrivals join the queue (FIFO by arrival, then declaration order); free chargers are assigned
		for _, b := range bs {
			if b.arr == t && t < b.dep {
				b.waiting = true
				queue = append(queue, b)
			}
		}
		for freeChargers > 0 && len(queue) > 0 {
			b := queue[0]
			queue = queue[1:]
			if !b.waiting { // left before getting a charger
				continue
			}
			b.waiting, b.connected, b.everConnect, b.connectAt = false, true, true, t
			freeChargers--
			event = true
		}

		// 3) control instant every Δt; also on a connection or disconnection unless the scenario is periodic-only
		if t%dt != 0 && event && !s.PeriodicOnly && row != nil {
			assigned := control(s, strategy, params, bs, t, row.AvailKW)
			row.AssignedKW = math.Max(row.AssignedKW, assigned)
		}
		if t%dt == 0 {
			if row != nil {
				closeRow(row, bs)
				series = append(series, *row)
			}
			auxMax := 0.0
			for k := t; k < min(t+dt, end); k++ {
				auxMax = math.Max(auxMax, s.auxAt(k))
			}
			avail := s.SiteLimitKW - s.MarginKW - auxMax
			row = &IntervalRow{Min: t, AuxMaxKW: auxMax, AvailKW: avail, BusKW: make([]float64, n), BusSoC: make([]float64, n)}
			row.AssignedKW = control(s, strategy, params, bs, t, avail)
		}

		// 4) physics of this minute
		ev := 0.0
		for i, b := range bs {
			if !b.connected {
				continue
			}
			sp := b.setpointKW
			if !coordinated {
				sp = math.Min(s.ChargerKW, b.MaxKW) // the baseline takes whatever the charger and the battery accept
			}
			kw := math.Min(sp, math.Min(s.ChargerKW, acceptKW(b.MaxKW, b.soc, s.Taper)))
			kw = math.Min(kw, gridKWhToTarget(b.soc, b.TargetSoC, b.BatteryKWh, s.Efficiency)*60) // stops at target
			kw = math.Max(0, kw)
			if kw > 1e-6 {
				b.chargingMin++
			}
			b.soc = nextSoC(b.soc, kw, 1.0/60, s.Efficiency, b.BatteryKWh) // (9), 1-minute step
			b.energy += kw / 60
			ev += kw
			row.BusKW[i] += kw / float64(dt)
		}
		site := s.auxAt(t) + ev
		siteMinute[t] = site
		m.PeakKW = math.Max(m.PeakKW, site)
		m.PeakEVKW = math.Max(m.PeakEVKW, ev)
		row.SiteAvgKW += site / float64(dt)
		row.SiteMaxKW = math.Max(row.SiteMaxKW, site)
		if !coordinated {
			row.AssignedKW = math.Max(row.AssignedKW, ev)
		}
	}
	if row != nil {
		closeRow(row, bs)
		series = append(series, *row)
	}

	// non-compliance: an interval whose assigned power + auxiliary load, or whose measured site power, is above the limit
	for _, r := range series {
		if r.AssignedKW+r.AuxMaxKW > s.SiteLimitKW+1e-6 || r.SiteMaxKW > s.SiteLimitKW+1e-6 {
			m.LimitViol++
		}
	}

	// 15-minute demand on clock blocks (:00, :15, :30, :45); minutes outside the horizon count as 0 kW
	st, _ := parseClock(s.Start)
	blocks := map[int]float64{}
	for t, p := range siteMinute {
		blocks[(st+t)/15] += p
	}
	for _, v := range blocks {
		m.Peak15KW = math.Max(m.Peak15KW, v/15)
	}

	// per-bus indicators (a bus still connected at the end leaves at its departure, the end of the horizon)
	sumSoC, connected := 0.0, 0
	for _, b := range bs {
		if b.connected {
			b.connectedMin += end - b.connectAt
		}
		m.Departures++
		if b.soc >= b.MinSoC-1e-6 {
			m.MetMin++
		}
		if b.soc >= b.TargetSoC-0.05 {
			m.MetTarget++
		}
		m.MinDepSoC = math.Min(m.MinDepSoC, b.soc)
		sumSoC += b.soc
		m.EnergyKWh += b.energy
		if b.everConnect {
			connected++
			m.MeanConnectH += float64(b.connectedMin) / 60
			m.MeanChargingH += float64(b.chargingMin) / 60
		} else {
			m.NeverCharged++
		}
		if b.contingency > 0 {
			m.ContingencyB++
		}
		m.ContingencyBI += b.contingency
		m.SetpointChg += b.spChanges
		m.PerBus = append(m.PerBus, BusResult{ID: b.ID, ArrivalSoC: b.ArrivalSoC, DepSoC: b.soc, MinSoC: b.MinSoC,
			TargetSoC: b.TargetSoC, EnergyKWh: b.energy, Contingency: b.contingency})
	}
	m.MeanDepSoC = sumSoC / float64(n)
	if connected > 0 {
		m.MeanConnectH /= float64(connected)
		m.MeanChargingH /= float64(connected)
	}
	m.BatteryKWh = m.EnergyKWh * s.Efficiency
	m.HorizonH = float64(end-first) / 60
	if m.HorizonH > 0 {
		m.Utilization = m.EnergyKWh / (float64(s.Chargers) * s.ChargerKW * m.HorizonH)
	}
	return Result{Metrics: m, Series: series}
}

// control computes the setpoints of one control interval and returns their sum (kW). For the baseline it only
// evaluates the formulas (to count contingency activations) and returns 0.
func control(s *Scenario, strategy string, p engine.Params, bs []*busState, t int, availKW float64) float64 {
	var sessions []engine.Session
	var active []*busState
	for _, b := range bs {
		if !b.connected {
			continue
		}
		e := gridKWhToTarget(b.soc, b.TargetSoC, b.BatteryKWh, s.Efficiency)
		if e < 1e-6 { // at its target: no longer competes (it would only dilute the reference share of formula 7)
			b.setpointKW = 0
			continue
		}
		// cap = min(charger, battery acceptance now, energy still needed within Δt): the session's real demand
		capKW := math.Min(s.ChargerKW, acceptKW(b.MaxKW, b.soc, s.Taper))
		capKW = math.Min(capKW, e/p.IntervalH)
		soc, target, minSoC, batt := b.soc, b.TargetSoC, b.MinSoC, b.BatteryKWh
		hours := float64(b.dep-t) / 60
		sessions = append(sessions, engine.Session{
			Key: b.ID, CapW: capKW * 1000, Priority: 5, StartedAt: int64(b.connectAt),
			NeedKWh: &e, HoursLeft: &hours,
			SoC: &soc, TargetSoC: &target, MinSoC: &minSoC, BatteryKWh: &batt,
			RatedW: math.Min(s.ChargerKW, b.MaxKW) * 1000, Importance: b.Importance,
		})
		active = append(active, b)
	}
	if len(sessions) == 0 {
		return 0
	}
	evals := engine.Evaluate(math.Max(0, availKW)*1000, p, sessions)
	for _, b := range active {
		if ev := evals[b.ID]; ev.Contingency && b.soc < b.TargetSoC-eps {
			b.contingency++
		}
	}
	if strategy == Uncoordinated {
		return 0
	}
	alloc := engine.AllocateWith(math.Max(0, availKW)*1000, 0, strategy, p, sessions)
	total := 0.0
	for _, b := range active {
		kw := alloc[b.ID] / 1000
		if math.Abs(kw-b.setpointKW) >= engine.StepW/1000.0-eps {
			b.spChanges++
		}
		b.setpointKW = kw
		total += kw
	}
	return total
}

func closeRow(r *IntervalRow, bs []*busState) {
	for i, b := range bs {
		r.BusSoC[i] = b.soc
	}
}
