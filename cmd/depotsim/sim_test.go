package main

import (
	"math"
	"path/filepath"
	"reflect"
	"testing"
)

func near(t *testing.T, name string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %.4f, want %.4f ± %.4f", name, got, want, tol)
	}
}

func load(t *testing.T, name string) *Scenario {
	t.Helper()
	s, err := LoadScenario(filepath.Join("scenarios", name))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func allScenarios(t *testing.T) []*Scenario {
	t.Helper()
	files, err := scenarioFiles("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	var out []*Scenario
	for _, f := range files {
		s, err := LoadScenario(f)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

// Formula (9): 150 kW for 10 min into 350 kWh at η = 0.95 adds 100·0.95·150·(1/6)/350 = 6.786 % SoC.
func TestNextSoCFormula9(t *testing.T) {
	near(t, "SoC'", nextSoC(40, 150, 10.0/60, 0.95, 350), 40+6.7857, 1e-3)
	near(t, "zero power", nextSoC(40, 0, 1, 0.95, 350), 40, 0)
	// formula (1): (90 − 40) % of 350 kWh / 0.95 = 184.2 kWh (Cuadro 9, bus A)
	near(t, "E", gridKWhToTarget(40, 90, 350, 0.95), 184.21, 0.01)
	near(t, "E at target", gridKWhToTarget(95, 90, 350, 0.95), 0, 0)
}

func TestTaper(t *testing.T) {
	tp := Taper{TransitionSoC: 80, EndFraction: 0.2}
	near(t, "below transition", acceptKW(150, 30, tp), 150, 0)
	near(t, "at transition", acceptKW(150, 80, tp), 150, 0)
	near(t, "halfway", acceptKW(150, 90, tp), 90, 1e-9) // 150 × (1 − 0.8 × 0.5)
	near(t, "full", acceptKW(150, 100, tp), 30, 1e-9)
}

// The SoC gained by every bus must match the energy delivered: ΔSoC × capacity / 100 = η × E.
func TestEnergyBalance(t *testing.T) {
	s := load(t, "s2_tight_limit.json")
	for _, st := range Strategies {
		for _, b := range Simulate(s, st).Metrics.PerBus {
			var bus Bus
			for _, x := range s.Buses {
				if x.ID == b.ID {
					bus = x
				}
			}
			near(t, st+" "+b.ID, (b.DepSoC-b.ArrivalSoC)/100*bus.BatteryKWh, s.Efficiency*b.EnergyKWh, 1e-6)
			if b.DepSoC > b.TargetSoC+1e-6 {
				t.Errorf("%s %s charged above its target: %.3f", st, b.ID, b.DepSoC)
			}
		}
	}
}

// Coordinated strategies never assign, nor draw, more than the site limit in any control interval.
func TestCoordinatedNeverAboveLimit(t *testing.T) {
	for _, s := range allScenarios(t) {
		for _, lim := range []float64{s.SiteLimitKW, 0.5 * s.SiteLimitKW} {
			v := s.clone()
			v.SiteLimitKW = lim
			for _, st := range coordinatedStrategies {
				r := Simulate(v, st)
				if r.Metrics.LimitViol != 0 {
					t.Errorf("%s %s limit %.0f: %d violations", s.ID, st, lim, r.Metrics.LimitViol)
				}
				for _, row := range r.Series {
					if row.AssignedKW+row.AuxMaxKW > lim+1e-6 || row.SiteMaxKW > lim+1e-6 {
						t.Fatalf("%s %s limit %.0f at minute %d: assigned %.1f + aux %.1f, site %.1f",
							s.ID, st, lim, row.Min, row.AssignedKW, row.AuxMaxKW, row.SiteMaxKW)
					}
				}
			}
		}
	}
}

// The baseline ignores the limit: with the S1 fleet it must exceed it.
func TestBaselineIgnoresLimit(t *testing.T) {
	s := load(t, "s1_base.json")
	m := Simulate(s, Uncoordinated).Metrics
	if m.PeakKW <= s.SiteLimitKW || m.LimitViol == 0 {
		t.Errorf("baseline peak %.1f kW with %d violations, want above the %.0f kW limit", m.PeakKW, m.LimitViol, s.SiteLimitKW)
	}
}

// Same scenario and seed → identical fleet and identical results; another seed → another fleet.
func TestDeterminism(t *testing.T) {
	a, b := load(t, "s4_generated_fleet.json"), load(t, "s4_generated_fleet.json")
	if !reflect.DeepEqual(a.Buses, b.Buses) {
		t.Fatal("same seed produced different fleets")
	}
	for _, st := range Strategies {
		if !reflect.DeepEqual(Simulate(a, st), Simulate(b, st)) {
			t.Errorf("%s: two runs of the same scenario differ", st)
		}
	}
	g := *a.Generator
	other, err := g.Generate(a.Seed+1, a.Start)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(other, a.Buses) {
		t.Error("a different seed produced the same fleet")
	}
	for _, bus := range a.Buses {
		arr, _ := a.minute(bus.Arrival)
		dep, _ := a.minute(bus.Departure)
		lo, _ := a.minute(g.ArrivalFrom)
		hi, _ := a.minute(g.DepartureTo)
		if arr < lo || dep > hi || bus.ArrivalSoC < g.ArrivalSoC[0] || bus.ArrivalSoC > g.ArrivalSoC[1] {
			t.Errorf("generated bus %+v outside the declared ranges", bus)
		}
	}
}

// The first weighted interval of the report example reproduces Cuadro 9 (via the engine).
func TestReportExampleCuadro9(t *testing.T) {
	s := load(t, "c8_report_example.json")
	r := Simulate(s, "weighted")
	want := map[string]float64{"A": 71.4, "B": 78.7, "C": 124.6, "D": 25.2}
	for i, b := range s.Buses {
		near(t, b.ID+" kW", r.Series[0].BusKW[i], want[b.ID], 0.2)
	}
	if r.Metrics.MetMin != 4 {
		t.Errorf("C8 weighted: %d of 4 departures met the minimum", r.Metrics.MetMin)
	}
}

func TestClockRollsOverMidnight(t *testing.T) {
	m, err := clockToMinute("20:00", "05:30")
	if err != nil || m != 570 {
		t.Errorf("05:30 from 20:00 = %d (%v), want 570", m, err)
	}
	if c := minuteToClock("20:00", 570); c != "05:30" {
		t.Errorf("clock = %s", c)
	}
}

func TestRenormaliseWeights(t *testing.T) {
	w := renormalise(engineWeights{Urgency: 0.4, Deficit: 0.2, Energy: 0.2, Importance: 0.2}, 0.55)
	near(t, "sum", w.Urgency+w.Deficit+w.Energy+w.Importance, 1, 1e-12)
	near(t, "deficit", w.Deficit, 0.15, 1e-12)
}
