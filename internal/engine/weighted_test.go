package engine

import (
	"math"
	"testing"
)

// bus builds a session from the columns of Cuadro 8 of the thesis report.
func bus(key string, capKWh, soc, target, minSoC, hours, maxKW float64, importance int) Session {
	return Session{Key: key, CapW: maxKW * 1000, RatedW: maxKW * 1000, Priority: 5, SoC: f(soc), TargetSoC: f(target),
		MinSoC: f(minSoC), BatteryKWh: f(capKWh), HoursLeft: f(hours), Importance: importance}
}

func reportBuses() []Session {
	return []Session{
		bus("A", 350, 40, 90, 80, 3.0, 150, 3),
		bus("B", 350, 55, 90, 80, 1.5, 150, 5),
		bus("C", 280, 20, 90, 80, 2.0, 150, 2),
		bus("D", 420, 85, 90, 80, 6.0, 150, 4),
	}
}

func near(t *testing.T, name string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Errorf("%s = %.3f, want %.3f ± %.3f", name, got, want, tol)
	}
}

// Cuadro 9 of the report: first control interval with 4 buses, 300 kW available, η = 0.95, Δt = 10 min.
func TestWeightedReproducesReportTable9(t *testing.T) {
	ev := Evaluate(300000, DefaultParams, reportBuses())
	want := map[string]struct {
		e, u, d, er, p float64
		cont           bool
	}{
		"A": {184.2, 0.41, 0.56, 0.89, 0.57, false},
		"B": {128.9, 0.57, 0.39, 0.62, 0.63, false},
		"C": {206.3, 0.69, 0.78, 1.00, 1.00, true},
		"D": {22.1, 0.02, 0.06, 0.11, 0.20, false},
	}
	for k, w := range want {
		e := ev[k]
		near(t, k+" energy", e.EnergyKWh, w.e, 0.05)
		near(t, k+" urgency", e.Urgency, w.u, 0.006)
		near(t, k+" deficit", e.Deficit, w.d, 0.006)
		near(t, k+" relative energy", e.EnergyRel, w.er, 0.006)
		near(t, k+" priority", e.Priority, w.p, 0.006)
		if e.Contingency != w.cont {
			t.Errorf("%s contingency = %v, want %v", k, e.Contingency, w.cont)
		}
	}
	near(t, "C forecast SoC", ev["C"].ForecastSoC, 70.9, 0.05)

	alloc := Allocate(300000, 0, "weighted", reportBuses())
	for k, kw := range map[string]float64{"A": 71.4, "B": 78.7, "C": 124.6, "D": 25.2} {
		near(t, k+" kW", alloc[k]/1000, kw, 0.2) // setpoints are floored to 100 W
	}
	total := 0.0
	for _, w := range alloc {
		total += w
	}
	if total > 300000 {
		t.Errorf("total %.0f W exceeds the 300 kW available", total)
	}
}

// A bus that only needs a little energy is capped at what it can take in Δt; the rest goes to the others.
func TestWeightedRedistributesWhatABusCannotTake(t *testing.T) {
	almostFull := bus("full", 300, 89.5, 90, 80, 0.5, 150, 5)
	empty := bus("empty", 300, 20, 90, 80, 3, 150, 1)
	alloc := Allocate(200000, 0, "weighted", []Session{almostFull, empty})
	// 0.5 % of 300 kWh / 0.95 = 1.58 kWh → at most 9.47 kW in 10 minutes
	if alloc["full"] > 9500 {
		t.Errorf("full = %.0f W, want ≤ 9.5 kW", alloc["full"])
	}
	if alloc["empty"] != 150000 {
		t.Errorf("empty = %.0f W, want its 150 kW cap", alloc["empty"])
	}
}

// Sessions without SoC data (e.g. AC cars) still charge with the mean priority of the others.
func TestWeightedSessionsWithoutDataStillCharge(t *testing.T) {
	car := Session{Key: "car", CapW: 22000, Priority: 5}
	alloc := Allocate(100000, 1380, "weighted", append(reportBuses()[:1], car))
	if alloc["car"] < 1380 {
		t.Errorf("car = %.0f W, want at least the minimum", alloc["car"])
	}
}
