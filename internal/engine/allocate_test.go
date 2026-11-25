package engine

import (
	"fmt"
	"testing"
)

func s(key string, capW float64, prio int, started int64) Session {
	return Session{Key: key, CapW: capW, Priority: prio, StartedAt: started}
}
func f(v float64) *float64 { return &v }

func expect(t *testing.T, r map[string]float64, want map[string]float64) {
	t.Helper()
	for k, w := range want {
		if r[k] != w {
			t.Errorf("%s = %v, want %v (all: %v)", k, r[k], w, r)
		}
	}
}

func TestEqualSplitsEvenly(t *testing.T) {
	r := Allocate(30000, 4200, "equal", []Session{s("a", 22000, 5, 0), s("b", 22000, 5, 0)})
	expect(t, r, map[string]float64{"a": 15000, "b": 15000})
}

func TestEqualRedistributesUnusedPower(t *testing.T) {
	r := Allocate(30000, 4200, "equal", []Session{s("a", 7400, 5, 0), s("b", 22000, 5, 0)})
	expect(t, r, map[string]float64{"a": 7400, "b": 22000})
}

func TestPausesLowestPriorityWhenShort(t *testing.T) {
	r := Allocate(8000, 4200, "equal", []Session{s("a", 22000, 3, 0), s("b", 22000, 8, 0)})
	expect(t, r, map[string]float64{"b": 8000, "a": 0})
}

func TestPriorityMinimumThenSurplusByPriority(t *testing.T) {
	r := Allocate(20000, 4200, "priority", []Session{s("a", 22000, 2, 0), s("b", 11000, 9, 0)})
	expect(t, r, map[string]float64{"b": 11000, "a": 9000})
}

func TestFifoFirstArrivalFillsFirst(t *testing.T) {
	r := Allocate(26000, 4200, "fifo", []Session{s("late", 22000, 5, 200), s("early", 22000, 5, 100)})
	expect(t, r, map[string]float64{"early": 21800, "late": 4200})
}

func TestNeedMorePowerToTheEVMissingMoreEnergy(t *testing.T) {
	a, b := s("little", 22000, 5, 0), s("lots", 22000, 5, 0)
	a.NeedKWh, b.NeedKWh = f(5), f(35)
	// proportional 5:35 floored at the minimum: 'little' stays at 4.2 kW, 'lots' gets the rest
	expect(t, Allocate(20000, 4200, "need", []Session{a, b}), map[string]float64{"little": 4200, "lots": 15800})
}

func TestNeedEarlierDepartureWins(t *testing.T) {
	a, b := s("long", 22000, 5, 0), s("urgent", 22000, 5, 0)
	a.NeedKWh, a.HoursLeft = f(20), f(8)
	b.NeedKWh, b.HoursLeft = f(20), f(1)
	// weights 2.5 vs 20 kW → 'long' stays at the minimum
	expect(t, Allocate(22000, 4200, "need", []Session{a, b}), map[string]float64{"long": 4200, "urgent": 17800})
}

func TestEqualSameTotalForSingleAndThreePhase(t *testing.T) {
	mono := s("mono", 7400, 5, 0)
	mono.MinW = f(1400)
	r := Allocate(20000, 4200, "equal", []Session{mono, s("tri1", 22000, 5, 0), s("tri2", 22000, 5, 0), s("tri3", 22000, 5, 0)})
	expect(t, r, map[string]float64{"mono": 5000, "tri1": 5000, "tri2": 5000, "tri3": 5000})
}

func TestOwnMinimumOfSinglePhaseEV(t *testing.T) {
	mono := s("mono", 7400, 9, 0)
	mono.MinW = f(1400)
	r := Allocate(5600, 4200, "equal", []Session{mono, s("tri", 22000, 1, 0)})
	expect(t, r, map[string]float64{"mono": 1400, "tri": 4200})
}

func TestNeverExceedsAvailablePower(t *testing.T) {
	var sessions []Session
	for i := 0; i < 7; i++ {
		x := s(fmt.Sprintf("c%d", i), float64(11000+i*1000), i, int64(i))
		x.NeedKWh = f(float64(i * 4))
		sessions = append(sessions, x)
	}
	for _, strategy := range []string{"equal", "priority", "fifo", "need"} {
		total := 0.0
		for _, w := range Allocate(37000, 4200, strategy, sessions) {
			total += w
		}
		if total > 37000 {
			t.Errorf("%s: total %v exceeds 37000", strategy, total)
		}
	}
}
