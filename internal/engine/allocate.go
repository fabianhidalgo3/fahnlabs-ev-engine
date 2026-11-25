// Package engine implements the power allocation algorithm (pure, stateless, easy to test).
//
// Input:
//
//	availableW  power available for charging (yard limit − auxiliary loads − reserves)
//	minW        default minimum power for an EV to charge (IEC 61851: 6 A)
//	strategy    "equal" | "priority" | "fifo" | "need" | "weighted"
//	sessions    each with CapW (min of charger, vehicle and real demand), Priority, StartedAt,
//	            optional MinW (e.g. single-phase EV: 6 A × 230 V = 1.38 kW), NeedKWh and HoursLeft;
//	            "weighted" also uses SoC, TargetSoC, MinSoC, BatteryKWh, RatedW and Importance (see weighted.go)
//
// Output: map key → allocated W. A session with 0 W is paused (SuspendedEVSE).
//
// Every strategy follows two phases:
//  1. Admission: sessions enter (by priority / arrival order) while their minimum can be guaranteed.
//  2. Distribution of the surplus above the minimum:
//     equal    → same total power for everyone, water-filling (what one cannot use goes to the others)
//     need     → proportional to energy pending / hours left, water-filling
//     priority → in priority order up to each cap
//     fifo     → in arrival order up to each cap
//     weighted → proportional to the priority of formulas 1–7 (urgency, SoC deficit, energy, importance, with
//     the contingency rule), each bounded by its cap and by the energy it can take in Δt, water-filling
package engine

import (
	"math"
	"sort"
)

// StepW is the setpoint granularity.
const StepW = 100

// Session is one charging session competing for power.
type Session struct {
	Key       string
	CapW      float64
	Priority  int
	StartedAt int64
	MinW      *float64
	NeedKWh   *float64
	HoursLeft *float64

	// Inputs of the "weighted" strategy (all optional)
	SoC        *float64 // current state of charge, %
	TargetSoC  *float64 // SoC to leave with, %
	MinSoC     *float64 // minimum acceptable SoC at departure, % (contingency rule)
	BatteryKWh *float64 // usable battery capacity
	RatedW     float64  // maximum power the vehicle accepts now (0 = CapW)
	Importance int      // importance of the next service, 1..5 (0 = Priority/10)
}

func floorStep(w float64) float64 { return math.Max(0, math.Floor(w/StepW)*StepW) }

func order(sessions []Session, strategy string) []Session {
	s := append([]Session(nil), sessions...)
	sort.SliceStable(s, func(i, j int) bool {
		if strategy == "fifo" {
			return s[i].StartedAt < s[j].StartedAt
		}
		if s[i].Priority != s[j].Priority {
			return s[i].Priority > s[j].Priority
		}
		return s[i].StartedAt < s[j].StartedAt
	})
	return s
}

type admitted struct {
	Session
	min    float64
	weight float64
}

// Allocate splits availableW among the sessions according to the strategy, with the default parameters.
func Allocate(availableW, minW float64, strategy string, sessions []Session) map[string]float64 {
	return AllocateWith(availableW, minW, strategy, DefaultParams, sessions)
}

// AllocateWith is Allocate with explicit parameters (weights, η and Δt of the "weighted" strategy).
func AllocateWith(availableW, minW float64, strategy string, p Params, sessions []Session) map[string]float64 {
	result := make(map[string]float64, len(sessions))
	for _, s := range sessions {
		result[s.Key] = 0
	}
	if availableW <= 0 || len(sessions) == 0 {
		return result
	}

	// 1) Admission
	var adm []admitted
	reserved := 0.0
	for _, s := range order(sessions, strategy) {
		capW := math.Max(0, s.CapW)
		min := minW
		if s.MinW != nil {
			min = *s.MinW
		}
		if capW < min || reserved+min > availableW {
			continue // cannot even take the minimum, or no room for it
		}
		s.CapW = capW
		adm = append(adm, admitted{Session: s, min: min})
		reserved += min
	}
	if len(adm) == 0 {
		return result
	}

	// 2) Distribution
	if strategy == "weighted" {
		weightedCaps(adm, Evaluate(availableW, p, sessions), p)
		return waterFill(adm, availableW, result)
	}
	if strategy == "equal" || strategy == "need" {
		// Bounded water level: each session gets clamp(level × weight, min, cap) and the level that uses up
		// the available power is searched by bisection. 'equal' → weight 1 (same TOTAL power for everyone).
		known, sum := 0, 0.0
		for _, a := range adm {
			if a.NeedKWh != nil {
				known++
				sum += *a.NeedKWh
			}
		}
		avgNeed := 20.0
		if known > 0 {
			avgNeed = sum / float64(known)
		}
		for i := range adm {
			adm[i].weight = 1
			if strategy == "need" {
				need, hours := avgNeed, 4.0 // 4 h assumed without a plan with a departure time
				if adm[i].NeedKWh != nil {
					need = *adm[i].NeedKWh
				}
				if adm[i].HoursLeft != nil {
					hours = *adm[i].HoursLeft
				}
				adm[i].weight = math.Max(0.1, need/math.Max(0.25, hours))
			}
		}
		return waterFill(adm, availableW, result)
	}

	remaining := availableW - reserved
	for _, a := range adm {
		extra := math.Min(a.CapW-a.min, remaining)
		remaining -= extra
		result[a.Key] = floorStep(a.min + extra)
	}
	return result
}

// waterFill gives each session clamp(level × weight, min, cap) and searches by bisection the level that uses up
// the available power: a proportional split in which what one session cannot take goes to the others.
func waterFill(adm []admitted, availableW float64, result map[string]float64) map[string]float64 {
	hi := 0.0
	for _, a := range adm {
		hi = math.Max(hi, a.CapW/a.weight)
	}
	at := func(level float64) float64 {
		t := 0.0
		for _, a := range adm {
			t += math.Min(a.CapW, math.Max(a.min, level*a.weight))
		}
		return t
	}
	lo := 0.0
	if at(hi) <= availableW {
		lo = hi // everyone reaches its cap
	} else {
		for k := 0; k < 60; k++ {
			mid := (lo + hi) / 2
			if at(mid) <= availableW {
				lo = mid
			} else {
				hi = mid
			}
		}
	}
	for _, a := range adm {
		result[a.Key] = floorStep(math.Min(a.CapW, math.Max(a.min, lo*a.weight)))
	}
	return result
}
