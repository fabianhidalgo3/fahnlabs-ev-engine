package main

import (
	"fmt"
	"strconv"
)

// ScenarioRun holds the results of every strategy on one scenario.
type ScenarioRun struct {
	Scenario *Scenario
	File     string
	Results  map[string]Result
}

// SweepRow is one strategy on one variant of a sensitivity sweep.
type SweepRow struct {
	Sweep    string // limit_kw | fleet_size | urgency_weight | departure_shift_min | interval_min
	Scenario string
	Value    string
	Strategy string
	M        Metrics
	BaseM    Metrics   // uncoordinated run of the same variant
	V        *Scenario // the variant
}

// ReductionPct is the peak reduction (1-minute peak) against the uncoordinated baseline of the same variant.
func (r SweepRow) ReductionPct() float64 { return reduction(r.BaseM.PeakKW, r.M.PeakKW) }

func reduction(base, v float64) float64 {
	if base <= 0 {
		return 0
	}
	return 100 * (base - v) / base
}

// Sweep runs the sensitivity variants declared in the scenario.
func Sweep(s *Scenario) ([]SweepRow, error) {
	var rows []SweepRow
	add := func(sweep, value string, v *Scenario, strategies []string) {
		base := Simulate(v, Uncoordinated).Metrics
		for _, st := range strategies {
			m := base
			if st != Uncoordinated {
				m = Simulate(v, st).Metrics
			}
			rows = append(rows, SweepRow{Sweep: sweep, Scenario: s.ID, Value: value, Strategy: st, M: m, BaseM: base, V: v})
		}
	}
	all := Strategies
	w := s.Sweeps

	for _, l := range w.LimitKW {
		v := s.clone()
		v.SiteLimitKW = l
		add("limit_kw", strconv.FormatFloat(l, 'f', -1, 64), v, all)
	}
	for _, n := range w.FleetSizes {
		if s.Generator == nil {
			return nil, fmt.Errorf("%s: fleet_sizes needs a generator", s.ID)
		}
		v := s.clone()
		g := *s.Generator
		g.N = n
		v.Generator, v.Buses, v.Chargers = &g, nil, n
		if err := v.Prepare(); err != nil {
			return nil, err
		}
		add("fleet_size", strconv.Itoa(n), v, all)
	}
	for _, u := range w.UrgencyWeights {
		v := s.clone()
		v.Weights = renormalise(s.Weights, u)
		add("urgency_weight", fmt.Sprintf("%.2f", u), v, []string{Uncoordinated, "weighted"})
	}
	for _, d := range w.DepartureShift {
		v := s.clone()
		for i, b := range v.Buses {
			arr, _ := v.minute(b.Arrival)
			dep, _ := v.minute(b.Departure)
			v.Buses[i].Departure = v.clock(max(arr+v.IntervalMin, dep+d))
		}
		add("departure_shift_min", fmt.Sprintf("%+d", d), v, all)
	}
	for _, dt := range w.IntervalMin {
		v := s.clone()
		v.IntervalMin = dt
		add("interval_min", strconv.Itoa(dt), v, all)
	}
	return rows, nil
}

// renormalise sets the urgency weight and scales the other three so the four still add up to 1, keeping their
// proportions.
func renormalise(w engineWeights, u float64) engineWeights {
	rest := w.Deficit + w.Energy + w.Importance
	k := (1 - u) / rest
	return engineWeights{Urgency: u, Deficit: w.Deficit * k, Energy: w.Energy * k, Importance: w.Importance * k}
}
