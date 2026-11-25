package engine

import "math"

// Weighted priority strategy ("weighted"): the bus-depot model of the thesis report (formulas 1 to 9).
//
// For every connected vehicle, in each control interval:
//
//	(1) E      = (SoCtarget − SoC)/100 × capacity / η                 energy still to deliver (kWh)
//	(2) Pneed  = E / hours left                                       average power needed (kW)
//	(3) U      = min(1, Pneed / Pmax)                                 urgency
//	(4) D      = (SoCtarget − SoC) / SoCtarget                        SoC deficit
//	(5) Er     = E / max(E)                                           relative energy
//	(6) P      = wU·U + wD·D + wE·Er + wI·I                           priority; I = importance in [0, 1]
//	(7) SoCf   = SoC + 100·η·Pref·hours / capacity                    forecast with an equal reference share
//	    contingency: SoCf < SoCmin  ⇒  P = 1
//	(8) Pi     = Pavailable × Pi / ΣP                                 proportional split, each bounded by
//	                                                                   min(Pmax, E/Δt); the excess is redistributed
//	(9) SoC'   = SoC + 100·η·Pi·Δt / capacity                         (applied by the vehicle / the simulator)
//
// Sessions without SoC, battery capacity or target SoC cannot be evaluated by the formulas: they get the mean
// priority of the others (or 0.5 when nobody has data), so they still charge.

// Weights of the priority function (formula 6). They should add up to 1.
type Weights struct {
	Urgency    float64 `json:"urgency"`
	Deficit    float64 `json:"deficit"`
	Energy     float64 `json:"energy"`
	Importance float64 `json:"importance"`
}

// Params are the tunable parameters of the allocation.
type Params struct {
	Weights    Weights
	Efficiency float64 // η, charging efficiency (0 < η ≤ 1)
	IntervalH  float64 // Δt, control interval in hours (caps a session at the power that would finish it in Δt)
	DefaultH   float64 // hours left assumed when a session has no departure time
}

// DefaultParams are the values proposed in the report: weights 0.40/0.20/0.20/0.20, η = 0.95, Δt = 10 min.
var DefaultParams = Params{
	Weights:    Weights{Urgency: 0.40, Deficit: 0.20, Energy: 0.20, Importance: 0.20},
	Efficiency: 0.95,
	IntervalH:  10.0 / 60,
	DefaultH:   4,
}

// Evaluation is the per-session result of formulas 1 to 7 (for display, logs and tests).
type Evaluation struct {
	EnergyKWh     float64 `json:"energyKWh"`
	NeedKW        float64 `json:"needKW"`
	Urgency       float64 `json:"urgency"`
	Deficit       float64 `json:"deficit"`
	EnergyRel     float64 `json:"energyRel"`
	Importance    float64 `json:"importance"`
	Priority      float64 `json:"priority"`
	ForecastSoC   float64 `json:"forecastSoc"`
	Contingency   bool    `json:"contingency"`
	HasModelInput bool    `json:"hasModelInput"`
}

func (s Session) ratedW() float64 {
	if s.RatedW > 0 {
		return s.RatedW
	}
	return s.CapW
}

// importance in [0, 1]: explicit Importance (1..5) or the 1..10 priority scaled.
func (s Session) importance() float64 {
	if s.Importance > 0 {
		return math.Min(1, float64(s.Importance)/5)
	}
	return math.Max(0, math.Min(1, float64(s.Priority)/10))
}

// Evaluate applies formulas 1 to 7 to the sessions with the power available for the reference share.
func Evaluate(availableW float64, p Params, sessions []Session) map[string]Evaluation {
	eta := p.Efficiency
	if eta <= 0 || eta > 1 {
		eta = DefaultParams.Efficiency
	}
	out := make(map[string]Evaluation, len(sessions))
	maxE := 0.0
	for _, s := range sessions {
		ev := Evaluation{Importance: s.importance()}
		if s.SoC != nil && s.TargetSoC != nil && s.BatteryKWh != nil && *s.BatteryKWh > 0 && *s.TargetSoC > 0 {
			ev.HasModelInput = true
			ev.EnergyKWh = math.Max(0, (*s.TargetSoC-*s.SoC)/100**s.BatteryKWh/eta)     // (1)
			ev.Deficit = math.Max(0, math.Min(1, (*s.TargetSoC-*s.SoC) / *s.TargetSoC)) // (4)
			maxE = math.Max(maxE, ev.EnergyKWh)
		}
		out[s.Key] = ev
	}
	n := float64(len(sessions))
	known, sumP := 0, 0.0
	for _, s := range sessions {
		ev := out[s.Key]
		if !ev.HasModelInput {
			continue
		}
		hours := p.DefaultH
		if s.HoursLeft != nil {
			hours = *s.HoursLeft
		}
		hours = math.Max(hours, 1.0/60)  // a departure already due counts as one minute left
		ev.NeedKW = ev.EnergyKWh / hours // (2)
		if r := s.ratedW() / 1000; r > 0 {
			ev.Urgency = math.Min(1, ev.NeedKW/r) // (3)
		} else {
			ev.Urgency = 1
		}
		if maxE > 0 {
			ev.EnergyRel = ev.EnergyKWh / maxE // (5)
		}
		w := p.Weights
		ev.Priority = w.Urgency*ev.Urgency + w.Deficit*ev.Deficit + w.Energy*ev.EnergyRel + w.Importance*ev.Importance // (6)
		// (7) forecast with the equal reference share, never above what the vehicle accepts
		refKW := math.Min(availableW/math.Max(1, n), s.ratedW()) / 1000
		ev.ForecastSoC = *s.SoC + 100*eta*refKW*hours / *s.BatteryKWh
		if s.MinSoC != nil && ev.ForecastSoC < *s.MinSoC {
			ev.Contingency = true
			ev.Priority = 1
		}
		out[s.Key] = ev
		known++
		sumP += ev.Priority
	}
	fallback := 0.5
	if known > 0 {
		fallback = sumP / float64(known)
	}
	for _, s := range sessions {
		if ev := out[s.Key]; !ev.HasModelInput {
			ev.Priority = fallback
			out[s.Key] = ev
		}
	}
	return out
}

// weightedCaps bounds each admitted session by the energy it can still take in one control interval.
func weightedCaps(adm []admitted, evals map[string]Evaluation, p Params) {
	dt := p.IntervalH
	if dt <= 0 {
		dt = DefaultParams.IntervalH
	}
	for i := range adm {
		ev := evals[adm[i].Key]
		if ev.HasModelInput {
			adm[i].CapW = math.Max(adm[i].min, math.Min(adm[i].CapW, ev.EnergyKWh/dt*1000))
		}
		adm[i].weight = math.Max(1e-3, ev.Priority)
	}
}
