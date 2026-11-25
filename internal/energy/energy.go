// Package energy computes energy-management indicators: how much the power control saves compared with
// unmanaged charging.
//
// CONTROLLED scenario   = measured power (meter samples) of each session, held between samples.
// UNCONTROLLED baseline = "immediate full-power charging" (standard unmanaged reference): every vehicle charges
// from arrival at P_max = min(charger, on-board charger) until it has received the SAME energy it received under
// control. Demand is measured, as in electricity billing, as 15-minute averages.
//
// socCompliance: among finished sessions whose departure time falls in the period and that recorded a departure
// verdict (minimum SoC + departure time + SoC known, see store.Session.MinSoCMet), how many left with SoC ≥ minimum.
package energy

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/fhidalgo/emob/motor/internal/store"
)

const (
	min15 = int64(15 * 60_000)
	hour  = 3_600_000.0
	day   = 86_400_000.0
)

// Defaults of the tariffs and factors (CLP, Chilean grid; editable in the dashboard).
var Defaults = map[string]float64{
	"energyPriceClp":    150,  // energy price, CLP/kWh
	"demandChargeClpKw": 9000, // maximum-demand charge, CLP/kW·month
	"gridCo2KgKwh":      0.29, // grid emission factor, kg CO2/kWh
	"evKmPerKwh":        6,    // average EV efficiency, km/kWh
	"iceCo2KgKm":        0.18, // combustion car emissions, kg CO2/km
	"iceKmPerL":         12,   // combustion car efficiency, km/L
	"fuelPriceClpL":     1300, // fuel price, CLP/L
}

// Assumptions merges the configured tariffs over the defaults.
func Assumptions(cfg map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range Defaults {
		out[k] = v
	}
	for k, v := range cfg {
		out[k] = v
	}
	return out
}

// GroupOf returns the yard (group) of a charger: prefix before the dash (PRUEBA-03 → PRUEBA).
func GroupOf(id string) string {
	if i := strings.Index(id, "-"); i >= 0 {
		return id[:i]
	}
	return id
}

// Group summarises the sessions of one yard.
type Group struct {
	Group         string `json:"group"`
	Sessions      int    `json:"sessions"`
	LastSessionAt int64  `json:"lastSessionAt"`
}

// Groups lists the yards with sessions.
func Groups(ctx context.Context, ops *store.Ops) ([]Group, error) {
	type row struct {
		ID   string `bson:"_id"`
		N    int    `bson:"n"`
		Last int64  `bson:"last"`
	}
	rows, err := store.Aggregate[row](ctx, ops.Sessions, []bsonD{
		{{Key: "$group", Value: bsonM{"_id": "$charger_id", "n": bsonM{"$sum": 1}, "last": bsonM{"$max": "$started_at"}}}},
	})
	if err != nil {
		return nil, err
	}
	m := map[string]*Group{}
	for _, r := range rows {
		g := GroupOf(r.ID)
		if m[g] == nil {
			m[g] = &Group{Group: g}
		}
		m[g].Sessions += r.N
		m[g].LastSessionAt = max(m[g].LastSessionAt, r.Last)
	}
	out := []Group{}
	for _, g := range m {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Group < out[j].Group })
	return out, nil
}

func r1(x float64) float64 { return math.Round(x*10) / 10 }
func r2(x float64) float64 { return math.Round(x*100) / 100 }

// Point is one window of the chart series.
type Point struct {
	T              int64   `json:"t"`
	ControlledKw   float64 `json:"controlledKw"`
	UncontrolledKw float64 `json:"uncontrolledKw"`
	AuxKw          float64 `json:"auxKw"`
}

// Report computes the indicators for [from, to) and the given yards (empty = all).
func Report(ctx context.Context, ops *store.Ops, master *store.Master, siteLimitKw float64, tariffsCfg map[string]float64, from, to int64, groups []string) (map[string]any, error) {
	now := time.Now().UnixMilli()
	to = min(to, now)
	tariffs := Assumptions(tariffsCfg)
	limitW := siteLimitKw * 1000
	inGroup := func(id string) bool {
		if len(groups) == 0 {
			return true
		}
		g := GroupOf(id)
		for _, x := range groups {
			if x == g {
				return true
			}
		}
		return false
	}

	all, err := ops.SessionsInRange(ctx, from, to, "")
	if err != nil {
		return nil, err
	}
	var sessions []store.Session
	var txIDs []int64
	for _, s := range all {
		if inGroup(s.ChargerID) {
			sessions = append(sessions, s)
			txIDs = append(txIDs, s.TransactionID)
		}
	}
	vehicles, err := master.ListVehicles(ctx)
	if err != nil {
		return nil, err
	}
	vehicleAC := map[int64]store.Vehicle{}
	for _, v := range vehicles {
		vehicleAC[v.ID] = v
	}
	chargers, err := master.ListChargers(ctx)
	if err != nil {
		return nil, err
	}
	chargerKW := map[string]float64{}
	chargerDC := map[string]bool{}
	for _, c := range chargers {
		chargerKW[c.ID] = c.MaxPowerKW
		chargerDC[c.ID] = c.CurrentType == store.CurrentDC
	}
	samplesAll, err := ops.SamplesOf(ctx, txIDs)
	if err != nil {
		return nil, err
	}
	samples := map[int64][]store.MeterSample{}
	for _, s := range samplesAll {
		samples[s.TransactionID] = append(samples[s.TransactionID], s)
	}

	// 15-minute windows
	w0 := (from / min15) * min15
	nW := int(max(1, int64(math.Ceil(float64(to-w0)/float64(min15)))))
	if nW > 6000 {
		return nil, store.Invalid("Period too long (max ~60 days)")
	}
	ctrl := make([]float64, nW) // Wh per window, controlled
	unc := make([]float64, nW)  // Wh per window, uncontrolled
	addSpan := func(arr []float64, t1, t2 int64, w float64) {
		t1 = max(t1, w0)
		t2 = min(t2, w0+int64(nW)*min15)
		if t2 <= t1 || w <= 0 {
			return
		}
		for i := int((t1 - w0) / min15); i < nW; i++ {
			ws := w0 + int64(i)*min15
			if ws >= t2 {
				break
			}
			ov := min(ws+min15, t2) - max(ws, t1)
			if ov > 0 {
				arr[i] += w * float64(ov) / hour
			}
		}
	}

	var energyWh, extraMin float64
	nDur, reached, withTarget := 0, 0, 0
	departures, departuresMet := 0, 0
	for _, s := range sessions {
		b := now
		if s.StoppedAt != nil {
			b = *s.StoppedAt
		}
		// Departure compliance (recorded at the end of the session): departures within the period
		if s.MinSoCMet != nil && s.DepartureAt != nil && *s.DepartureAt >= from && *s.DepartureAt < to {
			departures++
			if *s.MinSoCMet {
				departuresMet++
			}
		}
		capKW := 22.0
		if v, ok := chargerKW[s.ChargerID]; ok {
			capKW = v
		}
		var target *float64
		if s.VehicleID != nil {
			if v, ok := vehicleAC[*s.VehicleID]; ok {
				if !chargerDC[s.ChargerID] {
					capKW = math.Min(capKW, v.MaxACKW)
				} else if v.MaxDCKW != nil {
					capKW = math.Min(capKW, *v.MaxDCKW)
				}
				t := v.TargetSoC
				target = &t
			}
		}
		capW := math.Max(1000, capKW*1000)
		ss := samples[s.TransactionID]
		// Controlled: measured power held until the next sample (or the end of the session)
		wh := 0.0
		for i, smp := range ss {
			t1 := smp.T
			t2 := min(b, t1+15*60000)
			if i+1 < len(ss) {
				t2 = ss[i+1].T
			}
			p := 0.0
			if smp.PowerW != nil {
				p = *smp.PowerW
			}
			addSpan(ctrl, t1, t2, p)
			wh += p * float64(max(0, min(t2, b)-t1)) / hour
		}
		e := wh
		if s.EnergyWh != nil {
			e = *s.EnergyWh // OCPP meter; integrated when missing
		}
		// Uncontrolled: the SAME energy at full power from arrival. The integrated energy is used (same basis as
		// the controlled curve), so the scenario stays comparable even when the simulator accelerates the meter.
		eBase := e
		if len(ss) > 1 {
			eBase = wh
		}
		durUnc := eBase / capW * hour
		addSpan(unc, s.StartedAt, s.StartedAt+int64(durUnc), capW)
		// Energy and metrics only for what falls inside the period
		share := math.Max(0, float64(min(b, to)-max(s.StartedAt, from))) / math.Max(1, float64(b-s.StartedAt))
		energyWh += eBase * share
		if b < now && eBase > 500 {
			extraMin += math.Max(0, (float64(b-s.StartedAt)-durUnc)/60000)
			nDur++
		}
		if s.SoCEnd != nil && target != nil && b < now {
			withTarget++
			if *s.SoCEnd >= *target-1 || (s.StopReason != nil && *s.StopReason == "EVDisconnected") {
				reached++
			}
		}
	}

	// Measured auxiliary loads per window (site samples), only for the live yard
	aux := make([]float64, nW)
	auxN := make([]int, nW)
	includeAux := len(groups) == 0
	for _, g := range groups {
		if g != "HIST" {
			includeAux = true
		}
	}
	if includeAux {
		site, err := ops.SiteSamplesFrom(ctx, w0, to)
		if err != nil {
			return nil, err
		}
		for _, r := range site {
			i := int((r.T - w0) / min15)
			if i >= 0 && i < nW {
				aux[i] += r.BuildingW
				auxN[i]++
			}
		}
	}

	series := make([]Point, 0, nW)
	var peakC, peakU, peakSiteC, peakSiteU, sumC, sumU, shiftedWh, cumC, cumU float64
	active, overU, overC := 0, 0, 0
	for i := 0; i < nW; i++ {
		kwC := ctrl[i] * 4 / 1000 // Wh in 15 min → average kW
		kwU := unc[i] * 4 / 1000
		kwAux := 0.0
		if auxN[i] > 0 {
			kwAux = aux[i] / float64(auxN[i]) / 1000
		}
		cumC += ctrl[i]
		cumU += unc[i]
		shiftedWh = math.Max(shiftedWh, cumU-cumC)
		if kwC > 0.05 || kwU > 0.05 {
			active++
			sumC += kwC
			sumU += kwU
			if (kwU+kwAux)*1000 > limitW+1 {
				overU++
			}
			if (kwC+kwAux)*1000 > limitW+1 {
				overC++
			}
		}
		peakC, peakU = math.Max(peakC, kwC), math.Max(peakU, kwU)
		peakSiteC, peakSiteU = math.Max(peakSiteC, kwC+kwAux), math.Max(peakSiteU, kwU+kwAux)
		series = append(series, Point{T: w0 + int64(i)*min15, ControlledKw: r2(kwC), UncontrolledKw: r2(kwU), AuxKw: r2(kwAux)})
	}

	kWh := energyWh / 1000
	days := math.Max(1.0/96, float64(to-from)/day)
	avgC := 0.0
	if active > 0 {
		avgC = sumC / float64(active)
	}
	peakAvoided := math.Max(0, peakSiteU-peakSiteC)
	evKm := kWh * tariffs["evKmPerKwh"]
	co2Ice := evKm * tariffs["iceCo2KgKm"]
	co2Ev := kWh * tariffs["gridCo2KgKwh"]
	fuelClp := evKm / tariffs["iceKmPerL"] * tariffs["fuelPriceClpL"]
	energyClp := kWh * tariffs["energyPriceClp"]

	// For long periods the chart series is aggregated per hour (max of the 15-min windows)
	chart := series
	if len(series) > 400 {
		chart = nil
		for i := 0; i < len(series); i += 4 {
			p := series[i]
			for _, x := range series[i:min(i+4, len(series))] {
				p.ControlledKw, p.UncontrolledKw, p.AuxKw = math.Max(p.ControlledKw, x.ControlledKw), math.Max(p.UncontrolledKw, x.UncontrolledKw), math.Max(p.AuxKw, x.AuxKw)
			}
			chart = append(chart, p)
		}
	}
	pct := func(a, b float64) float64 {
		if b <= 0 {
			return 0
		}
		return r1(a / b * 100)
	}
	var target any
	if withTarget > 0 {
		target = r1(float64(reached) / float64(withTarget) * 100)
	}
	// % of departures with SoC ≥ minimum (null when no session had a minimum SoC and a departure time)
	var compliancePct any
	if departures > 0 {
		compliancePct = r1(float64(departuresMet) / float64(departures) * 100)
	}
	extra := 0
	if nDur > 0 {
		extra = int(math.Round(extraMin / float64(nDur)))
	}
	loadU := 0.0
	if peakU > 0 && active > 0 {
		loadU = r1(sumU / float64(active) / peakU * 100)
	}
	costKm := 0.0
	if evKm > 0 {
		costKm = r1(energyClp / evKm)
	}
	gs := groups
	if len(gs) == 0 {
		gs = []string{"all"}
	}
	return map[string]any{
		"period":      map[string]any{"from": from, "to": to, "days": r2(days), "groups": gs},
		"assumptions": tariffs,
		"siteLimitKw": siteLimitKw,
		"sessions":    len(sessions),
		"energyKwh":   r1(kWh),
		"demand": map[string]any{
			"peakControlledKw": r1(peakSiteC), "peakUncontrolledKw": r1(peakSiteU), "peakEvControlledKw": r1(peakC),
			"peakEvUncontrolledKw": r1(peakU), "peakAvoidedKw": r1(peakAvoided), "peakReductionPct": pct(peakAvoided, peakSiteU),
			"overLimitUncontrolledPct": pct(float64(overU), float64(active)), "overLimitControlledPct": pct(float64(overC), float64(active)),
			"requiredConnectionUncontrolledKw": r1(peakSiteU),
		},
		"efficiency": map[string]any{
			"loadFactorControlledPct": pct(avgC, peakC), "loadFactorUncontrolledPct": loadU,
			"contractedPowerUsePct": pct(peakSiteC, siteLimitKw), "shiftedEnergyKwh": r1(shiftedWh / 1000),
			"extraMinutesPerSession": extra, "sessionsReachingTargetPct": target,
		},
		"socCompliance": map[string]any{"departures": departures, "met": departuresMet, "pct": compliancePct},
		"economics": map[string]any{
			"energyCostClp": math.Round(energyClp), "demandChargeSavingsMonthClp": math.Round(peakAvoided * tariffs["demandChargeClpKw"]),
			"savingsVsFuelClp": math.Round(fuelClp - energyClp), "costPerKmClp": costKm,
		},
		"environment": map[string]any{
			"electricKm": math.Round(evKm), "co2AvoidedKg": math.Round(co2Ice - co2Ev), "co2CombustionKg": math.Round(co2Ice), "co2GridKg": math.Round(co2Ev),
		},
		"series": chart,
	}, nil
}
