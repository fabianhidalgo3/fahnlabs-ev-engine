// Package analytics computes predictive-maintenance indicators for chargers.
//
// Sources: charger_events (connect/disconnect/fault), ocpp_log (errors, communication gaps),
// sessions and meter_samples (usage, abnormal stops, setpoint tracking).
//
// Methods (documented in the thesis):
//   - Availability = 1 − (disconnected time / observed time).
//   - MTBF = observed hours / incidents (faults + disconnections);  MTTR = down hours / incidents.
//   - Risk 0–100 = weighted sum of fault rate, disconnection rate, availability deficit, OCPP error rate,
//     setpoint-tracking deficit and growing incident trend.
//   - Forecast: daily rate λ = 0.5·EWMA(α=0.3) + 0.5·linear regression projected to mid-horizon;
//     P(≥1 incident in h days) = 1 − e^(−λ·h) (Poisson process). Anomaly: z-score of the last day.
package analytics

import (
	"context"
	"math"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/fhidalgo/emob/motor/internal/store"
)

const (
	day   = int64(86_400_000)
	hour  = int64(3_600_000)
	gapMs = int64(15 * 60_000) // communication gap: >15 min without messages from the charger
)

var abnormalStops = []string{"Other", "PowerLoss", "EmergencyStop", "HardReset", "SoftReset", "Reboot", "ServerRestart", "DeAuthorized", "UnlockCommand"}

// Analytics computes indicators over the operational store.
type Analytics struct {
	ops    *store.Ops
	master *store.Master
}

// New builds the analytics service.
func New(ops *store.Ops, master *store.Master) *Analytics {
	return &Analytics{ops: ops, master: master}
}

func r1(x float64) *float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return nil
	}
	v := math.Round(x*10) / 10
	return &v
}
func r2(x float64) *float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return nil
	}
	v := math.Round(x*100) / 100
	return &v
}
func clamp01(x float64) float64 { return math.Max(0, math.Min(1, x)) }
func mean(a []float64) float64 {
	if len(a) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range a {
		s += x
	}
	return s / float64(len(a))
}
func sd(a []float64) float64 {
	if len(a) < 2 {
		return 0
	}
	m, s := mean(a), 0.0
	for _, x := range a {
		s += (x - m) * (x - m)
	}
	return math.Sqrt(s / float64(len(a)-1))
}

type fit struct{ slope, intercept, r2 float64 }

func linreg(ys []float64) fit {
	n := len(ys)
	if n < 2 {
		if n == 1 {
			return fit{intercept: ys[0]}
		}
		return fit{}
	}
	xs := make([]float64, n)
	for i := range xs {
		xs[i] = float64(i)
	}
	mx, my := mean(xs), mean(ys)
	var sxy, sxx, syy float64
	for i := 0; i < n; i++ {
		sxy += (xs[i] - mx) * (ys[i] - my)
		sxx += (xs[i] - mx) * (xs[i] - mx)
		syy += (ys[i] - my) * (ys[i] - my)
	}
	f := fit{}
	if sxx != 0 {
		f.slope = sxy / sxx
	}
	f.intercept = my - f.slope*mx
	if sxx != 0 && syy != 0 {
		f.r2 = sxy * sxy / (sxx * syy)
	}
	return f
}

func ewma(ys []float64) float64 {
	if len(ys) == 0 {
		return 0
	}
	v := ys[0]
	for _, y := range ys[1:] {
		v = 0.3*y + 0.7*v
	}
	return v
}

// ChargerIDs lists known charger IDs (registry, events or sessions) matching an optional LIKE pattern / prefix.
func (a *Analytics) ChargerIDs(ctx context.Context, pattern string) ([]string, error) {
	re := ".*"
	if pattern != "" {
		p := pattern
		if !containsWildcard(p) {
			p += "%"
		}
		re = store.LikeToRegex(p)
	}
	set := map[string]bool{}
	rows, err := a.master.ListChargers(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		set[r.ID] = true
	}
	for _, c := range []*mongo.Collection{a.ops.ChargerEvents, a.ops.Sessions} {
		res := c.Distinct(ctx, "charger_id", bson.M{"charger_id": bson.M{"$regex": re}})
		var ids []string
		if err := res.Decode(&ids); err != nil {
			return nil, err
		}
		for _, id := range ids {
			set[id] = true
		}
	}
	out := []string{}
	for id := range set {
		if matched(re, id) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out, nil
}

func containsWildcard(s string) bool {
	for _, r := range s {
		if r == '%' || r == '_' {
			return true
		}
	}
	return false
}

type availability struct {
	observedH, downH, availabilityPct, longestH float64
	disconnects                                 int
}

// availability from connect/disconnect events in [from, to); nil when there are no events to tell.
func (a *Analytics) availability(ctx context.Context, id string, from, to int64) (*availability, error) {
	types := bson.M{"$in": []string{"connect", "disconnect"}}
	var before []store.ChargerEvent
	before, err := store.Find[store.ChargerEvent](ctx, a.ops.ChargerEvents, bson.M{"charger_id": id, "t": bson.M{"$lt": from}, "type": types},
		options.Find().SetSort(bson.M{"t": -1}).SetLimit(1))
	if err != nil {
		return nil, err
	}
	evs, err := store.Find[store.ChargerEvent](ctx, a.ops.ChargerEvents, bson.M{"charger_id": id, "t": bson.M{"$gte": from, "$lt": to}, "type": types},
		options.Find().SetSort(bson.M{"t": 1}))
	if err != nil {
		return nil, err
	}
	if len(before) == 0 && len(evs) == 0 {
		return nil, nil
	}
	start := from
	online := false
	if len(before) > 0 {
		online = before[0].Type == "connect"
	} else {
		start = evs[0].T
		online = evs[0].Type == "disconnect" // if it starts with a disconnect, it was online before
	}
	last, down, longest, disc := start, int64(0), int64(0), 0
	for _, e := range evs {
		if !online {
			down += e.T - last
			longest = max(longest, e.T-last)
		}
		if e.Type == "disconnect" {
			disc++
		}
		online = e.Type == "connect"
		last = e.T
	}
	if !online {
		down += to - last
		longest = max(longest, to-last)
	}
	observed := float64(max(1, to-start))
	return &availability{observedH: observed / float64(hour), downH: float64(down) / float64(hour),
		availabilityPct: 100 * (1 - float64(down)/observed), longestH: float64(longest) / float64(hour), disconnects: disc}, nil
}

type faults struct {
	count   int
	hours   float64
	causes  map[string]int
	openNow int
}

func (a *Analytics) faults(ctx context.Context, id string, from, to int64) (faults, error) {
	evs, err := store.Find[store.ChargerEvent](ctx, a.ops.ChargerEvents,
		bson.M{"charger_id": id, "t": bson.M{"$gte": from, "$lt": to}, "type": bson.M{"$in": []string{"fault", "fault_clear"}}},
		options.Find().SetSort(bson.M{"t": 1}))
	f := faults{causes: map[string]int{}}
	if err != nil {
		return f, err
	}
	open := map[int]int64{}
	for _, e := range evs {
		cid := 0
		if e.ConnectorID != nil {
			cid = *e.ConnectorID
		}
		if e.Type == "fault" {
			f.count++
			open[cid] = e.T
			k := "no detail"
			if e.Detail != nil {
				k = *e.Detail
			}
			f.causes[k]++
		} else if t, ok := open[cid]; ok {
			f.hours += float64(e.T-t) / float64(hour)
			delete(open, cid)
		}
	}
	for _, t := range open {
		f.hours += float64(to-t) / float64(hour)
	}
	f.openNow = len(open)
	return f, nil
}

type ocppStats struct {
	msgs, errors, callErrors, rejectedProfiles, commGaps int
	maxGapMin                                            float64
}

func (a *Analytics) ocpp(ctx context.Context, id string, from, to int64) (ocppStats, error) {
	var s ocppStats
	type counts struct {
		Msgs, Errors, CallErrors, RejectedProfiles int
	}
	isErr := bson.M{"$gt": bson.A{"$error_code", nil}}
	c, err := store.Aggregate[counts](ctx, a.ops.OcppLog, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"charger_id": id, "t": bson.M{"$gte": from, "$lt": to}}}},
		{{Key: "$group", Value: bson.M{
			"_id":              nil,
			"msgs":             bson.M{"$sum": 1},
			"errors":           bson.M{"$sum": bson.M{"$cond": bson.A{isErr, 1, 0}}},
			"callerrors":       bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{"$kind", "CALLERROR"}}, 1, 0}}},
			"rejectedprofiles": bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$and": bson.A{bson.M{"$eq": bson.A{"$action", "SetChargingProfile"}}, bson.M{"$ne": bson.A{"$kind", "CALL"}}, isErr}}, 1, 0}}},
		}}},
	})
	if err != nil {
		return s, err
	}
	if len(c) > 0 {
		s.msgs, s.errors, s.callErrors, s.rejectedProfiles = c[0].Msgs, c[0].Errors, c[0].CallErrors, c[0].RejectedProfiles
	}
	type gaps struct {
		N      int     `bson:"n"`
		MaxGap float64 `bson:"maxGap"`
	}
	g, err := store.Aggregate[gaps](ctx, a.ops.OcppLog, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"charger_id": id, "dir": "in", "t": bson.M{"$gte": from, "$lt": to}}}},
		{{Key: "$setWindowFields", Value: bson.M{"sortBy": bson.M{"t": 1}, "output": bson.M{"prev": bson.M{"$shift": bson.M{"output": "$t", "by": -1}}}}}},
		{{Key: "$set", Value: bson.M{"gap": bson.M{"$subtract": bson.A{"$t", "$prev"}}}}},
		{{Key: "$match", Value: bson.M{"gap": bson.M{"$gt": gapMs}}}},
		{{Key: "$group", Value: bson.M{"_id": nil, "n": bson.M{"$sum": 1}, "maxGap": bson.M{"$max": "$gap"}}}},
	})
	if err != nil {
		return s, err
	}
	if len(g) > 0 {
		s.commGaps, s.maxGapMin = g[0].N, g[0].MaxGap/60000
	}
	return s, nil
}

type sessionStats struct {
	n, short, abnormal int
	kWh                float64
	tracking           *float64
	trackingSamples    int
}

func (a *Analytics) sessions(ctx context.Context, id string, from, to int64) (sessionStats, error) {
	var st sessionStats
	ss, err := store.Find[store.Session](ctx, a.ops.Sessions, bson.M{"charger_id": id, "started_at": bson.M{"$gte": from, "$lt": to}, "stopped_at": bson.M{"$ne": nil}}, nil)
	if err != nil {
		return st, err
	}
	abnormal := map[string]bool{}
	for _, r := range abnormalStops {
		abnormal[r] = true
	}
	for _, s := range ss {
		st.n++
		e := 0.0
		if s.EnergyWh != nil {
			e = *s.EnergyWh
		}
		st.kWh += e / 1000
		if *s.StoppedAt-s.StartedAt < 300_000 || e < 500 {
			st.short++
		}
		if s.StopReason != nil && abnormal[*s.StopReason] {
			st.abnormal++
		}
	}
	// Setpoint tracking: delivered / allocated power while the EV is not tapering (SoC < 75 %)
	type tr struct {
		Ratio float64 `bson:"ratio"`
		N     int     `bson:"n"`
	}
	txs, err := a.ops.Sessions.Distinct(ctx, "_id", bson.M{"charger_id": id, "started_at": bson.M{"$lt": to},
		"$or": bson.A{bson.M{"stopped_at": nil}, bson.M{"stopped_at": bson.M{"$gte": from}}}}).Raw()
	if err != nil {
		return st, err
	}
	var ids []int64
	vals, _ := txs.Values()
	for _, v := range vals {
		if n, ok := v.AsInt64OK(); ok {
			ids = append(ids, n)
		} else if n, ok := v.Int32OK(); ok {
			ids = append(ids, int64(n))
		}
	}
	if len(ids) == 0 {
		return st, nil
	}
	t, err := store.Aggregate[tr](ctx, a.ops.MeterSamples, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"transaction_id": bson.M{"$in": ids}, "ts": bson.M{"$gte": bson.DateTime(from), "$lt": bson.DateTime(to)},
			"allocated_w": bson.M{"$gte": 2000}, "$or": bson.A{bson.M{"soc": nil}, bson.M{"soc": bson.M{"$lt": 75}}}}}},
		{{Key: "$group", Value: bson.M{"_id": nil, "n": bson.M{"$sum": 1},
			"ratio": bson.M{"$avg": bson.M{"$min": bson.A{1.2, bson.M{"$divide": bson.A{"$power_w", "$allocated_w"}}}}}}}},
	})
	if err != nil {
		return st, err
	}
	if len(t) > 0 {
		st.trackingSamples = t[0].N
		if t[0].N >= 5 {
			r := t[0].Ratio
			st.tracking = &r
		}
	}
	return st, nil
}

// Point is one bucket of a charger's history.
type Point struct {
	T                int64    `json:"t"`
	Label            string   `json:"label"`
	Incidents        int      `json:"incidents"`
	Faults           int      `json:"faults"`
	Disconnects      int      `json:"disconnects"`
	AvailabilityPct  *float64 `json:"availabilityPct"`
	OcppErrors       int      `json:"ocppErrors"`
	CommGaps         int      `json:"commGaps"`
	Sessions         int      `json:"sessions"`
	AbnormalStops    int      `json:"abnormalStops"`
	KWh              *float64 `json:"kWh"`
	SetpointTracking *float64 `json:"setpointTracking"`
}

// Series returns the history of a charger per "day" or "hour" bucket.
func (a *Analytics) Series(ctx context.Context, id string, from, to int64, bucket string) ([]Point, error) {
	step := day
	d := time.UnixMilli(from)
	if bucket == "hour" {
		step = hour
		d = time.Date(d.Year(), d.Month(), d.Day(), d.Hour(), 0, 0, 0, d.Location())
	} else {
		d = time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, d.Location())
	}
	out := []Point{}
	for t := d.UnixMilli(); t < to && len(out) < 400; t += step {
		lo, hi := max(t, from), min(t+step, to)
		av, err := a.availability(ctx, id, lo, hi)
		if err != nil {
			return nil, err
		}
		f, err := a.faults(ctx, id, lo, hi)
		if err != nil {
			return nil, err
		}
		o, err := a.ocpp(ctx, id, lo, hi)
		if err != nil {
			return nil, err
		}
		s, err := a.sessions(ctx, id, lo, hi)
		if err != nil {
			return nil, err
		}
		label := time.UnixMilli(t).Format("2006-01-02")
		if step == hour {
			label = time.UnixMilli(t).Format("2006-01-02 15:04")
		}
		p := Point{T: t, Label: label, Faults: f.count, OcppErrors: o.errors, CommGaps: o.commGaps, Sessions: s.n, AbnormalStops: s.abnormal, KWh: r1(s.kWh)}
		if av != nil {
			p.Disconnects = av.disconnects
			p.AvailabilityPct = r1(av.availabilityPct)
		}
		p.Incidents = p.Faults + p.Disconnects
		if s.tracking != nil {
			p.SetpointTracking = r2(*s.tracking)
		}
		out = append(out, p)
	}
	return out, nil
}

// Health returns health indicators and the 0–100 risk score of a charger in [from, to).
func (a *Analytics) Health(ctx context.Context, id string, from, to int64) (map[string]any, error) {
	days := math.Max(1.0/24, float64(to-from)/float64(day))
	av, err := a.availability(ctx, id, from, to)
	if err != nil {
		return nil, err
	}
	f, err := a.faults(ctx, id, from, to)
	if err != nil {
		return nil, err
	}
	o, err := a.ocpp(ctx, id, from, to)
	if err != nil {
		return nil, err
	}
	s, err := a.sessions(ctx, id, from, to)
	if err != nil {
		return nil, err
	}
	disconnects := 0
	observedH := days * 24
	if av != nil {
		disconnects, observedH = av.disconnects, av.observedH
	}
	incidents := f.count + disconnects

	// Incident trend (per day) inside the window
	var daily []float64
	if days >= 3 {
		ser, err := a.Series(ctx, id, from, to, "day")
		if err != nil {
			return nil, err
		}
		for _, p := range ser {
			daily = append(daily, float64(p.Incidents))
		}
	}
	trend := linreg(daily)
	errRate := 0.0
	if o.msgs > 0 {
		errRate = float64(o.errors) / float64(o.msgs) * 1000
	}
	// Risk rates use the LAST 7 days of the window (current condition), so a long window with a healthy earlier
	// period does not dilute a recent degradation; the trend uses the whole window.
	rf, rav, ro, rs, rdays := f, av, o, s, days
	if days > 7 {
		rfrom := to - 7*day
		if rf, err = a.faults(ctx, id, rfrom, to); err != nil {
			return nil, err
		}
		if rav, err = a.availability(ctx, id, rfrom, to); err != nil {
			return nil, err
		}
		if ro, err = a.ocpp(ctx, id, rfrom, to); err != nil {
			return nil, err
		}
		if rs, err = a.sessions(ctx, id, rfrom, to); err != nil {
			return nil, err
		}
		rdays = 7
	}
	rErrRate := 0.0
	if ro.msgs > 0 {
		rErrRate = float64(ro.errors) / float64(ro.msgs) * 1000
	}
	sumDaily := 0.0
	for _, x := range daily {
		sumDaily += x
	}
	enoughTrend := len(daily) >= 3 && sumDaily >= 3
	comp := []struct {
		k string
		v float64
	}{
		{"faults", clamp01(float64(rf.count)/rdays) * 30},
		{"disconnects", 0},
		{"availability", 0},
		{"ocppErrors", clamp01(rErrRate/20) * 10},
		{"setpointTracking", 0},
		{"trend", 0},
	}
	if rav != nil {
		comp[1].v = clamp01(float64(rav.disconnects)/rdays/2) * 20
		comp[2].v = clamp01((100-rav.availabilityPct)/10) * 15
	}
	if rs.tracking != nil {
		comp[4].v = clamp01((0.95-*rs.tracking)/0.2) * 10
	}
	if enoughTrend {
		comp[5].v = clamp01(trend.slope/0.2) * 15
	}
	total := 0.0
	for _, x := range comp {
		total += x.v
	}
	risk := int(math.Round(total))
	level := "low"
	switch {
	case risk >= 75:
		level = "critical"
	case risk >= 50:
		level = "high"
	case risk >= 25:
		level = "medium"
	}
	sort.SliceStable(comp, func(i, j int) bool { return comp[i].v > comp[j].v })
	factors := []map[string]any{}
	for _, x := range comp {
		if x.v >= 1 {
			factors = append(factors, map[string]any{"factor": x.k, "points": int(math.Round(x.v))})
		}
	}
	out := map[string]any{
		"chargerId": id, "windowDays": r1(days), "risk": risk, "level": level, "riskFactors": factors,
		"disconnects": disconnects, "faults": f.count, "openFaultsNow": f.openNow, "faultHours": r1(f.hours), "faultCauses": f.causes,
		"mtbfHours": r1(observedH / math.Max(1, float64(incidents))), "incidentsPerDay": r2(float64(incidents) / days),
		"ocppMessages": o.msgs, "ocppErrors": o.errors, "errorsPerThousandMessages": r1(errRate), "callErrors": o.callErrors,
		"rejectedProfiles": o.rejectedProfiles, "commGaps": o.commGaps, "maxGapMinutes": r1(o.maxGapMin),
		"sessions": s.n, "kWh": r1(s.kWh), "shortSessions": s.short, "abnormalStops": s.abnormal,
		"availabilityPct": nil, "downHours": nil, "longestOutageHours": nil, "mttrHours": nil,
		"incidentTrendPerDay": nil, "meanDailyIncidents": nil, "setpointTracking": nil,
		"riskMeasuredOver": "whole window",
	}
	if av != nil {
		out["availabilityPct"], out["downHours"], out["longestOutageHours"] = r2(av.availabilityPct), r1(av.downH), r1(av.longestH)
	}
	if incidents > 0 {
		down := f.hours
		if av != nil {
			down += av.downH
		}
		out["mttrHours"] = r2(down / float64(incidents))
	}
	if len(daily) >= 3 {
		out["incidentTrendPerDay"] = r2(trend.slope)
	}
	if len(daily) > 0 {
		out["meanDailyIncidents"] = r2(mean(daily))
	}
	if s.tracking != nil {
		out["setpointTracking"] = r2(*s.tracking)
	}
	if days > 7 {
		out["riskMeasuredOver"] = "last-7-days rates; trend over the whole window"
		l7 := map[string]any{"faults": rf.count, "disconnects": 0, "availabilityPct": nil, "errorsPerThousandMessages": r1(rErrRate), "setpointTracking": nil}
		if rav != nil {
			l7["disconnects"], l7["availabilityPct"] = rav.disconnects, r2(rav.availabilityPct)
		}
		if rs.tracking != nil {
			l7["setpointTracking"] = r2(*rs.tracking)
		}
		out["last7Days"] = l7
	}
	return out, nil
}

// Predict forecasts incidents over horizonDays from the daily series of the last windowDays.
func (a *Analytics) Predict(ctx context.Context, id string, horizonDays, windowDays float64) (map[string]any, error) {
	to := time.Now().UnixMilli()
	ser, err := a.Series(ctx, id, to-int64(windowDays*float64(day)), to, "day")
	if err != nil {
		return nil, err
	}
	// Only days with data for this charger (from its first activity)
	first := -1
	for i, x := range ser {
		if x.AvailabilityPct != nil || x.Sessions > 0 || x.Incidents > 0 || x.OcppErrors > 0 {
			first = i
			break
		}
	}
	var s []Point
	if first >= 0 {
		s = ser[first:]
	}
	n := len(s)
	var inc, flt, trk []float64
	for _, x := range s {
		inc = append(inc, float64(x.Incidents))
		flt = append(flt, float64(x.Faults))
		if x.SetpointTracking != nil {
			trk = append(trk, *x.SetpointTracking)
		}
	}
	h := horizonDays
	type est struct{ lambda, slope, r2, ewma float64 }
	estimate := func(ys []float64) est {
		if len(ys) == 0 {
			return est{}
		}
		lr := linreg(ys)
		ew := ewma(ys)
		proj := math.Max(0, lr.intercept+lr.slope*(float64(len(ys)-1)+h/2))
		return est{lambda: math.Max(0, 0.5*ew+0.5*proj), slope: lr.slope, r2: lr.r2, ewma: ew}
	}
	I, F := estimate(inc), estimate(flt)

	// Anomaly of the last day compared with the previous ones
	var z *float64
	if n >= 5 {
		prev := inc[:n-1]
		v := (inc[n-1] - mean(prev)) / math.Max(0.5, sd(prev))
		z = &v
	}
	m := mean(inc)
	weekChange := 0.0
	switch {
	case m > 0:
		weekChange = I.slope * 7 / m
	case I.slope > 0:
		weekChange = 1
	}
	events := 0.0
	for _, x := range inc {
		events += x
	}
	// With fewer than 3 incidents there is no basis for a trend
	trend := "stable"
	if events >= 3 {
		if weekChange > 0.2 {
			trend = "worsening"
		} else if weekChange < -0.2 {
			trend = "improving"
		}
	}
	// Days until 1 incident/day is exceeded if the trend continues
	var daysTo *float64
	current := math.Max(0, I.ewma)
	if I.slope > 0.005 && current < 1 {
		daysTo = r1((1 - current) / I.slope)
	}
	// Setpoint tracking degradation (delivered / allocated power)
	var tracking map[string]any
	below85 := false
	if len(trk) >= 3 {
		lr := linreg(trk)
		last := trk[len(trk)-1]
		below85 = last < 0.85
		tracking = map[string]any{"current": r2(last), "trendPerDay": math.Round(lr.slope*10000) / 10000, "below85pct": below85, "daysTo85pct": nil}
		if lr.slope < -0.001 && last > 0.85 {
			tracking["daysTo85pct"] = r1((0.85 - last) / lr.slope)
		}
	}
	confidence := "high"
	switch {
	case n < 7 || events < 5:
		confidence = "low"
	case n < 20 || events < 20:
		confidence = "medium"
	}
	pAny := 1 - math.Exp(-I.lambda*h)
	pFault := 1 - math.Exp(-F.lambda*h)
	rec := "no-action"
	switch {
	case pFault >= 0.8 || (z != nil && *z >= 3) || below85:
		rec = "corrective-maintenance-asap"
	case pFault >= 0.5 || trend == "worsening":
		rec = "preventive-inspection-soon"
	case pAny >= 0.5:
		rec = "monitor"
	}
	daily := []map[string]any{}
	for _, x := range s {
		daily = append(daily, map[string]any{"day": x.Label, "incidents": x.Incidents, "faults": x.Faults, "availabilityPct": x.AvailabilityPct, "tracking": x.SetpointTracking})
	}
	out := map[string]any{
		"chargerId": id, "horizonDays": h, "daysAnalyzed": n, "incidentsInWindow": int(events),
		"incidentRatePerDay": r2(I.lambda), "incidentProbability": r2(pAny), "expectedIncidents": r1(I.lambda * h),
		"faultRatePerDay": r2(F.lambda), "faultProbability": r2(pFault), "trend": trend, "incidentSlopePerDay": r2(I.slope),
		"fitR2": r2(I.r2), "daysToOneIncidentPerDay": daysTo, "lastDayAnomalyZ": nil, "setpointTracking": tracking,
		"confidence": confidence, "recommendation": rec,
		"method": "λ = 0.5·EWMA(α=0.3) + 0.5·linear regression projected to mid-horizon; P(≥1) = 1 − e^(−λ·h) (Poisson). " +
			"Anomaly: z-score of the last day vs the previous ones. Incident = fault (Faulted) or disconnection.",
		"dailySeries": daily,
	}
	if z != nil {
		out["lastDayAnomalyZ"] = r1(*z)
	}
	return out, nil
}
