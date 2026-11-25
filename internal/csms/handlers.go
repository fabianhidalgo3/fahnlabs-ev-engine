package csms

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/fhidalgo/emob/motor/internal/gateway"
	"github.com/fhidalgo/emob/motor/internal/store"
)

// ---------- OCPP gateway events (the motor no longer holds charger sockets) ----------

// OnHello reconciles the connected chargers after (re)attaching to the gateway stream.
func (c *Csms) OnHello(connected []string) {
	set := map[string]bool{}
	for _, id := range connected {
		set[id] = true
	}
	c.mu.Lock()
	var gone []string
	for id, ch := range c.chargers {
		if ch.Connected && !set[id] {
			gone = append(gone, id)
		}
	}
	var fresh []string
	for _, id := range connected {
		if ch := c.chargers[id]; ch == nil || !ch.Connected {
			fresh = append(fresh, id)
		}
	}
	c.mu.Unlock()
	for _, id := range gone {
		c.OnDisconnected(id)
	}
	for _, id := range fresh {
		c.OnConnected(id)
	}
}

// OnConnected registers a new charger connection (or rejects it when unregistered chargers are not accepted).
func (c *Csms) OnConnected(id string) {
	c.mu.Lock()
	if !c.cfg.AcceptUnknownChargers && c.chargers[id] == nil {
		c.mu.Unlock()
		c.AddLog("warn", "Connection rejected: charger not registered", id)
		ctx, cancel := bg()
		defer cancel()
		_ = c.gw.Disconnect(ctx, id, "Charger not registered", true)
		return
	}
	ch := c.ensureCharger(id)
	ch.conn = c.gw.Link(id)
	ch.Connected = true
	ch.LastSeen = time.Now().UnixMilli()
	c.mu.Unlock()
	c.AddLog("info", "Charge point connected", id)
	c.chargerEvent(id, "connect", nil, "")
}

// OnDisconnected marks a charger offline; its last setpoints stay reserved.
func (c *Csms) OnDisconnected(id string) {
	c.mu.Lock()
	ch := c.chargers[id]
	if ch == nil || !ch.Connected {
		c.mu.Unlock()
		return
	}
	ch.conn = nil
	ch.Connected = false
	active := false
	for _, k := range ch.Connectors {
		if k.TransactionID != 0 {
			active = true
		}
	}
	row := c.chargerRow(ch)
	c.mu.Unlock()
	c.persist(row)
	c.AddLog("warn", "Charge point disconnected", id)
	detail := ""
	if active {
		detail = "with active session"
	}
	c.chargerEvent(id, "disconnect", nil, detail)
	c.Rebalance("disconnection", 300*time.Millisecond)
}

// OnStreamDown: without the gateway no charger can be reached; treat them as disconnected until the next hello.
func (c *Csms) OnStreamDown() {
	c.mu.Lock()
	var ids []string
	for id, ch := range c.chargers {
		if ch.Connected {
			ids = append(ids, id)
		}
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.OnDisconnected(id)
	}
}

// OnCall answers a CALL sent by a charger.
func (c *Csms) OnCall(id, action string, payload json.RawMessage) (any, error) {
	c.mu.Lock()
	ch := c.chargers[id]
	known := ch != nil && ch.Connected
	c.mu.Unlock()
	if !known {
		c.OnConnected(id) // a CALL can arrive before the "connected" event is processed
		c.mu.Lock()
		ch = c.chargers[id]
		c.mu.Unlock()
		if ch == nil || !ch.Connected {
			return nil, gateway.NewError("SecurityError", "Charger not registered")
		}
	}
	return c.handle(ch, action, payload)
}

// OnFrame logs every OCPP frame reported by the gateway.
func (c *Csms) OnFrame(id, dir string, frame []json.RawMessage, _ int64) {
	if len(frame) >= 3 {
		c.trace(id, dir, frame)
	}
}

var _ gateway.Handler = (*Csms)(nil)
var _ = context.Background

// trace logs every OCPP frame (memory + Mongo), correlating answers with the action of their request.
func (c *Csms) trace(id, dir string, frame []json.RawMessage) {
	t := time.Now().UnixMilli()
	var typ int
	var msgID string
	_ = json.Unmarshal(frame[0], &typ)
	_ = json.Unmarshal(frame[1], &msgID)
	kind := map[int]string{2: "CALL", 3: "CALLRESULT", 4: "CALLERROR"}[typ]
	key := id + "|" + msgID
	var action string
	var payload json.RawMessage
	c.mu.Lock()
	if typ == 2 {
		_ = json.Unmarshal(frame[2], &action)
		c.actionByID[key] = action
		if len(c.actionByID) > 20000 {
			for k := range c.actionByID {
				delete(c.actionByID, k)
				break
			}
		}
		if len(frame) > 3 {
			payload = frame[3]
		}
	} else {
		action = c.actionByID[key]
		delete(c.actionByID, key)
		if typ == 3 {
			payload = frame[2]
		} else {
			var code, desc string
			_ = json.Unmarshal(frame[2], &code)
			if len(frame) > 3 {
				_ = json.Unmarshal(frame[3], &desc)
			}
			payload, _ = json.Marshal(map[string]string{"code": code, "description": desc})
		}
	}
	// A CALLERROR or an answer with a non-accepted status is flagged as an error
	var errCode *string
	var p struct {
		Status    string `json:"status"`
		ErrorCode string `json:"errorCode"`
		IDTagInfo *struct {
			Status string `json:"status"`
		} `json:"idTagInfo"`
		Code string `json:"code"`
	}
	_ = json.Unmarshal(payload, &p)
	switch {
	case typ == 4:
		e := p.Code
		if e == "" {
			e = "CALLERROR"
		}
		errCode = &e
	case typ == 3:
		st := p.Status
		if st == "" && p.IDTagInfo != nil {
			st = p.IDTagInfo.Status
		}
		if st != "" && st != "Accepted" && st != "Unlocked" && st != "Unknown" {
			errCode = &st
		}
	case action == "StatusNotification" && p.ErrorCode != "" && p.ErrorCode != "NoError":
		e := p.ErrorCode
		errCode = &e
	}
	text, _ := json.Marshal(frame)
	label := action
	if label == "" {
		label = kind
	}
	cid := id
	c.log = append(c.log, LogLine{T: t, Level: "ocpp", Dir: dir, ChargerID: &cid, Msg: string(text), Action: label})
	if len(c.log) > 300 {
		c.log = c.log[len(c.log)-300:]
	}
	c.mu.Unlock()
	ps := string(payload)
	if ps == "" {
		ps = "{}"
	}
	if len(ps) > 4000 {
		ps = ps[:4000] + "…"
	}
	c.ops.AddOcpp(store.OcppEntry{T: t, ChargerID: id, Dir: dir, Kind: kind, Action: store.Str(action), MsgID: msgID, ErrorCode: errCode, Payload: ps})
	c.changed()
}

// ---------- OCPP messages ----------

type sampledValue struct {
	Value     string `json:"value"`
	Measurand string `json:"measurand"`
	Phase     string `json:"phase"`
	Unit      string `json:"unit"`
}

var vidTag = regexp.MustCompile(`(?i)^VID:`)

func ptr[T any](v T) *T { return &v }

func (c *Csms) handle(ch *Charger, action string, raw json.RawMessage) (any, error) {
	c.mu.Lock()
	ch.LastSeen = time.Now().UnixMilli()
	c.mu.Unlock()
	now := func() string { return time.Now().UTC().Format(time.RFC3339Nano) }

	switch action {
	case "BootNotification":
		var p struct {
			Vendor, Model, Serial, BoxSerial, Firmware string
		}
		var j map[string]any
		_ = json.Unmarshal(raw, &j)
		s := func(k string) string { v, _ := j[k].(string); return v }
		p.Vendor, p.Model, p.Serial, p.BoxSerial, p.Firmware = s("chargePointVendor"), s("chargePointModel"), s("chargePointSerialNumber"), s("chargeBoxSerialNumber"), s("firmwareVersion")
		c.mu.Lock()
		ch.Vendor, ch.Model, ch.Firmware = p.Vendor, p.Model, p.Firmware
		ch.Serial = p.Serial
		if ch.Serial == "" {
			ch.Serial = p.BoxSerial
		}
		row := c.chargerRow(ch)
		c.mu.Unlock()
		c.persist(row)
		c.AddLog("info", fmt.Sprintf("BootNotification %s %s", p.Vendor, p.Model), ch.ID)
		c.chargerEvent(ch.ID, "boot", nil, p.Firmware)
		time.AfterFunc(400*time.Millisecond, func() { c.afterBoot(ch) })
		return map[string]any{"status": "Accepted", "currentTime": now(), "interval": 30}, nil

	case "Heartbeat":
		return map[string]any{"currentTime": now()}, nil

	case "StatusNotification":
		var p struct {
			ConnectorID     int    `json:"connectorId"`
			Status          string `json:"status"`
			ErrorCode       string `json:"errorCode"`
			VendorErrorCode string `json:"vendorErrorCode"`
			Info            string `json:"info"`
		}
		_ = json.Unmarshal(raw, &p)
		c.mu.Lock()
		if p.ConnectorID == 0 {
			ch.Status = p.Status
			c.mu.Unlock()
		} else {
			k := c.connector(ch, p.ConnectorID)
			prev := k.Status
			k.Status = p.Status
			k.ErrorCode = p.ErrorCode
			if k.ErrorCode == "" {
				k.ErrorCode = "NoError"
			}
			if (p.Status == "Available" || p.Status == "Faulted" || p.Status == "Unavailable") && k.TransactionID == 0 {
				k.PowerW, k.AllocatedW = 0, 0
				if p.Status == "Available" {
					// EV unplugged: forget the identification
					k.Vehicle, k.IdentMethod, k.vehicleInfo = nil, "", nil
				}
			}
			c.mu.Unlock()
			if prev != p.Status {
				c.AddLog("info", fmt.Sprintf("Connector %d: %s → %s", p.ConnectorID, prev, p.Status), ch.ID)
				cid := p.ConnectorID
				switch {
				case p.Status == "Faulted":
					var parts []string
					for _, s := range []string{p.ErrorCode, p.VendorErrorCode, p.Info} {
						if s != "" {
							parts = append(parts, s)
						}
					}
					c.chargerEvent(ch.ID, "fault", &cid, strings.Join(parts, " · "))
				case prev == "Faulted":
					c.chargerEvent(ch.ID, "fault_clear", &cid, "")
				}
				if p.Status == "Unavailable" {
					c.chargerEvent(ch.ID, "unavailable", &cid, "")
				}
			}
		}
		c.Rebalance("status", 300*time.Millisecond)
		return map[string]any{}, nil

	case "Authorize":
		return map[string]any{"idTagInfo": map[string]any{"status": "Accepted"}}, nil

	case "StartTransaction":
		return c.startTransaction(ch, raw)

	case "StopTransaction":
		return c.stopTransaction(ch, raw)

	case "MeterValues":
		return c.meterValues(ch, raw)

	case "DataTransfer":
		return c.dataTransfer(ch, raw)

	case "DiagnosticsStatusNotification", "FirmwareStatusNotification":
		return map[string]any{}, nil
	}
	return nil, gateway.NewError("NotImplemented", "Action not supported: "+action)
}

func (c *Csms) startTransaction(ch *Charger, raw json.RawMessage) (any, error) {
	var p struct {
		ConnectorID int     `json:"connectorId"`
		IDTag       string  `json:"idTag"`
		MeterStart  float64 `json:"meterStart"`
		Timestamp   string  `json:"timestamp"`
	}
	_ = json.Unmarshal(raw, &p)
	ctx, cancel := bg()
	defer cancel()

	c.mu.Lock()
	k := c.connector(ch, p.ConnectorID)
	c.nextTx++
	txID := c.nextTx
	resetTx(k)
	started := time.Now().UnixMilli()
	if ts, err := time.Parse(time.RFC3339Nano, p.Timestamp); err == nil {
		started = ts.UnixMilli()
	}
	k.TransactionID, k.IDTag, k.StartedAt, k.MeterStartWh, k.MeterWh, k.EnergyWh = txID, p.IDTag, started, p.MeterStart, p.MeterStart, 0
	var vin, evccid string
	if k.vehicleInfo != nil && time.Since(k.vehicleInfo.t) < vehicleInfoTTL {
		vin, evccid = k.vehicleInfo.vin, k.vehicleInfo.evccid
	}
	pp := k.pendingPlan
	k.pendingPlan = nil
	c.mu.Unlock()

	// Linking: EV info (VIN/EVCCID via DataTransfer, ISO 15118) > idTag (VID:EVCCID or RFID)
	found, _ := c.master.FindVehicle(ctx, vin, evccid, p.IDTag)
	var vehicle *store.Vehicle
	method := "none"
	if found != nil {
		vehicle, method = found.Vehicle, found.Method
		// EVCCID + idTag "VID:…" = Autocharge (distinct from ISO 15118 / DIN without EVCCID authorisation)
		if method == "evccid" && vidTag.MatchString(p.IDTag) {
			method = "autocharge"
		}
	}
	// Charging plan that started this session
	var plan *store.Plan
	if pp != nil {
		if pl, _ := c.ops.GetPlan(ctx, pp.id); pl != nil && pl.Status == "starting" {
			_ = c.ops.SetPlanStatus(ctx, pl.ID, "active", &txID, "")
			pl.Status, pl.TransactionID = "active", &txID
			plan = pl
			// If the EV did not identify itself, link the plan's vehicle
			if vehicle == nil && pl.VehicleID != nil {
				if v, _ := c.master.GetVehicle(ctx, *pl.VehicleID); v != nil {
					vehicle, method = v, "plan"
				}
			}
		}
	}
	c.mu.Lock()
	k.Vehicle, k.IdentMethod, k.Plan = vehicle, method, plan
	c.mu.Unlock()

	s := store.Session{TransactionID: txID, ChargerID: ch.ID, ConnectorID: p.ConnectorID, IDTag: store.Str(p.IDTag),
		IdentMethod: &method, StartedAt: started, MeterStartWh: &p.MeterStart}
	if vehicle != nil {
		s.VehicleID, s.VehicleName, s.VehicleVIN, s.VehicleEVCCID = &vehicle.ID, &vehicle.Name, vehicle.VIN, vehicle.EVCCID
	}
	if err := c.ops.StartSession(ctx, s); err != nil {
		c.AddLog("error", "Could not store session: "+err.Error(), ch.ID)
	}
	who := "unidentified vehicle"
	if vehicle != nil {
		who = fmt.Sprintf("vehicle %s [%s]", vehicle.Name, strings.ToUpper(method))
	}
	c.AddLog("info", fmt.Sprintf("Session #%d started (connector %d, tag %s), %s", txID, p.ConnectorID, p.IDTag, who), ch.ID)
	c.markListsDirty()
	c.Rebalance("session start", 300*time.Millisecond)
	return map[string]any{"transactionId": txID, "idTagInfo": map[string]any{"status": "Accepted"}}, nil
}

func (c *Csms) stopTransaction(ch *Charger, raw json.RawMessage) (any, error) {
	var p struct {
		TransactionID int64   `json:"transactionId"`
		MeterStop     float64 `json:"meterStop"`
		Timestamp     string  `json:"timestamp"`
		Reason        string  `json:"reason"`
	}
	_ = json.Unmarshal(raw, &p)
	if p.Reason == "" {
		p.Reason = "Local"
	}
	ctx, cancel := bg()
	defer cancel()
	c.mu.Lock()
	var k *Connector
	for _, x := range ch.Connectors {
		if x.TransactionID == p.TransactionID {
			k = x
		}
	}
	if k == nil {
		c.mu.Unlock()
		c.AddLog("warn", fmt.Sprintf("StopTransaction for unknown transaction #%d", p.TransactionID), ch.ID)
		c.Rebalance("session end", 0)
		return map[string]any{"idTagInfo": map[string]any{"status": "Accepted"}}, nil
	}
	energy := math.Max(0, p.MeterStop-k.MeterStartWh)
	socStart, socEnd, maxP, plan := k.SoCStart, k.SoC, k.MaxPowerW, k.Plan
	minSoC := minSoCOf(k.Vehicle, plan)
	resetTx(k)
	c.mu.Unlock()

	stopped := time.Now().UnixMilli()
	if ts, err := time.Parse(time.RFC3339Nano, p.Timestamp); err == nil {
		stopped = ts.UnixMilli()
	}
	_ = c.ops.StopSession(ctx, p.TransactionID, stopped, p.MeterStop, energy, socStart, socEnd, &maxP, p.Reason,
		c.departureOutcome(ctx, p.TransactionID, stopped, socEnd, minSoC, plan))
	if plan != nil {
		reached := plan.TargetSoC != nil && socEnd != nil && *socEnd >= *plan.TargetSoC
		note := "Target SoC reached"
		if !reached {
			note = "Stopped (" + p.Reason + ")"
			if plan.DepartureAt != nil && time.Now().UnixMilli() > *plan.DepartureAt {
				note += " after the departure time"
			}
		}
		_ = c.ops.SetPlanStatus(ctx, plan.ID, "completed", nil, note)
	}
	c.AddLog("info", fmt.Sprintf("Session #%d ended: %.2f kWh (%s)", p.TransactionID, energy/1000, p.Reason), ch.ID)
	c.markListsDirty()
	c.Rebalance("session end", 0)
	return map[string]any{"idTagInfo": map[string]any{"status": "Accepted"}}, nil
}

// departureOutcome judges the departure minimum of a closing session (KPI "% of departures with SoC ≥ minimum").
// It applies when a minimum SoC (plan, else vehicle) and a departure time (plan) are known. The SoC at departure is
// the final SoC when the session ended before the departure, else the last sample at or before the departure.
func (c *Csms) departureOutcome(ctx context.Context, txID, stopped int64, socEnd, minSoC *float64, plan *store.Plan) store.SessionOutcome {
	out := store.SessionOutcome{MinSoC: minSoC}
	if plan == nil || plan.DepartureAt == nil {
		return out
	}
	out.DepartureAt = plan.DepartureAt
	if minSoC == nil {
		return out
	}
	soc := socEnd
	if stopped > *plan.DepartureAt {
		if s := c.ops.SoCAt(ctx, txID, *plan.DepartureAt); s != nil {
			soc = s
		}
	}
	if soc == nil {
		return out
	}
	met := *soc >= *minSoC
	out.SoCDeparture, out.MinSoCMet = soc, &met
	return out
}

func (c *Csms) meterValues(ch *Charger, raw json.RawMessage) (any, error) {
	var p struct {
		ConnectorID   int   `json:"connectorId"`
		TransactionID int64 `json:"transactionId"`
		MeterValue    []struct {
			SampledValue []sampledValue `json:"sampledValue"`
		} `json:"meterValue"`
	}
	_ = json.Unmarshal(raw, &p)
	c.mu.Lock()
	k := c.connector(ch, p.ConnectorID)
	if p.TransactionID != 0 && p.TransactionID != k.TransactionID {
		c.mu.Unlock()
		c.stopOrphan(ch, p.TransactionID)
		return map[string]any{}, nil
	}
	for _, mv := range p.MeterValue {
		applySampledValues(k, mv.SampledValue)
	}
	k.LastMeterAt = time.Now()
	var sample *store.MeterSample
	var stopTx int64
	var planID int64
	var target float64
	if k.TransactionID != 0 {
		if k.SoCStart == nil && k.SoC != nil {
			k.SoCStart = ptr(*k.SoC)
		}
		k.MaxPowerW = math.Max(k.MaxPowerW, k.PowerW)
		alloc := k.AllocatedW
		sample = &store.MeterSample{TransactionID: k.TransactionID, T: k.LastMeterAt.UnixMilli(), PowerW: ptr(k.PowerW), AllocatedW: &alloc,
			EnergyWh: ptr(k.EnergyWh), SoC: k.SoC, CurrentA: k.CurrentA}
		// Charging plan: stop when the target SoC is reached
		if k.Plan != nil && k.Plan.TargetSoC != nil && k.SoC != nil && *k.SoC >= *k.Plan.TargetSoC && !k.targetStop && ch.conn != nil {
			k.targetStop = true
			stopTx, planID, target = k.TransactionID, k.Plan.ID, *k.Plan.TargetSoC
		}
	}
	conn := ch.conn
	c.mu.Unlock()
	if sample != nil {
		c.ops.AddMeterSample(*sample)
	}
	if stopTx != 0 {
		c.AddLog("info", fmt.Sprintf("Plan #%d: target SoC %.0f %% reached, stopping session #%d", planID, target, stopTx), ch.ID)
		go func() {
			time.Sleep(100 * time.Millisecond)
			ctx, cancel := bg()
			defer cancel()
			_, _ = conn.Call(ctx, "RemoteStopTransaction", map[string]any{"transactionId": stopTx})
		}()
	}
	c.changed()
	return map[string]any{}, nil
}

func applySampledValues(k *Connector, values []sampledValue) {
	num := func(v sampledValue) float64 {
		var f float64
		_, _ = fmt.Sscan(v.Value, &f)
		return f
	}
	var powerTotal *float64
	powerPhases, hasPhase := 0.0, false
	var current *float64
	for _, v := range values {
		m := v.Measurand
		if m == "" {
			m = "Energy.Active.Import.Register"
		}
		switch m {
		case "Power.Active.Import":
			w := num(v)
			if v.Unit == "kW" {
				w *= 1000
			}
			if v.Phase != "" {
				hasPhase = true
				powerPhases += w
			} else {
				powerTotal = &w
			}
		case "Energy.Active.Import.Register":
			wh := num(v)
			if v.Unit == "kWh" {
				wh *= 1000
			}
			k.MeterWh = wh
			if k.TransactionID != 0 {
				k.EnergyWh = math.Max(0, wh-k.MeterStartWh)
			}
		case "SoC":
			s := num(v)
			k.SoC = &s
		case "Current.Import":
			a := num(v)
			if v.Phase != "" && current != nil {
				a = math.Max(*current, a)
			}
			current = &a
		}
	}
	if current != nil {
		k.CurrentA = current
	}
	if powerTotal != nil {
		k.PowerW = *powerTotal
	} else if hasPhase {
		k.PowerW = powerPhases
	}
}

func (c *Csms) dataTransfer(ch *Charger, raw json.RawMessage) (any, error) {
	var p struct {
		VendorID  string          `json:"vendorId"`
		MessageID string          `json:"messageId"`
		Data      json.RawMessage `json:"data"`
	}
	_ = json.Unmarshal(raw, &p)
	var d struct {
		MaxPowerW   float64 `json:"maxPowerW"`
		ConnectorID float64 `json:"connectorId"`
		VIN         string  `json:"vin"`
		EVCCID      string  `json:"evccid"`
	}
	// data may be a JSON string (as OCPP defines it) or an object
	var s string
	if json.Unmarshal(p.Data, &s) == nil {
		_ = json.Unmarshal([]byte(s), &d)
	} else {
		_ = json.Unmarshal(p.Data, &d)
	}
	switch p.MessageID {
	case "Capabilities": // extension: the charger announces its maximum power
		c.mu.Lock()
		if d.MaxPowerW <= 0 || ch.ManualMax {
			c.mu.Unlock()
			break
		}
		ch.MaxPowerKw = d.MaxPowerW / 1000
		row := c.chargerRow(ch)
		c.mu.Unlock()
		c.persist(row)
		c.AddLog("info", fmt.Sprintf("Announced capacity: %g kW", d.MaxPowerW/1000), ch.ID)
	case "VehicleInfo": // extension: EV data from high-level communication (ISO 15118 / DIN 70121)
		if d.ConnectorID <= 0 {
			break
		}
		ctx, cancel := bg()
		defer cancel()
		c.mu.Lock()
		k := c.connector(ch, int(d.ConnectorID))
		k.vehicleInfo = &vehicleInfo{vin: d.VIN, evccid: d.EVCCID, t: time.Now()}
		inTx, hasVehicle, txID := k.TransactionID != 0, k.Vehicle != nil, k.TransactionID
		c.mu.Unlock()
		found, _ := c.master.FindVehicle(ctx, d.VIN, d.EVCCID, "")
		switch {
		case found == nil:
			c.AddLog("warn", fmt.Sprintf("Unknown EV (VIN %s, EVCCID %s)", dash(d.VIN), dash(d.EVCCID)), ch.ID)
		case inTx && !hasVehicle:
			c.mu.Lock()
			k.Vehicle, k.IdentMethod = found.Vehicle, found.Method
			c.mu.Unlock()
			_ = c.ops.LinkSessionVehicle(ctx, txID, found.Vehicle, found.Method)
			c.AddLog("info", fmt.Sprintf("Vehicle identified by %s: %s (connector %d, during the session)", strings.ToUpper(found.Method), found.Vehicle.Name, int(d.ConnectorID)), ch.ID)
		default:
			c.AddLog("info", fmt.Sprintf("EV connected: %s (%s)", found.Vehicle.Name, strings.ToUpper(found.Method)), ch.ID)
		}
		c.Rebalance("vehicle", 300*time.Millisecond)
	}
	return map[string]any{"status": "Accepted"}, nil
}

func dash(s string) string {
	if s == "" {
		return "–"
	}
	return s
}

// stopOrphan asks the charger, once, to stop a transaction unknown to the CSMS (e.g. after a server restart),
// so no energy is left unaccounted or uncontrolled.
func (c *Csms) stopOrphan(ch *Charger, txID int64) {
	key := fmt.Sprintf("%s#%d", ch.ID, txID)
	c.mu.Lock()
	if c.orphanStops[key] {
		c.mu.Unlock()
		return
	}
	c.orphanStops[key] = true
	conn := ch.conn
	c.mu.Unlock()
	c.AddLog("warn", fmt.Sprintf("Unknown transaction #%d (server restart?): requesting RemoteStop", txID), ch.ID)
	if conn == nil {
		return
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		ctx, cancel := bg()
		defer cancel()
		_, _ = conn.Call(ctx, "RemoteStopTransaction", map[string]any{"transactionId": txID})
	}()
}

func (c *Csms) afterBoot(ch *Charger) {
	c.mu.Lock()
	conn := ch.conn
	sched := c.schedule(0)
	c.mu.Unlock()
	if conn == nil {
		return
	}
	// TxDefaultProfile 0 W: a new session waits for the engine to allocate power (~0.3 s) instead of starting
	// at the minimum without checking the yard's headroom.
	ctx, cancel := bg()
	defer cancel()
	_, err := conn.Call(ctx, "SetChargingProfile", map[string]any{
		"connectorId": 0,
		"csChargingProfiles": map[string]any{
			"chargingProfileId": 1, "stackLevel": 0, "chargingProfilePurpose": "TxDefaultProfile",
			"chargingProfileKind": "Relative", "chargingSchedule": sched,
		},
	})
	if err != nil {
		c.AddLog("warn", "Could not set TxDefaultProfile: "+err.Error(), ch.ID)
	}
	c.mu.Lock()
	for _, k := range ch.Connectors {
		k.LastSentW = nil
	}
	c.mu.Unlock()
	c.Rebalance("boot", 0)
}

// schedule builds a charging schedule for limitW in the configured unit (caller holds the lock).
func (c *Csms) schedule(limitW float64) map[string]any {
	limit := math.Round(limitW)
	if c.cfg.ChargingRateUnit == "A" {
		limit = math.Round(limitW/(c.cfg.Voltage*float64(c.cfg.Phases))*10) / 10
	}
	return map[string]any{
		"chargingRateUnit":       c.cfg.ChargingRateUnit,
		"chargingSchedulePeriod": []map[string]any{{"startPeriod": 0, "limit": limit, "numberPhases": c.cfg.Phases}},
	}
}
