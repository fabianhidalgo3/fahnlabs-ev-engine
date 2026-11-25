package csms

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/fhidalgo/emob/motor/internal/store"
)

// resolveIDTag derives the idTag from the vehicle (its RFID or "VID:"+EVCCID) when vehicleId is given,
// so the session links to it through the same mechanism as a real identification.
func (c *Csms) resolveIDTag(ctx context.Context, vehicleID *int64, idTag string) (string, *store.Vehicle, error) {
	var v *store.Vehicle
	if vehicleID != nil && *vehicleID > 0 {
		var err error
		if v, err = c.master.GetVehicle(ctx, *vehicleID); err != nil {
			return "", nil, err
		}
		if v == nil {
			return "", nil, store.Invalid("Vehicle not found")
		}
		switch {
		case v.IDTag != nil:
			idTag = *v.IDTag
		case v.EVCCID != nil:
			idTag = "VID:" + *v.EVCCID
		default:
			return "", nil, store.Invalid("The vehicle has neither idTag nor EVCCID to identify it")
		}
	}
	idTag = strings.TrimSpace(idTag)
	if idTag == "" || len(idTag) > 20 {
		return "", nil, store.Invalid("idTag required (max 20 characters)")
	}
	return idTag, v, nil
}

// PlanRequest creates a charging plan.
type PlanRequest struct {
	ChargerID   string   `json:"chargerId"`
	ConnectorID *int     `json:"connectorId"`
	VehicleID   *int64   `json:"vehicleId"`
	IDTag       string   `json:"idTag"`
	StartAt     *int64   `json:"startAt"`
	DepartureAt *int64   `json:"departureAt"`
	TargetSoC   *float64 `json:"targetSoc"`
	MinSoC      *float64 `json:"minSoc"` // minimum acceptable SoC at departure (overrides the vehicle's min_soc)
	MaxKW       *float64 `json:"maxKw"`
}

// CreatePlan creates a plan: start (now or scheduled), optional departure, target SoC and kW limit. The scheduler
// starts it with RemoteStartTransaction; during the session the engine uses its data and stops it at the target SoC.
func (c *Csms) CreatePlan(r PlanRequest) (*store.Plan, error) {
	ctx, cancel := bg()
	defer cancel()
	c.mu.Lock()
	_, ok := c.chargers[r.ChargerID]
	c.mu.Unlock()
	if !ok {
		return nil, store.Invalid("Charger not found")
	}
	connector := 1
	if r.ConnectorID != nil {
		connector = *r.ConnectorID
	}
	if connector < 1 || connector > 32 {
		return nil, store.Invalid("Invalid connector")
	}
	idTag, v, err := c.resolveIDTag(ctx, r.VehicleID, r.IDTag)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	start := now
	if r.StartAt != nil && *r.StartAt > 0 {
		start = *r.StartAt
	}
	if r.DepartureAt != nil && *r.DepartureAt <= max(start, now) {
		return nil, store.Invalid("Departure must be after the start")
	}
	if r.TargetSoC != nil && (*r.TargetSoC < 10 || *r.TargetSoC > 100) {
		return nil, store.Invalid("Target SoC must be between 10 and 100 %")
	}
	if r.MinSoC != nil && (*r.MinSoC < 10 || *r.MinSoC > 100) {
		return nil, store.Invalid("Minimum SoC must be between 10 and 100 %")
	}
	if r.MinSoC != nil && r.TargetSoC != nil && *r.MinSoC > *r.TargetSoC {
		return nil, store.Invalid("Minimum SoC cannot be greater than the target SoC")
	}
	if r.MaxKW != nil && !(*r.MaxKW > 0 && *r.MaxKW <= 1000) {
		return nil, store.Invalid("Invalid kW limit")
	}
	// Prevents two overlapping open plans on the same connector. Without departure a 4 h window is assumed.
	const h4 = 4 * 3600000
	end := func(p store.Plan) int64 {
		if p.DepartureAt != nil {
			return *p.DepartureAt
		}
		if p.Status == "scheduled" {
			return p.StartAt + h4
		}
		return max(p.StartAt, now) + h4
	}
	newEnd := start + h4
	if r.DepartureAt != nil {
		newEnd = *r.DepartureAt
	}
	open, err := c.ops.ListPlans(ctx, 0, 1<<62)
	if err != nil {
		return nil, err
	}
	for _, p := range open {
		if p.ChargerID == r.ChargerID && p.ConnectorID == connector && slices.Contains(store.OpenPlanStatuses, p.Status) && p.StartAt < newEnd && end(p) > start {
			return nil, store.Invalid(fmt.Sprintf("The connector already has plan #%d (%s)", p.ID, p.Status))
		}
	}
	plan := store.Plan{ChargerID: r.ChargerID, ConnectorID: connector, VehicleID: r.VehicleID, IDTag: idTag, StartAt: start,
		DepartureAt: r.DepartureAt, TargetSoC: r.TargetSoC, MinSoC: r.MinSoC, MaxKW: r.MaxKW}
	if v != nil {
		plan.VehicleName = &v.Name
	} else {
		plan.VehicleID = nil
	}
	created, err := c.ops.CreatePlan(ctx, plan)
	if err != nil {
		return nil, err
	}
	when := "for now"
	if start > now+5000 {
		when = "scheduled for " + time.UnixMilli(start).UTC().Format(time.RFC3339)
	}
	who := ""
	if v != nil {
		who = ", " + v.Name
	}
	c.AddLog("info", fmt.Sprintf("Charging plan #%d %s (connector %d%s)", created.ID, when, connector, who), r.ChargerID)
	c.markListsDirty()
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = c.runPlans()
	}()
	return created, nil
}

// CancelPlan cancels an open plan. An active plan stops applying its target/limit; the session goes on.
func (c *Csms) CancelPlan(id int64) error {
	ctx, cancel := bg()
	defer cancel()
	p, err := c.ops.GetPlan(ctx, id)
	if err != nil {
		return err
	}
	if p == nil {
		return store.ErrNotFound
	}
	if !slices.Contains(store.OpenPlanStatuses, p.Status) {
		return store.Invalid("The plan is already " + p.Status)
	}
	if err := c.ops.SetPlanStatus(ctx, id, "cancelled", nil, "Cancelled by the operator"); err != nil {
		return err
	}
	c.mu.Lock()
	for _, ch := range c.chargers {
		for _, k := range ch.Connectors {
			if k.Plan != nil && k.Plan.ID == id {
				k.Plan = nil
			}
		}
	}
	c.mu.Unlock()
	c.AddLog("info", fmt.Sprintf("Plan #%d cancelled", id), p.ChargerID)
	c.markListsDirty()
	c.Rebalance("plan", 0)
	return nil
}

var plansMu sync.Mutex

// runPlans starts due plans (retried for up to 15 min) and fails plans whose charger never started the session.
func (c *Csms) runPlans() error {
	if !plansMu.TryLock() {
		return nil
	}
	defer plansMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now()

	type stale struct {
		ch string
		id int64
	}
	var expired []stale
	c.mu.Lock()
	for _, ch := range c.chargers {
		for _, k := range ch.Connectors {
			if k.pendingPlan != nil && now.Sub(k.pendingPlan.t) > 90*time.Second {
				expired = append(expired, stale{ch.ID, k.pendingPlan.id})
				k.pendingPlan = nil
			}
		}
	}
	c.mu.Unlock()
	for _, s := range expired {
		_ = c.ops.SetPlanStatus(ctx, s.id, "failed", nil, "The charger did not start the session")
		c.AddLog("warn", fmt.Sprintf("Plan #%d: the charger did not start the session", s.id), s.ch)
	}

	due, err := c.ops.DuePlans(ctx, now.UnixMilli())
	if err != nil {
		return err
	}
	for _, p := range due {
		c.mu.Lock()
		ch := c.chargers[p.ChargerID]
		var k *Connector
		why := ""
		switch {
		case ch == nil:
			why = "charger removed"
		case !ch.Connected:
			why = "charger disconnected"
		default:
			k = ch.Connectors[p.ConnectorID]
			if k != nil && k.TransactionID != 0 {
				why = "connector busy"
			} else if k != nil && k.Status == "Faulted" {
				why = "connector faulted"
			}
		}
		if why != "" {
			c.mu.Unlock()
			if now.UnixMilli()-p.StartAt > 15*60000 || ch == nil {
				_ = c.ops.SetPlanStatus(ctx, p.ID, "failed", nil, why)
				c.AddLog("warn", fmt.Sprintf("Plan #%d failed: %s", p.ID, why), p.ChargerID)
				c.markListsDirty()
			}
			continue // retried in the next cycle (up to 15 min)
		}
		k = c.connector(ch, p.ConnectorID)
		k.pendingPlan = &pendingPlan{id: p.ID, t: now}
		conn := ch.conn
		c.mu.Unlock()
		_ = c.ops.SetPlanStatus(ctx, p.ID, "starting", nil, "")
		c.markListsDirty()
		res, err := conn.Call(ctx, "RemoteStartTransaction", map[string]any{"connectorId": p.ConnectorID, "idTag": p.IDTag})
		if err == nil {
			var r struct {
				Status string `json:"status"`
			}
			_ = json.Unmarshal(res, &r)
			if r.Status != "Accepted" {
				err = fmt.Errorf("RemoteStart %s", r.Status)
			}
		}
		if err != nil {
			c.mu.Lock()
			k.pendingPlan = nil
			c.mu.Unlock()
			_ = c.ops.SetPlanStatus(ctx, p.ID, "failed", nil, err.Error())
			c.AddLog("warn", fmt.Sprintf("Plan #%d failed: %s", p.ID, err), ch.ID)
			continue
		}
		c.AddLog("info", fmt.Sprintf("Plan #%d: RemoteStart accepted", p.ID), ch.ID)
	}
	return nil
}

// ---------- remote commands ----------

func (c *Csms) requireConn(id string) (*Charger, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.chargers[id]
	if ch == nil {
		return nil, store.ErrNotFound
	}
	if ch.conn == nil {
		return nil, store.Invalid("Charger disconnected")
	}
	return ch, nil
}

func (c *Csms) command(ch *Charger, action string, payload any) (map[string]any, error) {
	c.mu.Lock()
	conn := ch.conn
	c.mu.Unlock()
	if conn == nil {
		return nil, store.Invalid("Charger disconnected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	res, err := conn.Call(ctx, action, payload)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	_ = json.Unmarshal(res, &out)
	return out, nil
}

// RemoteStart starts a session on a connector, optionally on behalf of a vehicle.
func (c *Csms) RemoteStart(id string, connectorID int, idTag string, vehicleID *int64) (map[string]any, error) {
	ch, err := c.requireConn(id)
	if err != nil {
		return nil, err
	}
	if connectorID < 1 {
		return nil, store.Invalid("Invalid connector")
	}
	c.mu.Lock()
	k := ch.Connectors[connectorID]
	busy := k != nil && k.TransactionID != 0
	c.mu.Unlock()
	if busy {
		return nil, store.Invalid(fmt.Sprintf("Connector %d already has an active session", connectorID))
	}
	ctx, cancel := bg()
	tag, _, err := c.resolveIDTag(ctx, vehicleID, idTag)
	cancel()
	if err != nil {
		return nil, err
	}
	res, err := c.command(ch, "RemoteStartTransaction", map[string]any{"connectorId": connectorID, "idTag": tag})
	if err == nil {
		c.AddLog("info", fmt.Sprintf("RemoteStart connector %d: %v", connectorID, res["status"]), id)
	}
	return res, err
}

// RemoteStop stops the session of a connector.
func (c *Csms) RemoteStop(id string, connectorID int) (map[string]any, error) {
	ch, err := c.requireConn(id)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	k := ch.Connectors[connectorID]
	var tx int64
	if k != nil {
		tx = k.TransactionID
	}
	c.mu.Unlock()
	if tx == 0 {
		return nil, store.Invalid("No active session on that connector")
	}
	res, err := c.command(ch, "RemoteStopTransaction", map[string]any{"transactionId": tx})
	if err == nil {
		c.AddLog("info", fmt.Sprintf("RemoteStop #%d: %v", tx, res["status"]), id)
	}
	return res, err
}

// Reset sends a Soft or Hard reset.
func (c *Csms) Reset(id, typ string) (map[string]any, error) {
	ch, err := c.requireConn(id)
	if err != nil {
		return nil, err
	}
	if typ != "Hard" {
		typ = "Soft"
	}
	res, err := c.command(ch, "Reset", map[string]any{"type": typ})
	if err == nil {
		c.AddLog("info", fmt.Sprintf("Reset %s: %v", typ, res["status"]), id)
	}
	return res, err
}
