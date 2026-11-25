// Package csms is the OCPP 1.6J central system logic: charge point sessions, vehicle linking, charging plans and the
// dynamic power manager that keeps the yard under its contracted limit.
//
// OCPP transport lives in the separate OCPP gateway; the CSMS receives its events (gateway.Handler) and sends commands
// through gateway.Link. Concurrency: all in-memory state (chargers, connectors, config, logs) is guarded by Csms.mu. OCPP calls and
// database writes are done outside the lock.
package csms

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fhidalgo/emob/motor/internal/engine"
	"github.com/fhidalgo/emob/motor/internal/gateway"
	"github.com/fhidalgo/emob/motor/internal/store"
)

// Config is the engine configuration (persisted in Mongo, settings/engine).
type Config struct {
	SiteLimitKw           float64            `json:"siteLimitKw" bson:"siteLimitKw"`           // grid connection / yard contract
	BuildingLoadKw        float64            `json:"buildingLoadKw" bson:"buildingLoadKw"`     // auxiliary loads (not managed; 0 when none)
	BuildingLoadMode      string             `json:"buildingLoadMode" bson:"buildingLoadMode"` // fixed | simulated
	Strategy              string             `json:"strategy" bson:"strategy"`                 // equal | priority | fifo | need | weighted
	MinPowerKw            float64            `json:"minPowerKw" bson:"minPowerKw"`             // 6 A three-phase ≈ 4.14 kW
	ChargingRateUnit      string             `json:"chargingRateUnit" bson:"chargingRateUnit"` // W | A
	Voltage               float64            `json:"voltage" bson:"voltage"`
	Phases                int                `json:"phases" bson:"phases"`
	DefaultMaxPowerKw     float64            `json:"defaultMaxPowerKw" bson:"defaultMaxPowerKw"`
	AcceptUnknownChargers bool               `json:"acceptUnknownChargers" bson:"acceptUnknownChargers"`
	SafetyMarginKw        float64            `json:"safetyMarginKw" bson:"safetyMarginKw"` // margin under the limit
	RampDelayMs           int                `json:"rampDelayMs" bson:"rampDelayMs"`       // wait between decreases and increases
	Energy                map[string]float64 `json:"energy,omitempty" bson:"energy,omitempty"`

	// Parameters of the "weighted" strategy (priority model of the bus-depot report)
	Weights            engine.Weights `json:"weights" bson:"weights"`                       // wU, wD, wE, wI; each 0–1, sum 1
	ChargeEfficiency   float64        `json:"chargeEfficiency" bson:"chargeEfficiency"`     // η, 0.5–1
	ControlIntervalMin float64        `json:"controlIntervalMin" bson:"controlIntervalMin"` // Δt in minutes, 1–60
}

// DefaultConfig is used on an empty database.
func DefaultConfig() Config {
	return Config{SiteLimitKw: 50, BuildingLoadMode: "fixed", Strategy: "equal", MinPowerKw: 4.2, ChargingRateUnit: "W",
		Voltage: 230, Phases: 3, DefaultMaxPowerKw: 22, AcceptUnknownChargers: true, SafetyMarginKw: 1, RampDelayMs: 2500,
		Weights: engine.DefaultParams.Weights, ChargeEfficiency: engine.DefaultParams.Efficiency, ControlIntervalMin: engine.DefaultParams.IntervalH * 60}
}

// fillDefaults completes a configuration stored before the weighted-strategy fields existed.
func (c *Config) fillDefaults() {
	d := DefaultConfig()
	w := c.Weights
	if w.Urgency+w.Deficit+w.Energy+w.Importance <= 0 {
		c.Weights = d.Weights
	}
	if c.ChargeEfficiency <= 0 {
		c.ChargeEfficiency = d.ChargeEfficiency
	}
	if c.ControlIntervalMin <= 0 {
		c.ControlIntervalMin = d.ControlIntervalMin
	}
}

// params are the engine parameters derived from the configuration.
func (c Config) params() engine.Params {
	return engine.Params{Weights: c.Weights, Efficiency: c.ChargeEfficiency, IntervalH: c.ControlIntervalMin / 60, DefaultH: engine.DefaultParams.DefaultH}
}

const vehicleInfoTTL = 10 * time.Minute

type vehicleInfo struct {
	vin, evccid string
	t           time.Time
}

type pendingPlan struct {
	id int64
	t  time.Time
}

// Connector is the live state of one connector.
type Connector struct {
	ID            int
	Status        string
	ErrorCode     string
	TransactionID int64
	IDTag         string
	StartedAt     int64
	MeterStartWh  float64
	MeterWh       float64
	EnergyWh      float64
	PowerW        float64
	CurrentA      *float64
	SoC           *float64
	SoCStart      *float64
	MaxPowerW     float64
	AllocatedW    float64
	LastSentW     *float64
	LastSentAt    time.Time
	CapW          *float64
	DemandLimited bool
	NeedKWh       *float64
	Eval          *engine.Evaluation // priority model of the last allocation ("weighted" strategy only)
	LastMeterAt   time.Time
	Vehicle       *store.Vehicle
	IdentMethod   string
	vehicleInfo   *vehicleInfo
	Plan          *store.Plan
	pendingPlan   *pendingPlan
	targetStop    bool
}

// Charger is the live state of one charge point.
type Charger struct {
	ID, Vendor, Model, Serial, Firmware, Location string
	Connected                                     bool
	Status                                        string
	LastSeen, FirstSeen                           int64
	MaxPowerKw                                    float64
	CurrentType                                   string // AC | DC
	ManualMax                                     bool
	Priority                                      int
	Connectors                                    map[int]*Connector
	conn                                          *gateway.Link // nil when offline; commands go through the OCPP gateway
}

// LogLine is a line of the in-memory event log shown in the dashboard.
type LogLine struct {
	T         int64   `json:"t"`
	Level     string  `json:"level"`
	Msg       string  `json:"msg"`
	ChargerID *string `json:"chargerId"`
	Dir       string  `json:"dir,omitempty"`
	Action    string  `json:"action,omitempty"`
}

// HistoryPoint is a yard power sample kept in memory for the live chart.
type HistoryPoint struct {
	T         int64   `json:"t"`
	EvW       float64 `json:"evW"`
	AllocW    float64 `json:"allocW"`
	BuildingW float64 `json:"buildingW"`
	LimitW    float64 `json:"limitW"`
	Sessions  int     `json:"sessions"`
}

// Csms is the central system.
type Csms struct {
	master *store.Master
	ops    *store.Ops
	gw     *gateway.Client

	mu            sync.Mutex
	cfg           Config
	chargers      map[string]*Charger
	nextTx        int64
	log           []LogLine
	history       []HistoryPoint
	startedAt     time.Time
	auxW          float64
	auxAtAlloc    float64
	lastBuildingW float64
	actionByID    map[string]string
	orphanStops   map[string]bool

	// cached lists for snapshots (refreshed in the background)
	recentTx    []store.Session
	recentPlans []store.Plan
	listsDirty  chan struct{}

	rebalanceTimer *time.Timer
	rebalanceCh    chan string

	// OnChange is called (non-blocking, possibly often) whenever the live state changes.
	OnChange func()
}

// New loads configuration and registered chargers and starts the background loops.
func New(ctx context.Context, master *store.Master, ops *store.Ops, gw *gateway.Client) (*Csms, error) {
	c := &Csms{
		master: master, ops: ops, gw: gw, chargers: map[string]*Charger{}, startedAt: time.Now(),
		actionByID: map[string]string{}, orphanStops: map[string]bool{},
		listsDirty: make(chan struct{}, 1), rebalanceCh: make(chan string, 1),
	}
	c.cfg = DefaultConfig()
	if _, err := ops.LoadSettings(ctx, "engine", &c.cfg); err != nil {
		return nil, err
	}
	c.cfg.fillDefaults()
	c.nextTx = max(1000, ops.MaxTransactionID(ctx))
	if err := ops.CloseOrphanSessions(ctx); err != nil {
		return nil, err
	}
	if err := ops.CloseOrphanPlans(ctx); err != nil {
		return nil, err
	}
	rows, err := master.ListChargers(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		ch := c.ensureCharger(r.ID)
		ch.Vendor, ch.Model, ch.Serial, ch.Firmware, ch.Location = store.Deref(r.Vendor), store.Deref(r.Model), store.Deref(r.Serial), store.Deref(r.Firmware), store.Deref(r.Location)
		ch.MaxPowerKw, ch.Priority, ch.ManualMax = r.MaxPowerKW, r.Priority, r.ManualMax
		if r.CurrentType == store.CurrentDC {
			ch.CurrentType = store.CurrentDC
		}
		if r.LastSeen != nil {
			ch.LastSeen = *r.LastSeen
		}
		if r.FirstSeen != nil {
			ch.FirstSeen = *r.FirstSeen
		}
	}
	c.auxW = c.readAuxW()
	c.auxAtAlloc = c.auxW
	c.refreshLists()

	go c.rebalanceLoop()
	go c.listsLoop()
	go every(5*time.Second, func() { c.Rebalance("periodic", 0) })
	go every(2*time.Second, c.sample)
	go every(time.Second, func() {
		c.mu.Lock()
		c.auxW = c.readAuxW()
		jump := math.Abs(c.auxW-c.auxAtAlloc) >= 250
		c.mu.Unlock()
		if jump {
			c.Rebalance("auxiliary loads", 0)
		}
	})
	go every(3*time.Second, func() {
		if err := c.runPlans(); err != nil {
			c.AddLog("error", "Scheduler: "+err.Error(), "")
		}
	})
	return c, nil
}

func every(d time.Duration, fn func()) {
	for range time.Tick(d) {
		fn()
	}
}

func bg() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func (c *Csms) changed() {
	if c.OnChange != nil {
		c.OnChange()
	}
}

// ---------- configuration ----------

// Config returns a copy of the configuration.
func (c *Csms) Config() Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	cfg := c.cfg
	return cfg
}

var energyKeys = []string{"energyPriceClp", "demandChargeClpKw", "gridCo2KgKwh", "evKmPerKwh", "iceCo2KgKm", "iceKmPerL", "fuelPriceClpL"}

// UpdateConfig validates and applies a partial configuration.
func (c *Csms) UpdateConfig(patch map[string]any) (Config, error) {
	c.mu.Lock()
	next := c.cfg
	if next.Energy != nil {
		next.Energy = cloneMap(next.Energy)
	}
	c.mu.Unlock()
	num := func(k string, min, max float64) (float64, bool, error) {
		v, ok := patch[k]
		if !ok || v == nil {
			return 0, false, nil
		}
		f, ok := v.(float64)
		if !ok || math.IsNaN(f) || f < min || f > max {
			return 0, true, store.Invalid(fmt.Sprintf("Value out of range: %s=%v", k, v))
		}
		return f, true, nil
	}
	for _, x := range []struct {
		k        string
		min, max float64
		set      func(float64)
	}{
		{"siteLimitKw", 0, 100000, func(v float64) { next.SiteLimitKw = v }},
		{"buildingLoadKw", 0, 100000, func(v float64) { next.BuildingLoadKw = v }},
		{"minPowerKw", 0, 1000, func(v float64) { next.MinPowerKw = v }},
		{"voltage", 100, 1000, func(v float64) { next.Voltage = v }},
		{"phases", 1, 3, func(v float64) { next.Phases = int(v) }},
		{"safetyMarginKw", 0, 1000, func(v float64) { next.SafetyMarginKw = v }},
		{"chargeEfficiency", 0.5, 1, func(v float64) { next.ChargeEfficiency = v }},
		{"controlIntervalMin", 1, 60, func(v float64) { next.ControlIntervalMin = v }},
	} {
		v, ok, err := num(x.k, x.min, x.max)
		if err != nil {
			return Config{}, err
		}
		if ok {
			x.set(v)
		}
	}
	str := func(k string, allowed ...string) (string, bool, error) {
		v, ok := patch[k]
		if !ok || v == nil {
			return "", false, nil
		}
		s, _ := v.(string)
		for _, a := range allowed {
			if s == a {
				return s, true, nil
			}
		}
		return "", true, store.Invalid("Invalid " + k)
	}
	if w, ok := patch["weights"]; ok && w != nil {
		m, isMap := w.(map[string]any)
		if !isMap {
			return Config{}, store.Invalid("weights must be an object {urgency, deficit, energy, importance}")
		}
		ws := next.Weights
		for _, x := range []struct {
			k   string
			dst *float64
		}{{"urgency", &ws.Urgency}, {"deficit", &ws.Deficit}, {"energy", &ws.Energy}, {"importance", &ws.Importance}} {
			v, present := m[x.k]
			if !present || v == nil {
				continue
			}
			f, isNum := v.(float64)
			if !isNum || math.IsNaN(f) || f < 0 || f > 1 {
				return Config{}, store.Invalid(fmt.Sprintf("Value out of range: weights.%s=%v (0–1)", x.k, v))
			}
			*x.dst = f
		}
		if sum := ws.Urgency + ws.Deficit + ws.Energy + ws.Importance; math.Abs(sum-1) > 0.01 {
			return Config{}, store.Invalid(fmt.Sprintf("The weights must add up to 1 (they add up to %.3f)", sum))
		}
		next.Weights = ws
	}
	if s, ok, err := str("strategy", "equal", "priority", "fifo", "need", "weighted"); err != nil {
		return Config{}, err
	} else if ok {
		next.Strategy = s
	}
	if s, ok, err := str("buildingLoadMode", "fixed", "simulated"); err != nil {
		return Config{}, err
	} else if ok {
		next.BuildingLoadMode = s
	}
	if s, ok, err := str("chargingRateUnit", "W", "A"); err != nil {
		return Config{}, err
	} else if ok {
		next.ChargingRateUnit = s
	}
	if v, ok := patch["acceptUnknownChargers"].(bool); ok {
		next.AcceptUnknownChargers = v
	}
	if e, ok := patch["energy"].(map[string]any); ok {
		if next.Energy == nil {
			next.Energy = map[string]float64{}
		}
		for _, k := range energyKeys {
			if v, ok := e[k].(float64); ok {
				if v < 0 || v > 1e7 {
					return Config{}, store.Invalid("Value out of range: " + k)
				}
				next.Energy[k] = v
			}
		}
	}
	ctx, cancel := bg()
	defer cancel()
	if err := c.ops.SaveSettings(ctx, "engine", next); err != nil {
		return Config{}, err
	}
	c.mu.Lock()
	c.cfg = next
	_, unit := patch["chargingRateUnit"]
	_, volt := patch["voltage"]
	_, ph := patch["phases"]
	if unit || volt || ph {
		for _, ch := range c.chargers {
			for _, k := range ch.Connectors {
				k.LastSentW = nil
			}
		}
	}
	c.mu.Unlock()
	c.AddLog("info", fmt.Sprintf("Configuration updated: %v", patch), "")
	c.Rebalance("config", 0)
	return next, nil
}

func cloneMap(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ---------- charger registry ----------

func (c *Csms) ensureCharger(id string) *Charger {
	ch := c.chargers[id]
	if ch == nil {
		ch = &Charger{ID: id, Status: "Unknown", FirstSeen: time.Now().UnixMilli(), MaxPowerKw: c.cfg.DefaultMaxPowerKw, CurrentType: store.CurrentAC,
			Priority: 5, Connectors: map[int]*Connector{}}
		c.chargers[id] = ch
	}
	return ch
}

func (c *Csms) connector(ch *Charger, id int) *Connector {
	k := ch.Connectors[id]
	if k == nil {
		k = &Connector{ID: id, Status: "Unknown", ErrorCode: "NoError"}
		ch.Connectors[id] = k
	}
	return k
}

func resetTx(k *Connector) {
	k.TransactionID, k.IDTag, k.StartedAt, k.PowerW, k.AllocatedW = 0, "", 0, 0, 0
	k.LastSentW, k.LastSentAt, k.CapW, k.DemandLimited = nil, time.Time{}, nil, false
	k.SoC, k.SoCStart, k.MaxPowerW, k.CurrentA, k.Plan, k.targetStop, k.NeedKWh = nil, nil, 0, nil, nil, false, nil
	k.Eval = nil
}

// minSoCOf is the minimum SoC at departure: the plan's, else the vehicle's (nil when neither defines one).
func minSoCOf(v *store.Vehicle, plan *store.Plan) *float64 {
	if plan != nil && plan.MinSoC != nil {
		return plan.MinSoC
	}
	if v != nil {
		return v.MinSoC
	}
	return nil
}

// targetSoCOf is the target SoC: the plan's, else the vehicle's.
func targetSoCOf(v *store.Vehicle, plan *store.Plan) *float64 {
	if plan != nil && plan.TargetSoC != nil {
		return plan.TargetSoC
	}
	if v != nil {
		t := v.TargetSoC
		return &t
	}
	return nil
}

// chargerRow converts the live state to the registry row (caller holds the lock).
func (c *Csms) chargerRow(ch *Charger) store.Charger {
	n := len(ch.Connectors)
	var conns *int
	if n > 0 {
		conns = &n
	}
	first, last := ch.FirstSeen, ch.LastSeen
	row := store.Charger{ID: ch.ID, Vendor: store.Str(ch.Vendor), Model: store.Str(ch.Model), Serial: store.Str(ch.Serial),
		Firmware: store.Str(ch.Firmware), Location: store.Str(ch.Location), MaxPowerKW: ch.MaxPowerKw, Priority: ch.Priority,
		ManualMax: ch.ManualMax, Connectors: conns, FirstSeen: &first, CurrentType: ch.CurrentType}
	if last > 0 {
		row.LastSeen = &last
	}
	return row
}

func (c *Csms) persist(row store.Charger) {
	ctx, cancel := bg()
	defer cancel()
	if err := c.master.UpsertCharger(ctx, row); err != nil {
		slog.Warn("persist charger", "id", row.ID, "err", err)
	}
}

var chargerIDRe = regexp.MustCompile(`^[\w.:-]{1,48}$`)

// ChargerPatch is the editable part of a charger.
type ChargerPatch struct {
	ID         string   `json:"id"`
	Location   *string  `json:"location"`
	MaxPowerKw *float64 `json:"maxPowerKw"`
	Priority   *float64 `json:"priority"`
	// CurrentType is "AC" or "DC" (case-insensitive); AC caps a session with the vehicle's max_ac_kw, DC with max_dc_kw.
	CurrentType *string `json:"currentType"`
}

// RegisterCharger pre-registers a charger (it shows up offline until it connects).
func (c *Csms) RegisterCharger(p ChargerPatch) error {
	id := strings.TrimSpace(p.ID)
	if !chargerIDRe.MatchString(id) {
		return store.Invalid("Invalid ID (letters, digits, . _ : - ; max 48)")
	}
	c.mu.Lock()
	if c.chargers[id] != nil {
		c.mu.Unlock()
		return store.Invalid("That charger already exists")
	}
	c.ensureCharger(id)
	c.mu.Unlock()
	if p.Priority == nil {
		five := 5.0
		p.Priority = &five
	}
	return c.UpdateCharger(id, p)
}

// UpdateCharger changes location, maximum power (fixed manually) or priority.
func (c *Csms) UpdateCharger(id string, p ChargerPatch) error {
	c.mu.Lock()
	ch := c.chargers[id]
	if ch == nil {
		c.mu.Unlock()
		return store.ErrNotFound
	}
	if p.CurrentType != nil {
		t := strings.ToUpper(strings.TrimSpace(*p.CurrentType))
		if t != store.CurrentAC && t != store.CurrentDC {
			c.mu.Unlock()
			return store.Invalid("currentType must be AC or DC")
		}
		ch.CurrentType = t
	}
	if p.MaxPowerKw != nil {
		if !(*p.MaxPowerKw > 0 && *p.MaxPowerKw <= 1000) {
			c.mu.Unlock()
			return store.Invalid("Invalid maximum power")
		}
		ch.MaxPowerKw, ch.ManualMax = *p.MaxPowerKw, true
	}
	if p.Priority != nil {
		pr := int(math.Round(*p.Priority))
		if pr < 1 || pr > 10 {
			c.mu.Unlock()
			return store.Invalid("Priority must be between 1 and 10")
		}
		ch.Priority = pr
	}
	if p.Location != nil {
		loc := strings.TrimSpace(*p.Location)
		if len(loc) > 120 {
			loc = loc[:120]
		}
		ch.Location = loc
	}
	row := c.chargerRow(ch)
	c.mu.Unlock()
	c.persist(row)
	c.Rebalance("charger settings", 0)
	return nil
}

// RemoveCharger deletes a disconnected charger from the registry.
func (c *Csms) RemoveCharger(id string) error {
	c.mu.Lock()
	ch := c.chargers[id]
	if ch == nil {
		c.mu.Unlock()
		return store.ErrNotFound
	}
	if ch.Connected {
		c.mu.Unlock()
		return store.Invalid("Only disconnected chargers can be removed")
	}
	delete(c.chargers, id)
	c.mu.Unlock()
	ctx, cancel := bg()
	defer cancel()
	if err := c.master.DeleteCharger(ctx, id); err != nil {
		return err
	}
	c.changed()
	return nil
}

// RefreshVehicles reloads the in-memory vehicle of active sessions after an edit/delete.
func (c *Csms) RefreshVehicles() {
	ctx, cancel := bg()
	defer cancel()
	c.mu.Lock()
	var ks []*Connector
	for _, ch := range c.chargers {
		for _, k := range ch.Connectors {
			if k.Vehicle != nil {
				ks = append(ks, k)
			}
		}
	}
	c.mu.Unlock()
	for _, k := range ks {
		v, _ := c.master.GetVehicle(ctx, k.Vehicle.ID)
		c.mu.Lock()
		k.Vehicle = v
		c.mu.Unlock()
	}
	c.Rebalance("vehicles", 0)
}

// ---------- logging and telemetry ----------

// AddLog records an engine event (memory + Mongo) and notifies listeners.
func (c *Csms) AddLog(level, msg, chargerID string) {
	t := time.Now().UnixMilli()
	cid := store.Str(chargerID)
	c.mu.Lock()
	c.log = append(c.log, LogLine{T: t, Level: level, Msg: msg, ChargerID: cid})
	if len(c.log) > 300 {
		c.log = c.log[len(c.log)-300:]
	}
	c.mu.Unlock()
	c.ops.AddEvent(store.EngineEvent{T: t, Level: level, ChargerID: cid, Msg: msg})
	c.changed()
}

func (c *Csms) chargerEvent(chargerID, typ string, connectorID *int, detail string) {
	ctx, cancel := bg()
	defer cancel()
	_ = c.ops.AddChargerEvent(ctx, store.ChargerEvent{T: time.Now().UnixMilli(), ChargerID: chargerID, Type: typ, ConnectorID: connectorID, Detail: store.Str(detail)})
}

// readAuxW is the yard auxiliary load "measured" now (caller holds the lock or is in New).
func (c *Csms) readAuxW() float64 {
	base := c.cfg.BuildingLoadKw * 1000
	if c.cfg.BuildingLoadMode != "simulated" {
		return base
	}
	t := time.Since(c.startedAt).Seconds()
	wave := math.Sin(2*math.Pi*t/180) * 0.35
	noise := (rand.Float64() - 0.5) * 0.06
	return math.Max(0, math.Round(base*(1+wave+noise)))
}

func (c *Csms) sample() {
	c.mu.Lock()
	var evW, allocW float64
	sessions := 0
	for _, ch := range c.chargers {
		for _, k := range ch.Connectors {
			if k.TransactionID != 0 {
				sessions++
				evW += k.PowerW
				allocW += k.AllocatedW
			}
		}
	}
	bw := c.lastBuildingW
	if bw == 0 {
		bw = c.auxW
	}
	p := HistoryPoint{T: time.Now().UnixMilli(), EvW: evW, AllocW: allocW, BuildingW: bw, LimitW: c.cfg.SiteLimitKw * 1000, Sessions: sessions}
	c.history = append(c.history, p)
	if len(c.history) > 300 {
		c.history = c.history[1:]
	}
	store15 := sessions > 0 || len(c.history)%15 == 0 // idle: 1 sample every 30 s
	c.mu.Unlock()
	if store15 {
		ctx, cancel := bg()
		defer cancel()
		_ = c.ops.AddSiteSample(ctx, store.SiteSample{T: p.T, LimitW: p.LimitW, BuildingW: p.BuildingW, EvW: p.EvW, AllocW: p.AllocW, Sessions: p.Sessions})
	}
	c.changed()
}

func (c *Csms) markListsDirty() {
	select {
	case c.listsDirty <- struct{}{}:
	default:
	}
}

func (c *Csms) listsLoop() {
	tick := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-tick.C:
		case <-c.listsDirty:
			time.Sleep(200 * time.Millisecond)
		}
		c.refreshLists()
		c.changed()
	}
}

func (c *Csms) refreshLists() {
	ctx, cancel := bg()
	defer cancel()
	tx, err1 := c.ops.RecentFinishedSessions(ctx, 15)
	plans, err2 := c.ops.ListPlans(ctx, time.Now().Add(-6*time.Hour).UnixMilli(), math.MaxInt64)
	if len(plans) > 50 {
		plans = plans[len(plans)-50:]
	}
	c.mu.Lock()
	if err1 == nil {
		c.recentTx = tx
	}
	if err2 == nil {
		c.recentPlans = plans
	}
	c.mu.Unlock()
}

// sortedChargers returns chargers ordered by id (caller holds the lock).
func (c *Csms) sortedChargers() []*Charger {
	out := make([]*Charger, 0, len(c.chargers))
	for _, ch := range c.chargers {
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Connected reports which chargers are online.
func (c *Csms) ConnectedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, ch := range c.chargers {
		if ch.Connected {
			n++
		}
	}
	return n
}
