package csms

import (
	"sort"
	"time"

	"github.com/fhidalgo/emob/motor/internal/engine"
	"github.com/fhidalgo/emob/motor/internal/store"
)

// JSON shape of the live state (contract with the BFF and the dashboard).

type vehicleJSON struct {
	ID         int64    `json:"id"`
	Name       string   `json:"name"`
	VIN        *string  `json:"vin"`
	EVCCID     *string  `json:"evccid"`
	MaxAcKw    float64  `json:"maxAcKw"`
	TargetSoc  float64  `json:"targetSoc"`
	Priority   int      `json:"priority"`
	BatteryKwh float64  `json:"batteryKwh"`
	MinSoc     *float64 `json:"minSoc"`
	MaxDcKw    *float64 `json:"maxDcKw"`
}

type planJSON struct {
	ID          int64    `json:"id"`
	TargetSoc   *float64 `json:"targetSoc"`
	MinSoc      *float64 `json:"minSoc"`
	DepartureAt *int64   `json:"departureAt"`
	MaxKw       *float64 `json:"maxKw"`
}

type connectorJSON struct {
	ConnectorID   int          `json:"connectorId"`
	Status        string       `json:"status"`
	ErrorCode     string       `json:"errorCode"`
	TransactionID *int64       `json:"transactionId"`
	IDTag         *string      `json:"idTag"`
	StartedAt     *int64       `json:"startedAt"`
	EnergyWh      float64      `json:"energyWh"`
	PowerW        float64      `json:"powerW"`
	CurrentA      *float64     `json:"currentA"`
	SoC           *float64     `json:"soc"`
	AllocatedW    float64      `json:"allocatedW"`
	DemandLimited bool         `json:"demandLimited"`
	NeedKWh       *float64     `json:"needKWh"`
	IdentMethod   *string      `json:"identMethod"`
	Plan          *planJSON    `json:"plan"`
	PendingPlanID *int64       `json:"pendingPlanId"`
	Vehicle       *vehicleJSON `json:"vehicle"`
	// MinSoc is the minimum SoC at departure in force (plan, else vehicle).
	MinSoc *float64 `json:"minSoc"`
	// PriorityModel is the evaluation of the "weighted" strategy (formulas 1–7); null with other strategies.
	PriorityModel *engine.Evaluation `json:"priorityModel"`
}

type chargerJSON struct {
	ID          string          `json:"id"`
	Vendor      string          `json:"vendor"`
	Model       string          `json:"model"`
	Serial      string          `json:"serial"`
	Firmware    string          `json:"firmware"`
	Location    string          `json:"location"`
	Connected   bool            `json:"connected"`
	Status      string          `json:"status"`
	LastSeen    *int64          `json:"lastSeen"`
	MaxPowerKw  float64         `json:"maxPowerKw"`
	CurrentType string          `json:"currentType"`
	Priority    int             `json:"priority"`
	Connectors  []connectorJSON `json:"connectors"`
}

// Snapshot is the live state of the yard.
type Snapshot struct {
	Config          Config          `json:"config"`
	BuildingLoadW   float64         `json:"buildingLoadW"`
	AvailableW      float64         `json:"availableW"`
	ReservedW       float64         `json:"reservedW"`
	TotalPowerW     float64         `json:"totalPowerW"`
	TotalAllocatedW float64         `json:"totalAllocatedW"`
	ActiveSessions  int             `json:"activeSessions"`
	Chargers        []chargerJSON   `json:"chargers"`
	Transactions    []store.Session `json:"transactions"`
	Plans           []store.Plan    `json:"plans"`
	Log             []LogLine       `json:"log"`
	History         []HistoryPoint  `json:"history"`
	ServerTime      int64           `json:"serverTime"`
}

func nz[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}

// Snapshot returns the current live state.
func (c *Csms) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.computeBudget()
	c.lastBuildingW = b.BuildingW
	s := Snapshot{Config: c.cfg, BuildingLoadW: b.BuildingW, AvailableW: b.AvailableW, ReservedW: b.ReservedW, ServerTime: time.Now().UnixMilli()}
	for _, ch := range c.sortedChargers() {
		cj := chargerJSON{ID: ch.ID, Vendor: ch.Vendor, Model: ch.Model, Serial: ch.Serial, Firmware: ch.Firmware, Location: ch.Location,
			Connected: ch.Connected, Status: ch.Status, LastSeen: nz(ch.LastSeen), MaxPowerKw: ch.MaxPowerKw, CurrentType: ch.CurrentType,
			Priority: ch.Priority, Connectors: []connectorJSON{}}
		if cj.CurrentType == "" {
			cj.CurrentType = store.CurrentAC
		}
		ids := make([]int, 0, len(ch.Connectors))
		for id := range ch.Connectors {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			k := ch.Connectors[id]
			if k.TransactionID != 0 {
				s.ActiveSessions++
				s.TotalPowerW += k.PowerW
				s.TotalAllocatedW += k.AllocatedW
			}
			kj := connectorJSON{ConnectorID: k.ID, Status: k.Status, ErrorCode: k.ErrorCode, TransactionID: nz(k.TransactionID), IDTag: nz(k.IDTag),
				StartedAt: nz(k.StartedAt), EnergyWh: k.EnergyWh, PowerW: k.PowerW, CurrentA: k.CurrentA, SoC: k.SoC, AllocatedW: k.AllocatedW,
				DemandLimited: k.DemandLimited, NeedKWh: k.NeedKWh, IdentMethod: nz(k.IdentMethod)}
			if k.TransactionID != 0 {
				kj.MinSoc = minSoCOf(k.Vehicle, k.Plan)
				if c.cfg.Strategy == "weighted" && k.Eval != nil {
					ev := *k.Eval
					kj.PriorityModel = &ev
				}
			}
			if k.Plan != nil {
				kj.Plan = &planJSON{ID: k.Plan.ID, TargetSoc: k.Plan.TargetSoC, MinSoc: k.Plan.MinSoC, DepartureAt: k.Plan.DepartureAt, MaxKw: k.Plan.MaxKW}
			}
			if k.pendingPlan != nil {
				kj.PendingPlanID = &k.pendingPlan.id
			}
			if v := k.Vehicle; v != nil {
				kj.Vehicle = &vehicleJSON{ID: v.ID, Name: v.Name, VIN: v.VIN, EVCCID: v.EVCCID, MaxAcKw: v.MaxACKW, TargetSoc: v.TargetSoC, Priority: v.Priority, BatteryKwh: v.BatteryKWh,
					MinSoc: v.MinSoC, MaxDcKw: v.MaxDCKW}
			}
			cj.Connectors = append(cj.Connectors, kj)
		}
		s.Chargers = append(s.Chargers, cj)
	}
	if s.Chargers == nil {
		s.Chargers = []chargerJSON{}
	}
	s.Transactions = append([]store.Session{}, c.recentTx...)
	s.Plans = append([]store.Plan{}, c.recentPlans...)
	logs := c.log
	if len(logs) > 150 {
		logs = logs[len(logs)-150:]
	}
	s.Log = append([]LogLine{}, logs...)
	s.History = append([]HistoryPoint{}, c.history...)
	return s
}

// LiveSession is an active session with its live measurements (calendar, sessions list).
type LiveSession struct {
	store.Session
	PowerW     float64 `json:"power_w"`
	AllocatedW float64 `json:"allocated_w"`
	Live       bool    `json:"live"`
}

// LiveSessions returns the active sessions from memory.
func (c *Csms) LiveSessions() map[int64]LiveSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int64]LiveSession{}
	for _, ch := range c.chargers {
		for _, k := range ch.Connectors {
			if k.TransactionID == 0 {
				continue
			}
			e := k.EnergyWh
			s := store.Session{TransactionID: k.TransactionID, ChargerID: ch.ID, ConnectorID: k.ID, IDTag: nz(k.IDTag), IdentMethod: nz(k.IdentMethod),
				StartedAt: k.StartedAt, EnergyWh: &e, SoCStart: k.SoCStart, SoCEnd: k.SoC, MinSoC: minSoCOf(k.Vehicle, k.Plan)}
			if k.Plan != nil {
				s.DepartureAt = k.Plan.DepartureAt
			}
			if v := k.Vehicle; v != nil {
				s.VehicleID, s.VehicleName, s.VehicleVIN, s.VehicleEVCCID = &v.ID, &v.Name, v.VIN, v.EVCCID
			}
			out[k.TransactionID] = LiveSession{Session: s, PowerW: k.PowerW, AllocatedW: k.AllocatedW, Live: true}
		}
	}
	return out
}
