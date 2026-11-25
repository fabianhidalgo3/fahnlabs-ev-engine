// Package store holds the motor's persistence:
//
//	PostgreSQL (master data): vehicles, chargers
//	MongoDB (operational data): sessions, charging plans, meter samples, yard samples, OCPP log,
//	                            charger events, engine events, settings
//
// Times are exposed as epoch milliseconds (JSON contract of the API); Mongo documents also carry a
// BSON date ("ts") used by time-series collections and TTL indexes.
package store

import (
	"errors"
	"regexp"
	"strings"
)

// ErrNotFound is returned when a record does not exist.
var ErrNotFound = errors.New("not found")

// ValidationError is a client error (bad input), mapped to HTTP 400.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// Invalid builds a ValidationError.
func Invalid(msg string) error { return &ValidationError{Msg: msg} }

// Vehicle is a registered electric vehicle.
type Vehicle struct {
	ID         int64   `json:"id"`
	Name       string  `json:"name"`
	Make       *string `json:"make"`
	Model      *string `json:"model"`
	VIN        *string `json:"vin"`
	EVCCID     *string `json:"evccid"`
	IDTag      *string `json:"id_tag"`
	BatteryKWh float64 `json:"battery_kwh"`
	MaxACKW    float64 `json:"max_ac_kw"`
	Phases     int     `json:"phases"`
	TargetSoC  float64 `json:"target_soc"`
	Priority   int     `json:"priority"`
	CreatedAt  int64   `json:"created_at"`
	// MinSoC is the minimum acceptable SoC at departure (%, ≤ target_soc); nil = not defined.
	MinSoC *float64 `json:"min_soc"`
	// MaxDCKW is the maximum DC charging power the vehicle accepts; nil = unknown (the DC charger limit applies).
	MaxDCKW *float64 `json:"max_dc_kw"`
}

// Charger current types.
const (
	CurrentAC = "AC"
	CurrentDC = "DC"
)

// Charger is a registered charge point.
type Charger struct {
	ID         string  `json:"id"`
	Vendor     *string `json:"vendor"`
	Model      *string `json:"model"`
	Serial     *string `json:"serial"`
	Firmware   *string `json:"firmware"`
	Location   *string `json:"location"`
	MaxPowerKW float64 `json:"max_power_kw"`
	Priority   int     `json:"priority"`
	ManualMax  bool    `json:"manual_max"`
	Connectors *int    `json:"connectors"`
	FirstSeen  *int64  `json:"first_seen"`
	LastSeen   *int64  `json:"last_seen"`
	// CurrentType is "AC" or "DC"; it selects the vehicle limit used for the session cap.
	CurrentType string `json:"current_type"`
}

// Session is a charging transaction (vehicle data denormalised at link time).
type Session struct {
	TransactionID int64    `bson:"_id" json:"transaction_id"`
	ChargerID     string   `bson:"charger_id" json:"charger_id"`
	ConnectorID   int      `bson:"connector_id" json:"connector_id"`
	VehicleID     *int64   `bson:"vehicle_id" json:"vehicle_id"`
	VehicleName   *string  `bson:"vehicle_name" json:"vehicle_name"`
	VehicleVIN    *string  `bson:"vehicle_vin" json:"vehicle_vin"`
	VehicleEVCCID *string  `bson:"vehicle_evccid" json:"vehicle_evccid"`
	IDTag         *string  `bson:"id_tag" json:"id_tag"`
	IdentMethod   *string  `bson:"ident_method" json:"ident_method"`
	StartedAt     int64    `bson:"started_at" json:"started_at"`
	StoppedAt     *int64   `bson:"stopped_at" json:"stopped_at"`
	MeterStartWh  *float64 `bson:"meter_start_wh" json:"meter_start_wh"`
	MeterStopWh   *float64 `bson:"meter_stop_wh" json:"meter_stop_wh"`
	EnergyWh      *float64 `bson:"energy_wh" json:"energy_wh"`
	SoCStart      *float64 `bson:"soc_start" json:"soc_start"`
	SoCEnd        *float64 `bson:"soc_end" json:"soc_end"`
	MaxPowerW     *float64 `bson:"max_power_w" json:"max_power_w"`
	StopReason    *string  `bson:"stop_reason" json:"stop_reason"`

	// Departure compliance, recorded at the end of the session when a minimum SoC and a departure time are known
	// (minimum from the charging plan, else from the vehicle; departure from the plan). SoCDeparture is the SoC at the
	// departure time (the last sample before it when the session ran past the departure, else the final SoC);
	// MinSoCMet = SoCDeparture ≥ MinSoC, nil when it cannot be judged (no minimum, no departure or no SoC).
	MinSoC       *float64 `bson:"min_soc,omitempty" json:"min_soc"`
	DepartureAt  *int64   `bson:"departure_at,omitempty" json:"departure_at"`
	SoCDeparture *float64 `bson:"soc_departure,omitempty" json:"soc_departure"`
	MinSoCMet    *bool    `bson:"min_soc_met,omitempty" json:"min_soc_met"`
}

// SessionOutcome is the departure-compliance part written when a session closes.
type SessionOutcome struct {
	MinSoC       *float64
	DepartureAt  *int64
	SoCDeparture *float64
	MinSoCMet    *bool
}

// Plan is a charging plan (start now or scheduled, optional departure, target SoC and kW limit).
type Plan struct {
	ID            int64    `bson:"_id" json:"id"`
	ChargerID     string   `bson:"charger_id" json:"charger_id"`
	ConnectorID   int      `bson:"connector_id" json:"connector_id"`
	VehicleID     *int64   `bson:"vehicle_id" json:"vehicle_id"`
	VehicleName   *string  `bson:"vehicle_name" json:"vehicle_name"`
	IDTag         string   `bson:"id_tag" json:"id_tag"`
	StartAt       int64    `bson:"start_at" json:"start_at"`
	DepartureAt   *int64   `bson:"departure_at" json:"departure_at"`
	TargetSoC     *float64 `bson:"target_soc" json:"target_soc"`
	MinSoC        *float64 `bson:"min_soc,omitempty" json:"min_soc"`
	MaxKW         *float64 `bson:"max_kw" json:"max_kw"`
	Status        string   `bson:"status" json:"status"`
	TransactionID *int64   `bson:"transaction_id" json:"transaction_id"`
	Note          *string  `bson:"note" json:"note"`
	CreatedAt     int64    `bson:"created_at" json:"created_at"`
	UpdatedAt     *int64   `bson:"updated_at" json:"updated_at"`
}

// MeterSample is one power sample of a session.
type MeterSample struct {
	TransactionID int64    `bson:"transaction_id" json:"transaction_id"`
	T             int64    `bson:"t" json:"t"`
	PowerW        *float64 `bson:"power_w" json:"power_w"`
	AllocatedW    *float64 `bson:"allocated_w" json:"allocated_w"`
	EnergyWh      *float64 `bson:"energy_wh" json:"energy_wh"`
	SoC           *float64 `bson:"soc" json:"soc"`
	CurrentA      *float64 `bson:"current_a" json:"current_a"`
}

// SiteSample is one yard power sample.
type SiteSample struct {
	T         int64   `bson:"t" json:"t"`
	LimitW    float64 `bson:"limit_w" json:"limit_w"`
	BuildingW float64 `bson:"building_w" json:"building_w"`
	EvW       float64 `bson:"ev_w" json:"ev_w"`
	AllocW    float64 `bson:"alloc_w" json:"alloc_w"`
	Sessions  int     `bson:"sessions" json:"sessions"`
}

// OcppEntry is one OCPP frame of the log.
type OcppEntry struct {
	T         int64   `bson:"t" json:"t"`
	ChargerID string  `bson:"charger_id" json:"charger_id"`
	Dir       string  `bson:"dir" json:"dir"`
	Kind      string  `bson:"kind" json:"kind"`
	Action    *string `bson:"action" json:"action"`
	MsgID     string  `bson:"msg_id" json:"msg_id"`
	ErrorCode *string `bson:"error_code" json:"error_code"`
	Payload   string  `bson:"payload" json:"payload"`
}

// ChargerEvent is an availability event (connect, disconnect, boot, fault, fault_clear, unavailable).
type ChargerEvent struct {
	T           int64   `bson:"t" json:"t"`
	ChargerID   string  `bson:"charger_id" json:"charger_id"`
	Type        string  `bson:"type" json:"type"`
	ConnectorID *int    `bson:"connector_id" json:"connector_id"`
	Detail      *string `bson:"detail" json:"detail"`
}

// EngineEvent is an engine log line (info, warn, error, power).
type EngineEvent struct {
	T         int64   `bson:"t" json:"t"`
	Level     string  `bson:"level" json:"level"`
	ChargerID *string `bson:"charger_id" json:"charger_id"`
	Msg       string  `bson:"msg" json:"msg"`
}

// ---------- helpers ----------

// Str returns a pointer to s, or nil when it is empty after trimming.
func Str(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}

// Deref returns *p or "".
func Deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

var nonAlnum = regexp.MustCompile(`[^A-Z0-9]`)
var nonHex = regexp.MustCompile(`[^0-9A-F]`)

// NormVIN upper-cases and strips separators; empty → nil.
func NormVIN(v string) *string { return Str(nonAlnum.ReplaceAllString(strings.ToUpper(v), "")) }

// NormEVCCID upper-cases and keeps hex digits only; empty → nil.
func NormEVCCID(v string) *string { return Str(nonHex.ReplaceAllString(strings.ToUpper(v), "")) }

// LikeToRegex converts an SQL LIKE pattern ("PRUEBA-%") into an anchored regular expression.
// A pattern without wildcards matches the exact ID.
func LikeToRegex(p string) string {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range p {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return b.String()
}
