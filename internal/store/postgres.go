package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS vehicles (
  id           BIGSERIAL PRIMARY KEY,
  name         TEXT NOT NULL,
  make         TEXT,
  model        TEXT,
  vin          TEXT UNIQUE,                 -- 17 characters (ISO 3779)
  evccid       TEXT UNIQUE,                 -- EV communication controller ID (MAC, ISO 15118 / DIN 70121)
  id_tag       TEXT UNIQUE,                 -- associated RFID card (optional)
  battery_kwh  DOUBLE PRECISION NOT NULL DEFAULT 60,
  max_ac_kw    DOUBLE PRECISION NOT NULL DEFAULT 11,
  phases       SMALLINT NOT NULL DEFAULT 3 CHECK (phases BETWEEN 1 AND 3),
  target_soc   DOUBLE PRECISION NOT NULL DEFAULT 80 CHECK (target_soc BETWEEN 10 AND 100),
  priority     SMALLINT NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chargers (
  id            TEXT PRIMARY KEY,           -- OCPP chargePointId
  vendor        TEXT,
  model         TEXT,
  serial        TEXT,
  firmware      TEXT,
  location      TEXT,
  max_power_kw  DOUBLE PRECISION NOT NULL DEFAULT 22,
  priority      SMALLINT NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
  manual_max    BOOLEAN NOT NULL DEFAULT false,
  connectors    SMALLINT,
  first_seen    TIMESTAMPTZ,
  last_seen     TIMESTAMPTZ
);

-- Additive migrations (idempotent; existing rows keep their data)
ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS min_soc DOUBLE PRECISION NULL CHECK (min_soc BETWEEN 0 AND 100);
ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS max_dc_kw DOUBLE PRECISION NULL CHECK (max_dc_kw BETWEEN 1 AND 1000);
ALTER TABLE chargers ADD COLUMN IF NOT EXISTS current_type TEXT NOT NULL DEFAULT 'AC' CHECK (current_type IN ('AC','DC'));`

// Sample fleet. VIN, EVCCID and RFID are FICTITIOUS (manufacturer prefix "XS0", simulation only).
var seedVehicles = []struct {
	name, make, model, vin, evccid, tag string
	kwh, ac                             float64
	phases                              int
}{
	{"Tesla Model 3 RWD", "Tesla", "Model 3", "XS0TM3RWD00000001", "0A1B2C3D4E01", "RFID-0001", 57.5, 11, 3},
	{"Renault Zoe R135", "Renault", "Zoe", "XS0RZOE1350000002", "0A1B2C3D4E02", "RFID-0002", 52, 22, 3},
	{"Nissan Leaf 40", "Nissan", "Leaf", "XS0NLEAF400000003", "0A1B2C3D4E03", "RFID-0003", 39, 6.6, 1},
	{"Hyundai Kona 64", "Hyundai", "Kona Electric", "XS0HKONA640000004", "0A1B2C3D4E04", "RFID-0004", 64, 11, 3},
	{"VW ID.3 Pro", "Volkswagen", "ID.3", "XS0VWID3PR0000005", "0A1B2C3D4E05", "RFID-0005", 58, 11, 3},
	{"Kia EV6 LR", "Kia", "EV6", "XS0KEV6LR00000006", "0A1B2C3D4E06", "RFID-0006", 77.4, 11, 3},
	{"BYD Dolphin", "BYD", "Dolphin", "XS0BYDDLPH0000007", "0A1B2C3D4E07", "RFID-0007", 60.4, 7, 1},
	{"MG4 Standard", "MG", "MG4", "XS0MG4STD00000008", "0A1B2C3D4E08", "RFID-0008", 51, 6.6, 1},
	{"Audi Q8 e-tron", "Audi", "Q8 e-tron", "XS0AQ8ETR00000009", "0A1B2C3D4E09", "RFID-0009", 106, 22, 3},
	{"Peugeot e-208", "Peugeot", "e-208", "XS0PE208000000010", "0A1B2C3D4E0A", "RFID-0010", 50, 7.4, 1},
}

// Master is the PostgreSQL store of master data (vehicles and chargers).
type Master struct {
	Pool *pgxpool.Pool
}

// OpenMaster connects, creates the schema and seeds the sample fleet on an empty database.
func OpenMaster(ctx context.Context, url string) (*Master, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		return nil, fmt.Errorf("postgres schema: %w", err)
	}
	m := &Master{Pool: pool}
	var n int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM vehicles").Scan(&n)
	if n == 0 && !SkipSeed {
		for _, v := range seedVehicles {
			_, err := pool.Exec(ctx, `INSERT INTO vehicles (name, make, model, vin, evccid, id_tag, battery_kwh, max_ac_kw, phases)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, v.name, v.make, v.model, v.vin, v.evccid, v.tag, v.kwh, v.ac, v.phases)
			if err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

// SkipSeed disables the sample fleet (used by the SQLite migration tool).
var SkipSeed = false

// Close releases the pool.
func (m *Master) Close() { m.Pool.Close() }

func ms(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	v := t.UnixMilli()
	return &v
}

func fromMs(v *int64) *time.Time {
	if v == nil {
		return nil
	}
	t := time.UnixMilli(*v)
	return &t
}

// ---------- vehicles ----------

const vehicleCols = "id, name, make, model, vin, evccid, id_tag, battery_kwh, max_ac_kw, phases, target_soc, priority, created_at, min_soc, max_dc_kw"

func scanVehicle(row pgx.Row) (*Vehicle, error) {
	var v Vehicle
	var created time.Time
	err := row.Scan(&v.ID, &v.Name, &v.Make, &v.Model, &v.VIN, &v.EVCCID, &v.IDTag, &v.BatteryKWh, &v.MaxACKW, &v.Phases, &v.TargetSoC, &v.Priority, &created, &v.MinSoC, &v.MaxDCKW)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	v.CreatedAt = created.UnixMilli()
	return &v, nil
}

// ListVehicles returns every vehicle ordered by id.
func (m *Master) ListVehicles(ctx context.Context) ([]Vehicle, error) {
	rows, err := m.Pool.Query(ctx, "SELECT "+vehicleCols+" FROM vehicles ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Vehicle{}
	for rows.Next() {
		v, err := scanVehicle(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *v)
	}
	return out, rows.Err()
}

// GetVehicle returns the vehicle or nil.
func (m *Master) GetVehicle(ctx context.Context, id int64) (*Vehicle, error) {
	return scanVehicle(m.Pool.QueryRow(ctx, "SELECT "+vehicleCols+" FROM vehicles WHERE id = $1", id))
}

// VehicleInput is the editable part of a vehicle (all optional on update).
type VehicleInput struct {
	Name       *string  `json:"name"`
	Make       *string  `json:"make"`
	Model      *string  `json:"model"`
	VIN        *string  `json:"vin"`
	EVCCID     *string  `json:"evccid"`
	IDTag      *string  `json:"id_tag"`
	BatteryKWh *float64 `json:"battery_kwh"`
	MaxACKW    *float64 `json:"max_ac_kw"`
	Phases     *float64 `json:"phases"`
	TargetSoC  *float64 `json:"target_soc"`
	Priority   *float64 `json:"priority"`
	MinSoC     OptFloat `json:"min_soc"`   // null clears it
	MaxDCKW    OptFloat `json:"max_dc_kw"` // null clears it
}

// OptFloat is an optional, nullable number of a partial update: absent = keep, null = clear, number = set.
type OptFloat struct {
	Set bool
	Val *float64
}

// UnmarshalJSON records that the key was present (also for null).
func (o *OptFloat) UnmarshalJSON(b []byte) error {
	o.Set, o.Val = true, nil
	if string(b) == "null" {
		return nil
	}
	var f float64
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}
	o.Val = &f
	return nil
}

func (o OptFloat) apply(dst **float64) {
	if o.Set {
		*dst = o.Val
	}
}

func rangeCheck(name string, v, min, max float64) error {
	if math.IsNaN(v) || v < min || v > max {
		return Invalid(fmt.Sprintf("%s out of range (%g–%g)", name, min, max))
	}
	return nil
}

// merge applies the input over base and validates the result.
func (in VehicleInput) merge(base Vehicle) (Vehicle, error) {
	v := base
	if in.Name != nil {
		v.Name = strings.TrimSpace(*in.Name)
	}
	if in.Make != nil {
		v.Make = Str(*in.Make)
	}
	if in.Model != nil {
		v.Model = Str(*in.Model)
	}
	if in.VIN != nil {
		v.VIN = NormVIN(*in.VIN)
	}
	if in.EVCCID != nil {
		v.EVCCID = NormEVCCID(*in.EVCCID)
	}
	if in.IDTag != nil {
		v.IDTag = Str(*in.IDTag)
	}
	if in.BatteryKWh != nil {
		v.BatteryKWh = *in.BatteryKWh
	}
	if in.MaxACKW != nil {
		v.MaxACKW = *in.MaxACKW
	}
	if in.Phases != nil {
		v.Phases = int(*in.Phases)
	}
	if in.TargetSoC != nil {
		v.TargetSoC = *in.TargetSoC
	}
	if in.Priority != nil {
		v.Priority = int(math.Round(*in.Priority))
	}
	in.MinSoC.apply(&v.MinSoC)
	in.MaxDCKW.apply(&v.MaxDCKW)
	switch {
	case v.Name == "":
		return v, Invalid("name is required")
	case v.VIN != nil && len(*v.VIN) != 17:
		return v, Invalid("VIN must have 17 characters")
	case v.VIN != nil && strings.ContainsAny(*v.VIN, "IOQ"):
		return v, Invalid("VIN cannot contain I, O or Q")
	case v.EVCCID != nil && len(*v.EVCCID) != 12:
		return v, Invalid("EVCCID must have 12 hexadecimal digits (MAC)")
	case v.IDTag != nil && len(*v.IDTag) > 20:
		return v, Invalid("idTag allows at most 20 characters (OCPP 1.6)")
	}
	for _, c := range []struct {
		n        string
		v, lo, h float64
	}{
		{"battery_kwh", v.BatteryKWh, 1, 1000}, {"max_ac_kw", v.MaxACKW, 1, 50}, {"phases", float64(v.Phases), 1, 3},
		{"target_soc", v.TargetSoC, 10, 100}, {"priority", float64(v.Priority), 1, 10},
	} {
		if err := rangeCheck(c.n, c.v, c.lo, c.h); err != nil {
			return v, err
		}
	}
	if v.MinSoC != nil {
		if err := rangeCheck("min_soc", *v.MinSoC, 0, 100); err != nil {
			return v, err
		}
		if *v.MinSoC > v.TargetSoC {
			return v, Invalid("min_soc cannot be greater than target_soc")
		}
	}
	if v.MaxDCKW != nil {
		if err := rangeCheck("max_dc_kw", *v.MaxDCKW, 1, 1000); err != nil {
			return v, err
		}
	}
	return v, nil
}

var uniqueCol = regexp.MustCompile(`vehicles_(\w+)_key`)

func uniqueError(err error) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		col := "value"
		if m := uniqueCol.FindStringSubmatch(pe.ConstraintName); m != nil {
			col = m[1]
		}
		if col == "id_tag" {
			col = "idTag"
		} else {
			col = strings.ToUpper(col)
		}
		return Invalid("A vehicle with that " + col + " already exists")
	}
	return err
}

// CreateVehicle validates and inserts a vehicle.
func (m *Master) CreateVehicle(ctx context.Context, in VehicleInput) (*Vehicle, error) {
	v, err := in.merge(Vehicle{BatteryKWh: 60, MaxACKW: 11, Phases: 3, TargetSoC: 80, Priority: 5})
	if err != nil {
		return nil, err
	}
	row := m.Pool.QueryRow(ctx, `INSERT INTO vehicles (name, make, model, vin, evccid, id_tag, battery_kwh, max_ac_kw, phases, target_soc, priority, min_soc, max_dc_kw)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING `+vehicleCols,
		v.Name, v.Make, v.Model, v.VIN, v.EVCCID, v.IDTag, v.BatteryKWh, v.MaxACKW, v.Phases, v.TargetSoC, v.Priority, v.MinSoC, v.MaxDCKW)
	out, err := scanVehicle(row)
	return out, uniqueError(err)
}

// UpdateVehicle merges and saves a vehicle.
func (m *Master) UpdateVehicle(ctx context.Context, id int64, in VehicleInput) (*Vehicle, error) {
	cur, err := m.GetVehicle(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur == nil {
		return nil, ErrNotFound
	}
	v, err := in.merge(*cur)
	if err != nil {
		return nil, err
	}
	row := m.Pool.QueryRow(ctx, `UPDATE vehicles SET name=$2, make=$3, model=$4, vin=$5, evccid=$6, id_tag=$7, battery_kwh=$8,
		max_ac_kw=$9, phases=$10, target_soc=$11, priority=$12, min_soc=$13, max_dc_kw=$14 WHERE id=$1 RETURNING `+vehicleCols,
		id, v.Name, v.Make, v.Model, v.VIN, v.EVCCID, v.IDTag, v.BatteryKWh, v.MaxACKW, v.Phases, v.TargetSoC, v.Priority, v.MinSoC, v.MaxDCKW)
	out, err := scanVehicle(row)
	return out, uniqueError(err)
}

// DeleteVehicle removes a vehicle (sessions keep their denormalised name).
func (m *Master) DeleteVehicle(ctx context.Context, id int64) error {
	tag, err := m.Pool.Exec(ctx, "DELETE FROM vehicles WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Identification is a vehicle found by one of its identifiers.
type Identification struct {
	Vehicle *Vehicle
	Method  string // vin | evccid | autocharge | rfid
}

var autochargeTag = regexp.MustCompile(`(?i)^VID:?([0-9A-Fa-f:-]{12,17})$`)

// FindVehicle identifies a vehicle by VIN, EVCCID, Autocharge idTag ("VID:" + EVCCID) or RFID, in that order.
func (m *Master) FindVehicle(ctx context.Context, vin, evccid, idTag string) (*Identification, error) {
	by := func(col, val string) (*Vehicle, error) {
		return scanVehicle(m.Pool.QueryRow(ctx, "SELECT "+vehicleCols+" FROM vehicles WHERE "+col+" = $1", val))
	}
	if n := NormVIN(vin); n != nil {
		if v, err := by("vin", *n); err != nil || v != nil {
			return idf(v, "vin"), err
		}
	}
	if n := NormEVCCID(evccid); n != nil {
		if v, err := by("evccid", *n); err != nil || v != nil {
			return idf(v, "evccid"), err
		}
	}
	if idTag != "" {
		// Autocharge: many DC/AC chargers send idTag = "VID:" + EVCCID
		if mm := autochargeTag.FindStringSubmatch(idTag); mm != nil {
			if n := NormEVCCID(mm[1]); n != nil {
				if v, err := by("evccid", *n); err != nil || v != nil {
					return idf(v, "autocharge"), err
				}
			}
		}
		if v, err := by("id_tag", idTag); err != nil || v != nil {
			return idf(v, "rfid"), err
		}
	}
	return nil, nil
}

func idf(v *Vehicle, method string) *Identification {
	if v == nil {
		return nil
	}
	return &Identification{Vehicle: v, Method: method}
}

// ---------- chargers ----------

const chargerCols = "id, vendor, model, serial, firmware, location, max_power_kw, priority, manual_max, connectors, first_seen, last_seen, current_type"

func scanCharger(row pgx.Row) (*Charger, error) {
	var c Charger
	var first, last *time.Time
	err := row.Scan(&c.ID, &c.Vendor, &c.Model, &c.Serial, &c.Firmware, &c.Location, &c.MaxPowerKW, &c.Priority, &c.ManualMax, &c.Connectors, &first, &last, &c.CurrentType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.FirstSeen, c.LastSeen = ms(first), ms(last)
	return &c, nil
}

// ListChargers returns every registered charger ordered by id.
func (m *Master) ListChargers(ctx context.Context) ([]Charger, error) {
	rows, err := m.Pool.Query(ctx, "SELECT "+chargerCols+" FROM chargers ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Charger{}
	for rows.Next() {
		c, err := scanCharger(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// GetCharger returns the charger or nil.
func (m *Master) GetCharger(ctx context.Context, id string) (*Charger, error) {
	return scanCharger(m.Pool.QueryRow(ctx, "SELECT "+chargerCols+" FROM chargers WHERE id = $1", id))
}

// UpsertCharger inserts or updates a charger (first_seen is kept).
func (m *Master) UpsertCharger(ctx context.Context, c Charger) error {
	if c.CurrentType != CurrentDC {
		c.CurrentType = CurrentAC
	}
	_, err := m.Pool.Exec(ctx, `INSERT INTO chargers (`+chargerCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO UPDATE SET vendor=EXCLUDED.vendor, model=EXCLUDED.model, serial=EXCLUDED.serial, firmware=EXCLUDED.firmware,
		  location=EXCLUDED.location, max_power_kw=EXCLUDED.max_power_kw, priority=EXCLUDED.priority, manual_max=EXCLUDED.manual_max,
		  connectors=EXCLUDED.connectors, last_seen=EXCLUDED.last_seen, current_type=EXCLUDED.current_type`,
		c.ID, c.Vendor, c.Model, c.Serial, c.Firmware, c.Location, c.MaxPowerKW, c.Priority, c.ManualMax, c.Connectors, fromMs(c.FirstSeen), fromMs(c.LastSeen), c.CurrentType)
	return err
}

// DeleteCharger removes a charger from the registry.
func (m *Master) DeleteCharger(ctx context.Context, id string) error {
	_, err := m.Pool.Exec(ctx, "DELETE FROM chargers WHERE id = $1", id)
	return err
}

// ReadOnlyQuery runs one SELECT in a read-only transaction with a statement timeout (assistant tool).
func (m *Master) ReadOnlyQuery(ctx context.Context, sql string, maxRows int) ([]map[string]any, bool, error) {
	q := strings.TrimRight(strings.TrimSpace(sql), "; \n\t")
	if !regexp.MustCompile(`(?i)^(select|with)\b`).MatchString(q) {
		return nil, false, Invalid("Only SELECT (or WITH … SELECT) queries are allowed.")
	}
	if strings.Contains(q, ";") {
		return nil, false, Invalid(`A single statement, without ";".`)
	}
	tx, err := m.Pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = 5000"); err != nil {
		return nil, false, err
	}
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return nil, false, Invalid(err.Error())
	}
	defer rows.Close()
	out := []map[string]any{}
	fields := rows.FieldDescriptions()
	truncated := false
	for rows.Next() {
		if len(out) >= maxRows {
			truncated = true
			break
		}
		vals, err := rows.Values()
		if err != nil {
			return nil, false, err
		}
		r := make(map[string]any, len(vals))
		for i, f := range fields {
			r[f.Name] = vals[i]
		}
		out = append(out, r)
	}
	return out, truncated, rows.Err()
}
