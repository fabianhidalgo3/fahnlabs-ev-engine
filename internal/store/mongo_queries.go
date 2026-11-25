package store

import (
	"context"
	"errors"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ---------- charging plans ----------

// OpenPlanStatuses are the statuses of plans not finished yet.
var OpenPlanStatuses = []string{"scheduled", "starting", "active"}

// CreatePlan inserts a plan with a new id.
func (o *Ops) CreatePlan(ctx context.Context, p Plan) (*Plan, error) {
	id, err := o.NextID(ctx, "charging_plans", 0)
	if err != nil {
		return nil, err
	}
	p.ID = id
	p.Status = "scheduled"
	p.CreatedAt = time.Now().UnixMilli()
	if _, err := o.Plans.InsertOne(ctx, p); err != nil {
		return nil, err
	}
	return &p, nil
}

// GetPlan returns the plan or nil.
func (o *Ops) GetPlan(ctx context.Context, id int64) (*Plan, error) {
	var p Plan
	err := o.Plans.FindOne(ctx, bson.M{"_id": id}).Decode(&p)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return &p, err
}

// SetPlanStatus updates status, and optionally transaction id and note.
func (o *Ops) SetPlanStatus(ctx context.Context, id int64, status string, txID *int64, note string) error {
	set := bson.M{"status": status, "updated_at": time.Now().UnixMilli()}
	if txID != nil {
		set["transaction_id"] = *txID
	}
	if note != "" {
		set["note"] = note
	}
	_, err := o.Plans.UpdateByID(ctx, id, bson.M{"$set": set})
	return err
}

// DuePlans returns scheduled plans whose start time has come.
func (o *Ops) DuePlans(ctx context.Context, now int64) ([]Plan, error) {
	return find[Plan](ctx, o.Plans, bson.M{"status": "scheduled", "start_at": bson.M{"$lte": now}}, options.Find().SetSort(bson.M{"start_at": 1}))
}

// ListPlans returns plans overlapping [from, to) or still open.
func (o *Ops) ListPlans(ctx context.Context, from, to int64) ([]Plan, error) {
	f := bson.M{"start_at": bson.M{"$lt": to}, "$or": bson.A{
		bson.M{"status": bson.M{"$in": OpenPlanStatuses}},
		bson.M{"departure_at": bson.M{"$gte": from}},
		bson.M{"departure_at": nil, "start_at": bson.M{"$gte": from}},
	}}
	return find[Plan](ctx, o.Plans, f, options.Find().SetSort(bson.M{"start_at": 1}))
}

// CloseOrphanPlans marks plans that were running when the motor stopped as failed.
func (o *Ops) CloseOrphanPlans(ctx context.Context) error {
	_, err := o.Plans.UpdateMany(ctx, bson.M{"status": bson.M{"$in": []string{"starting", "active"}}},
		bson.M{"$set": bson.M{"status": "failed", "note": "Server restart", "updated_at": time.Now().UnixMilli()}})
	return err
}

// ---------- log queries ----------

// LogFilter filters the OCPP log, commands and engine events. Every field is optional.
type LogFilter struct {
	ChargerID  string // exact ID or SQL-like pattern with % (e.g. "PRUEBA-%")
	Action     string
	Dir        string
	Kind       string
	Level      string
	OnlyErrors bool
	Text       string
	From, To   int64
	Limit      int64
	Asc        bool
}

func (f LogFilter) base() bson.M {
	m := bson.M{}
	if f.ChargerID != "" {
		m["charger_id"] = bson.M{"$regex": LikeToRegex(f.ChargerID)}
	}
	t := bson.M{}
	if f.From > 0 {
		t["$gte"] = f.From
	}
	if f.To > 0 {
		t["$lt"] = f.To
	}
	if len(t) > 0 {
		m["t"] = t
	}
	return m
}

func limit(n, def, max int64) int64 {
	if n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// SearchOcpp searches OCPP frames, newest first by default.
func (o *Ops) SearchOcpp(ctx context.Context, f LogFilter) ([]OcppEntry, error) {
	m := f.base()
	if f.Action != "" {
		m["action"] = f.Action
	}
	if f.Dir != "" {
		m["dir"] = f.Dir
	}
	if f.Kind != "" {
		m["kind"] = f.Kind
	}
	if f.OnlyErrors {
		m["error_code"] = bson.M{"$ne": nil}
	}
	if f.Text != "" {
		m["payload"] = bson.M{"$regex": regexp.QuoteMeta(f.Text), "$options": "i"}
	}
	dir := -1
	if f.Asc {
		dir = 1
	}
	return find[OcppEntry](ctx, o.OcppLog, m, options.Find().SetSort(bson.D{{Key: "t", Value: dir}, {Key: "_id", Value: dir}}).SetLimit(limit(f.Limit, 50, 500)))
}

// CommandRow is a command sent to a charger with its answer.
type CommandRow struct {
	T             int64   `json:"t" bson:"t"`
	ChargerID     string  `json:"charger_id" bson:"charger_id"`
	Action        *string `json:"action" bson:"action"`
	Payload       string  `json:"payload" bson:"payload"`
	ResultT       *int64  `json:"result_t" bson:"result_t"`
	ResultKind    *string `json:"result_kind" bson:"result_kind"`
	ResultError   *string `json:"result_error" bson:"result_error"`
	ResultPayload *string `json:"result_payload" bson:"result_payload"`
}

func commandPipeline(match bson.M) mongo.Pipeline {
	return mongo.Pipeline{
		{{Key: "$match", Value: match}},
		{{Key: "$lookup", Value: bson.M{
			"from": "ocpp_log",
			"let":  bson.M{"cid": "$charger_id", "mid": "$msg_id"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$charger_id", "$$cid"}}, bson.M{"$eq": bson.A{"$msg_id", "$$mid"}}, bson.M{"$ne": bson.A{"$kind", "CALL"}},
				}}}},
				bson.M{"$limit": 1},
			},
			"as": "r",
		}}},
		{{Key: "$set", Value: bson.M{"r": bson.M{"$first": "$r"}}}},
	}
}

// SearchCommands returns outgoing CALLs joined with the charger's answer (result and latency).
func (o *Ops) SearchCommands(ctx context.Context, f LogFilter) ([]CommandRow, error) {
	m := f.base()
	m["kind"], m["dir"] = "CALL", "out"
	if f.Action != "" {
		m["action"] = f.Action
	}
	p := commandPipeline(m)
	p = append(p, bson.D{{Key: "$sort", Value: bson.D{{Key: "t", Value: -1}, {Key: "_id", Value: -1}}}})
	if f.OnlyErrors {
		p = append(p, bson.D{{Key: "$match", Value: bson.M{"$or": bson.A{bson.M{"r": nil}, bson.M{"r.error_code": bson.M{"$ne": nil}}}}}})
	}
	p = append(p, bson.D{{Key: "$limit", Value: limit(f.Limit, 100, 500)}},
		bson.D{{Key: "$project", Value: bson.M{"_id": 0, "t": 1, "charger_id": 1, "action": 1, "payload": 1,
			"result_t": "$r.t", "result_kind": "$r.kind", "result_error": "$r.error_code", "result_payload": "$r.payload"}}})
	return aggregate[CommandRow](ctx, o.OcppLog, p)
}

// CommandStat counts commands per type with rejections, missing answers and mean latency.
type CommandStat struct {
	Action     *string  `json:"action" bson:"_id"`
	N          int64    `json:"n" bson:"n"`
	Rejected   int64    `json:"rejected" bson:"rejected"`
	NoResponse int64    `json:"no_response" bson:"no_response"`
	LatencyMs  *float64 `json:"latency_ms" bson:"latency_ms"`
}

// CommandStats aggregates commands sent since `from`.
func (o *Ops) CommandStats(ctx context.Context, from int64, charger string) ([]CommandStat, error) {
	m := LogFilter{ChargerID: charger, From: from}.base()
	m["kind"], m["dir"] = "CALL", "out"
	p := commandPipeline(m)
	p = append(p,
		bson.D{{Key: "$group", Value: bson.M{
			"_id":         "$action",
			"n":           bson.M{"$sum": 1},
			"rejected":    bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$gt": bson.A{"$r.error_code", nil}}, 1, 0}}},
			"no_response": bson.M{"$sum": bson.M{"$cond": bson.A{bson.M{"$eq": bson.A{bson.M{"$type": "$r"}, "missing"}}, 1, 0}}},
			"latency_ms":  bson.M{"$avg": bson.M{"$subtract": bson.A{"$r.t", "$t"}}},
		}}},
		bson.D{{Key: "$sort", Value: bson.M{"n": -1}}})
	out, err := aggregate[CommandStat](ctx, o.OcppLog, p)
	for i := range out {
		if out[i].LatencyMs != nil {
			v := float64(int64(*out[i].LatencyMs + 0.5))
			out[i].LatencyMs = &v
		}
	}
	return out, err
}

// ActionCount is an OCPP action and how many frames carry it.
type ActionCount struct {
	Action string `json:"action" bson:"_id"`
	N      int64  `json:"n" bson:"n"`
}

// OcppActions lists actions seen in the last 7 days.
func (o *Ops) OcppActions(ctx context.Context) ([]ActionCount, error) {
	return aggregate[ActionCount](ctx, o.OcppLog, mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"t": bson.M{"$gte": time.Now().Add(-7 * 24 * time.Hour).UnixMilli()}, "action": bson.M{"$ne": nil}}}},
		{{Key: "$group", Value: bson.M{"_id": "$action", "n": bson.M{"$sum": 1}}}},
		{{Key: "$sort", Value: bson.M{"n": -1}}},
	})
}

// OcppSummary aggregates the OCPP log per charger, per action and the most frequent errors.
func (o *Ops) OcppSummary(ctx context.Context, charger string, from, to int64) (map[string]any, error) {
	if to <= 0 {
		to = time.Now().Add(time.Hour).UnixMilli()
	}
	m := LogFilter{ChargerID: charger, From: from, To: to}.base()
	isErr := bson.M{"$cond": bson.A{bson.M{"$gt": bson.A{"$error_code", nil}}, 1, 0}}
	byCharger, err := aggregate[bson.M](ctx, o.OcppLog, mongo.Pipeline{
		{{Key: "$match", Value: m}},
		{{Key: "$group", Value: bson.M{"_id": "$charger_id", "messages": bson.M{"$sum": 1}, "errors": bson.M{"$sum": isErr}, "first": bson.M{"$min": "$t"}, "last": bson.M{"$max": "$t"}}}},
		{{Key: "$sort", Value: bson.M{"_id": 1}}},
		{{Key: "$project", Value: bson.M{"_id": 0, "charger_id": "$_id", "messages": 1, "errors": 1, "first": 1, "last": 1}}},
	})
	if err != nil {
		return nil, err
	}
	byAction, err := aggregate[bson.M](ctx, o.OcppLog, mongo.Pipeline{
		{{Key: "$match", Value: m}},
		{{Key: "$group", Value: bson.M{"_id": bson.M{"action": "$action", "dir": "$dir", "kind": "$kind"}, "n": bson.M{"$sum": 1}, "errors": bson.M{"$sum": isErr}}}},
		{{Key: "$sort", Value: bson.M{"n": -1}}},
		{{Key: "$limit", Value: 60}},
		{{Key: "$project", Value: bson.M{"_id": 0, "action": "$_id.action", "dir": "$_id.dir", "kind": "$_id.kind", "n": 1, "errors": 1}}},
	})
	if err != nil {
		return nil, err
	}
	em := bson.M{}
	for k, v := range m {
		em[k] = v
	}
	em["error_code"] = bson.M{"$ne": nil}
	errs, err := aggregate[bson.M](ctx, o.OcppLog, mongo.Pipeline{
		{{Key: "$match", Value: em}},
		{{Key: "$group", Value: bson.M{"_id": bson.M{"charger_id": "$charger_id", "action": "$action", "error_code": "$error_code"}, "n": bson.M{"$sum": 1}, "last": bson.M{"$max": "$t"}}}},
		{{Key: "$sort", Value: bson.M{"n": -1}}},
		{{Key: "$limit", Value: 40}},
		{{Key: "$project", Value: bson.M{"_id": 0, "charger_id": "$_id.charger_id", "action": "$_id.action", "error_code": "$_id.error_code", "n": 1, "last": 1}}},
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"byCharger": byCharger, "byAction": byAction, "errors": errs}, nil
}

// SearchEvents returns engine events, newest first.
func (o *Ops) SearchEvents(ctx context.Context, f LogFilter) ([]EngineEvent, error) {
	m := f.base()
	if f.Level != "" {
		m["level"] = f.Level
	}
	if f.Text != "" {
		m["msg"] = bson.M{"$regex": regexp.QuoteMeta(f.Text), "$options": "i"}
	}
	return find[EngineEvent](ctx, o.Events, m, options.Find().SetSort(bson.D{{Key: "t", Value: -1}, {Key: "_id", Value: -1}}).SetLimit(limit(f.Limit, 50, 500)))
}

// ---------- exports ----------

// ExportSessions returns every session, oldest first.
func (o *Ops) ExportSessions(ctx context.Context) ([]Session, error) {
	return find[Session](ctx, o.Sessions, bson.M{}, options.Find().SetSort(bson.M{"started_at": 1}))
}

// ExportMeterSamples returns samples of one session, or all of them.
func (o *Ops) ExportMeterSamples(ctx context.Context, txID int64) ([]MeterSample, error) {
	f := bson.M{}
	if txID > 0 {
		f["transaction_id"] = txID
	}
	return find[MeterSample](ctx, o.MeterSamples, f, options.Find().SetSort(bson.M{"ts": 1}))
}

// ---------- read-only queries (assistant) ----------

var allowedStages = map[string]bool{
	"$match": true, "$group": true, "$sort": true, "$limit": true, "$skip": true, "$project": true, "$count": true,
	"$unwind": true, "$addFields": true, "$set": true, "$bucket": true, "$bucketAuto": true, "$facet": true, "$sortByCount": true,
}

// AllowedCollections can be queried by the assistant.
var AllowedCollections = map[string]bool{
	"sessions": true, "charging_plans": true, "meter_samples": true, "site_samples": true,
	"ocpp_log": true, "charger_events": true, "engine_events": true,
}

// ReadOnlyAggregate runs a restricted aggregation pipeline (no writes, no joins) with a 5 s limit.
func (o *Ops) ReadOnlyAggregate(ctx context.Context, collection string, pipeline []bson.M, maxRows int) ([]bson.M, bool, error) {
	if !AllowedCollections[collection] {
		return nil, false, Invalid("Unknown collection " + collection)
	}
	p := bson.A{}
	for _, st := range pipeline {
		if len(st) != 1 {
			return nil, false, Invalid("Each pipeline stage must have exactly one operator")
		}
		for k := range st {
			if !allowedStages[k] {
				return nil, false, Invalid("Stage not allowed: " + k)
			}
		}
		p = append(p, st)
	}
	p = append(p, bson.M{"$limit": maxRows + 1})
	cur, err := o.DB.Collection(collection).Aggregate(ctx, p, options.Aggregate().SetMaxAwaitTime(5*time.Second))
	if err != nil {
		return nil, false, Invalid(err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var rows []bson.M
	if err := cur.All(ctx, &rows); err != nil {
		return nil, false, Invalid(err.Error())
	}
	truncated := len(rows) > maxRows
	if truncated {
		rows = rows[:maxRows]
	}
	return rows, truncated, nil
}

func aggregate[T any](ctx context.Context, c *mongo.Collection, p mongo.Pipeline) ([]T, error) {
	cur, err := c.Aggregate(ctx, p)
	if err != nil {
		return nil, err
	}
	out := []T{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Aggregate is the exported generic aggregation helper used by analytics.
func Aggregate[T any](ctx context.Context, c *mongo.Collection, p mongo.Pipeline) ([]T, error) {
	return aggregate[T](ctx, c, p)
}
