package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// LogRetention is how long OCPP frames and engine events are kept (TTL index).
const LogRetention = 14 * 24 * time.Hour

// Ops is the MongoDB store of operational data.
type Ops struct {
	Client *mongo.Client
	DB     *mongo.Database

	Sessions, Plans, MeterSamples, SiteSamples, OcppLog, ChargerEvents, Events, Settings, Counters *mongo.Collection

	ocppBuf, sampleBuf, eventBuf *batcher
}

// OpenOps connects, creates collections (time-series, TTL) and indexes.
func OpenOps(ctx context.Context, uri, dbName string) (*Ops, error) {
	// Numbers written by JavaScript tools arrive as BSON doubles; allow decoding them into integer fields.
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second).
		SetBSONOptions(&options.BSONOptions{AllowTruncatingDoubles: true}))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo: %w", err)
	}
	db := client.Database(dbName)
	o := &Ops{Client: client, DB: db}

	existing, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	has := map[string]bool{}
	for _, n := range existing {
		has[n] = true
	}
	ts := func(name, meta string) error {
		if has[name] {
			return nil
		}
		t := options.TimeSeries().SetTimeField("ts").SetGranularity("seconds")
		if meta != "" {
			t.SetMetaField(meta)
		}
		return db.CreateCollection(ctx, name, options.CreateCollection().SetTimeSeriesOptions(t))
	}
	if err := ts("meter_samples", "transaction_id"); err != nil {
		return nil, err
	}
	if err := ts("site_samples", ""); err != nil {
		return nil, err
	}

	o.Sessions = db.Collection("sessions")
	o.Plans = db.Collection("charging_plans")
	o.MeterSamples = db.Collection("meter_samples")
	o.SiteSamples = db.Collection("site_samples")
	o.OcppLog = db.Collection("ocpp_log")
	o.ChargerEvents = db.Collection("charger_events")
	o.Events = db.Collection("engine_events")
	o.Settings = db.Collection("settings")
	o.Counters = db.Collection("counters")

	ttl := int32(LogRetention.Seconds())
	idx := []struct {
		c    *mongo.Collection
		keys bson.D
		opts *options.IndexOptionsBuilder
	}{
		{o.Sessions, bson.D{{Key: "started_at", Value: 1}}, nil},
		{o.Sessions, bson.D{{Key: "charger_id", Value: 1}, {Key: "started_at", Value: 1}}, nil},
		{o.Sessions, bson.D{{Key: "stopped_at", Value: 1}}, nil},
		{o.Plans, bson.D{{Key: "status", Value: 1}, {Key: "start_at", Value: 1}}, nil},
		{o.MeterSamples, bson.D{{Key: "transaction_id", Value: 1}, {Key: "ts", Value: 1}}, nil},
		{o.OcppLog, bson.D{{Key: "ts", Value: 1}}, options.Index().SetExpireAfterSeconds(ttl)},
		{o.OcppLog, bson.D{{Key: "charger_id", Value: 1}, {Key: "t", Value: -1}}, nil},
		{o.OcppLog, bson.D{{Key: "action", Value: 1}, {Key: "t", Value: -1}}, nil},
		{o.OcppLog, bson.D{{Key: "t", Value: -1}}, nil},
		{o.OcppLog, bson.D{{Key: "charger_id", Value: 1}, {Key: "msg_id", Value: 1}}, nil},
		{o.ChargerEvents, bson.D{{Key: "charger_id", Value: 1}, {Key: "t", Value: 1}}, nil},
		{o.ChargerEvents, bson.D{{Key: "type", Value: 1}, {Key: "t", Value: 1}}, nil},
		{o.Events, bson.D{{Key: "ts", Value: 1}}, options.Index().SetExpireAfterSeconds(ttl)},
		{o.Events, bson.D{{Key: "t", Value: -1}}, nil},
	}
	for _, i := range idx {
		m := mongo.IndexModel{Keys: i.keys}
		if i.opts != nil {
			m.Options = i.opts
		}
		if _, err := i.c.Indexes().CreateOne(ctx, m); err != nil {
			return nil, fmt.Errorf("index %s: %w", i.c.Name(), err)
		}
	}

	o.ocppBuf = newBatcher(o.OcppLog, 500*time.Millisecond)
	o.sampleBuf = newBatcher(o.MeterSamples, time.Second)
	o.eventBuf = newBatcher(o.Events, 500*time.Millisecond)
	return o, nil
}

// Close flushes pending writes and disconnects.
func (o *Ops) Close(ctx context.Context) {
	for _, b := range []*batcher{o.ocppBuf, o.sampleBuf, o.eventBuf} {
		b.flush()
	}
	_ = o.Client.Disconnect(ctx)
}

// ---------- write batching (OCPP frames and samples arrive many times per second) ----------

type batcher struct {
	mu   sync.Mutex
	c    *mongo.Collection
	docs []any
}

func newBatcher(c *mongo.Collection, every time.Duration) *batcher {
	b := &batcher{c: c}
	go func() {
		for range time.Tick(every) {
			b.flush()
		}
	}()
	return b
}

func (b *batcher) add(doc any) {
	b.mu.Lock()
	b.docs = append(b.docs, doc)
	full := len(b.docs) >= 1000
	b.mu.Unlock()
	if full {
		go b.flush()
	}
}

func (b *batcher) flush() {
	b.mu.Lock()
	docs := b.docs
	b.docs = nil
	b.mu.Unlock()
	if len(docs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := b.c.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
		slog.Warn("mongo batch insert failed", "collection", b.c.Name(), "docs", len(docs), "err", err)
	}
}

func date(t int64) bson.DateTime { return bson.DateTime(t) }

// ---------- counters ----------

// NextID returns the next value of a named sequence (atomic).
func (o *Ops) NextID(ctx context.Context, name string, atLeast int64) (int64, error) {
	if atLeast > 0 {
		_, _ = o.Counters.UpdateOne(ctx, bson.M{"_id": name}, bson.M{"$max": bson.M{"seq": atLeast}}, options.UpdateOne().SetUpsert(true))
	}
	var doc struct {
		Seq int64 `bson:"seq"`
	}
	err := o.Counters.FindOneAndUpdate(ctx, bson.M{"_id": name}, bson.M{"$inc": bson.M{"seq": 1}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	return doc.Seq, err
}

// ---------- settings ----------

// LoadSettings decodes the settings document `id` into out; false when it does not exist.
func (o *Ops) LoadSettings(ctx context.Context, id string, out any) (bool, error) {
	err := o.Settings.FindOne(ctx, bson.M{"_id": id}).Decode(out)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return err == nil, err
}

// SaveSettings replaces the settings document `id`.
func (o *Ops) SaveSettings(ctx context.Context, id string, doc any) error {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return err
	}
	var m bson.M
	_ = bson.Unmarshal(raw, &m)
	m["_id"] = id
	_, err = o.Settings.ReplaceOne(ctx, bson.M{"_id": id}, m, options.Replace().SetUpsert(true))
	return err
}

// ---------- sessions ----------

// MaxTransactionID returns the highest transaction id stored.
func (o *Ops) MaxTransactionID(ctx context.Context) int64 {
	var s Session
	if err := o.Sessions.FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.M{"_id": -1})).Decode(&s); err != nil {
		return 0
	}
	return s.TransactionID
}

// StartSession inserts a new session.
func (o *Ops) StartSession(ctx context.Context, s Session) error {
	_, err := o.Sessions.InsertOne(ctx, s)
	return err
}

// LinkSessionVehicle sets the vehicle of a running session.
func (o *Ops) LinkSessionVehicle(ctx context.Context, txID int64, v *Vehicle, method string) error {
	_, err := o.Sessions.UpdateByID(ctx, txID, bson.M{"$set": bson.M{
		"vehicle_id": v.ID, "vehicle_name": v.Name, "vehicle_vin": v.VIN, "vehicle_evccid": v.EVCCID, "ident_method": method,
	}})
	return err
}

// StopSession closes a session, with its departure-compliance outcome (fields left out when nil).
func (o *Ops) StopSession(ctx context.Context, txID, stoppedAt int64, meterStop, energy float64, socStart, socEnd, maxPower *float64, reason string, out SessionOutcome) error {
	set := bson.M{
		"stopped_at": stoppedAt, "meter_stop_wh": meterStop, "energy_wh": energy, "soc_start": socStart, "soc_end": socEnd,
		"max_power_w": maxPower, "stop_reason": reason,
	}
	if out.MinSoC != nil {
		set["min_soc"] = *out.MinSoC
	}
	if out.DepartureAt != nil {
		set["departure_at"] = *out.DepartureAt
	}
	if out.SoCDeparture != nil {
		set["soc_departure"] = *out.SoCDeparture
	}
	if out.MinSoCMet != nil {
		set["min_soc_met"] = *out.MinSoCMet
	}
	_, err := o.Sessions.UpdateByID(ctx, txID, bson.M{"$set": set})
	return err
}

// SoCAt returns the last SoC sampled for a session at or before t (nil when there is none).
func (o *Ops) SoCAt(ctx context.Context, txID, t int64) *float64 {
	var last MeterSample
	err := o.MeterSamples.FindOne(ctx, bson.M{"transaction_id": txID, "ts": bson.M{"$lte": date(t)}, "soc": bson.M{"$ne": nil}},
		options.FindOne().SetSort(bson.M{"ts": -1})).Decode(&last)
	if err != nil {
		return nil
	}
	return last.SoC
}

// RecentFinishedSessions returns the last n finished sessions (newest first).
func (o *Ops) RecentFinishedSessions(ctx context.Context, n int64) ([]Session, error) {
	return find[Session](ctx, o.Sessions, bson.M{"stopped_at": bson.M{"$ne": nil}}, options.Find().SetSort(bson.M{"started_at": -1}).SetLimit(n))
}

// SessionsInRange returns sessions overlapping [from, to) (open sessions count until now), oldest first.
func (o *Ops) SessionsInRange(ctx context.Context, from, to int64, chargerRegex string) ([]Session, error) {
	f := bson.M{"started_at": bson.M{"$lt": to}, "$or": bson.A{bson.M{"stopped_at": nil}, bson.M{"stopped_at": bson.M{"$gte": from}}}}
	if chargerRegex != "" {
		f["charger_id"] = bson.M{"$regex": chargerRegex}
	}
	return find[Session](ctx, o.Sessions, f, options.Find().SetSort(bson.M{"started_at": 1}))
}

// CloseOrphanSessions closes sessions left open by a previous run using their last sample.
func (o *Ops) CloseOrphanSessions(ctx context.Context) error {
	open, err := find[Session](ctx, o.Sessions, bson.M{"stopped_at": nil}, nil)
	if err != nil {
		return err
	}
	for _, s := range open {
		set := bson.M{"stopped_at": s.StartedAt, "stop_reason": "ServerRestart"}
		var last MeterSample
		if o.MeterSamples.FindOne(ctx, bson.M{"transaction_id": s.TransactionID}, options.FindOne().SetSort(bson.M{"ts": -1})).Decode(&last) == nil {
			set["stopped_at"] = last.T
			set["energy_wh"] = last.EnergyWh
			set["soc_end"] = last.SoC
		}
		if _, err := o.Sessions.UpdateByID(ctx, s.TransactionID, bson.M{"$set": set}); err != nil {
			return err
		}
	}
	return nil
}

// ---------- samples ----------

type meterDoc struct {
	TS          bson.DateTime `bson:"ts"`
	MeterSample `bson:",inline"`
}

type siteDoc struct {
	TS         bson.DateTime `bson:"ts"`
	SiteSample `bson:",inline"`
}

// AddMeterSample queues a session power sample.
func (o *Ops) AddMeterSample(s MeterSample) { o.sampleBuf.add(meterDoc{TS: date(s.T), MeterSample: s}) }

// AddSiteSample stores a yard sample.
func (o *Ops) AddSiteSample(ctx context.Context, s SiteSample) error {
	_, err := o.SiteSamples.InsertOne(ctx, siteDoc{TS: date(s.T), SiteSample: s})
	return err
}

// SessionSamples returns the samples of one session, oldest first.
func (o *Ops) SessionSamples(ctx context.Context, txID int64) ([]MeterSample, error) {
	return find[MeterSample](ctx, o.MeterSamples, bson.M{"transaction_id": txID}, options.Find().SetSort(bson.M{"ts": 1}))
}

// SamplesOf returns the samples of several sessions (sorted by session then time).
func (o *Ops) SamplesOf(ctx context.Context, txIDs []int64) ([]MeterSample, error) {
	if len(txIDs) == 0 {
		return nil, nil
	}
	return find[MeterSample](ctx, o.MeterSamples, bson.M{"transaction_id": bson.M{"$in": txIDs}},
		options.Find().SetSort(bson.D{{Key: "transaction_id", Value: 1}, {Key: "ts", Value: 1}}))
}

// SiteSamplesFrom returns yard samples in [from, to).
func (o *Ops) SiteSamplesFrom(ctx context.Context, from, to int64) ([]SiteSample, error) {
	return find[SiteSample](ctx, o.SiteSamples, bson.M{"ts": bson.M{"$gte": date(from), "$lt": date(to)}}, options.Find().SetSort(bson.M{"ts": 1}))
}

// ---------- events and logs ----------

type ocppDoc struct {
	TS        bson.DateTime `bson:"ts"`
	OcppEntry `bson:",inline"`
}

type eventDoc struct {
	TS          bson.DateTime `bson:"ts"`
	EngineEvent `bson:",inline"`
}

// AddOcpp queues an OCPP frame.
func (o *Ops) AddOcpp(e OcppEntry) { o.ocppBuf.add(ocppDoc{TS: date(e.T), OcppEntry: e}) }

// AddEvent queues an engine event.
func (o *Ops) AddEvent(e EngineEvent) { o.eventBuf.add(eventDoc{TS: date(e.T), EngineEvent: e}) }

// AddChargerEvent stores an availability event.
func (o *Ops) AddChargerEvent(ctx context.Context, e ChargerEvent) error {
	_, err := o.ChargerEvents.InsertOne(ctx, e)
	return err
}

// ---------- generic ----------

func find[T any](ctx context.Context, c *mongo.Collection, filter any, opts *options.FindOptionsBuilder) ([]T, error) {
	var cur *mongo.Cursor
	var err error
	if opts != nil {
		cur, err = c.Find(ctx, filter, opts)
	} else {
		cur, err = c.Find(ctx, filter)
	}
	if err != nil {
		return nil, err
	}
	out := []T{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Find is the exported generic finder used by analytics.
func Find[T any](ctx context.Context, c *mongo.Collection, filter any, opts *options.FindOptionsBuilder) ([]T, error) {
	return find[T](ctx, c, filter, opts)
}
