// Package api exposes the motor's domain REST API (/api/v1) and the live events WebSocket. Errors follow RFC 9457 (application/problem+json).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/fhidalgo/emob/motor/internal/analytics"
	"github.com/fhidalgo/emob/motor/internal/csms"
	"github.com/fhidalgo/emob/motor/internal/energy"
	"github.com/fhidalgo/emob/motor/internal/gateway"
	"github.com/fhidalgo/emob/motor/internal/store"
)

const prefix = "/api/v1"
const dayMs = int64(86_400_000)

// Server wires the HTTP handlers.
type Server struct {
	CSMS      *csms.Csms
	Master    *store.Master
	Ops       *store.Ops
	Analytics *analytics.Analytics
	Gateway   *gateway.Client // OCPP gateway (generic OCPP commands)
	Token     string

	hub *hub
}

// Handler builds the HTTP handler.
func (s *Server) Handler() http.Handler {
	s.hub = newHub(s.CSMS)
	mux := http.NewServeMux()
	h := func(pattern string, fn func(w http.ResponseWriter, r *http.Request) (any, error)) {
		mux.HandleFunc(pattern, s.wrap(fn))
	}

	// State and configuration
	h("GET "+prefix+"/state", func(w http.ResponseWriter, r *http.Request) (any, error) { return s.CSMS.Snapshot(), nil })
	h("GET "+prefix+"/config", func(w http.ResponseWriter, r *http.Request) (any, error) { return s.CSMS.Config(), nil })
	h("PUT "+prefix+"/config", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var patch map[string]any
		if err := decode(r, &patch); err != nil {
			return nil, err
		}
		return s.CSMS.UpdateConfig(patch)
	})

	// Vehicles (PostgreSQL)
	h("GET "+prefix+"/vehicles", func(w http.ResponseWriter, r *http.Request) (any, error) { return s.Master.ListVehicles(r.Context()) })
	h("GET "+prefix+"/vehicles/{id}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		v, err := s.Master.GetVehicle(r.Context(), pathInt(r, "id"))
		if v == nil && err == nil {
			err = store.ErrNotFound
		}
		return v, err
	})
	h("POST "+prefix+"/vehicles", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var in store.VehicleInput
		if err := decode(r, &in); err != nil {
			return nil, err
		}
		v, err := s.Master.CreateVehicle(r.Context(), in)
		if err == nil {
			s.CSMS.AddLog("info", "Vehicle registered: "+v.Name, "")
			w.WriteHeader(http.StatusCreated)
		}
		return v, err
	})
	h("PUT "+prefix+"/vehicles/{id}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var in store.VehicleInput
		if err := decode(r, &in); err != nil {
			return nil, err
		}
		v, err := s.Master.UpdateVehicle(r.Context(), pathInt(r, "id"), in)
		if err == nil {
			s.CSMS.RefreshVehicles()
		}
		return v, err
	})
	h("DELETE "+prefix+"/vehicles/{id}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		err := s.Master.DeleteVehicle(r.Context(), pathInt(r, "id"))
		if err == nil {
			s.CSMS.RefreshVehicles()
		}
		return noContent, err
	})

	// Chargers (PostgreSQL registry + live OCPP commands)
	h("GET "+prefix+"/chargers", func(w http.ResponseWriter, r *http.Request) (any, error) { return s.Master.ListChargers(r.Context()) })
	h("POST "+prefix+"/chargers", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var p csms.ChargerPatch
		if err := decode(r, &p); err != nil {
			return nil, err
		}
		if err := s.CSMS.RegisterCharger(p); err != nil {
			return nil, err
		}
		w.WriteHeader(http.StatusCreated)
		return s.Master.GetCharger(r.Context(), strings.TrimSpace(p.ID))
	})
	h("PUT "+prefix+"/chargers/{id}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var p csms.ChargerPatch
		if err := decode(r, &p); err != nil {
			return nil, err
		}
		if err := s.CSMS.UpdateCharger(r.PathValue("id"), p); err != nil {
			return nil, err
		}
		return s.Master.GetCharger(r.Context(), r.PathValue("id"))
	})
	h("DELETE "+prefix+"/chargers/{id}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return noContent, s.CSMS.RemoveCharger(r.PathValue("id"))
	})
	type command struct {
		ConnectorID *int   `json:"connectorId"`
		IDTag       string `json:"idTag"`
		VehicleID   *int64 `json:"vehicleId"`
		Type        string `json:"type"`
	}
	cmd := func(fn func(id string, c command) (map[string]any, error)) func(w http.ResponseWriter, r *http.Request) (any, error) {
		return func(w http.ResponseWriter, r *http.Request) (any, error) {
			var c command
			if err := decode(r, &c); err != nil {
				return nil, err
			}
			if c.ConnectorID == nil {
				one := 1
				c.ConnectorID = &one
			}
			return fn(r.PathValue("id"), c)
		}
	}
	h("POST "+prefix+"/chargers/{id}/remote-start", cmd(func(id string, c command) (map[string]any, error) {
		tag := c.IDTag
		if tag == "" {
			tag = "REMOTE"
		}
		return s.CSMS.RemoteStart(id, *c.ConnectorID, tag, c.VehicleID)
	}))
	h("POST "+prefix+"/chargers/{id}/remote-stop", cmd(func(id string, c command) (map[string]any, error) {
		return s.CSMS.RemoteStop(id, *c.ConnectorID)
	}))
	h("POST "+prefix+"/chargers/{id}/reset", cmd(func(id string, c command) (map[string]any, error) { return s.CSMS.Reset(id, c.Type) }))
	// Any other OCPP 1.6 command (ChangeAvailability, UnlockConnector, Get/ChangeConfiguration, TriggerMessage…),
	// relayed to the OCPP gateway as is. Session control should use the endpoints above (they keep the CSMS state).
	h("POST "+prefix+"/chargers/{id}/ocpp/{action}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var payload json.RawMessage
		if err := decode(r, &payload); err != nil {
			return nil, err
		}
		if len(payload) == 0 {
			payload = json.RawMessage("{}")
		}
		return s.Gateway.Command(r.Context(), r.PathValue("id"), r.PathValue("action"), payload)
	})

	// Charging plans (MongoDB)
	h("GET "+prefix+"/charging-plans", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return s.Ops.ListPlans(r.Context(), qInt(r, "from", 0), qInt(r, "to", math.MaxInt64))
	})
	h("POST "+prefix+"/charging-plans", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var p csms.PlanRequest
		if err := decode(r, &p); err != nil {
			return nil, err
		}
		plan, err := s.CSMS.CreatePlan(p)
		if err == nil {
			w.WriteHeader(http.StatusCreated)
		}
		return plan, err
	})
	h("DELETE "+prefix+"/charging-plans/{id}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return noContent, s.CSMS.CancelPlan(pathInt(r, "id"))
	})

	// Sessions (MongoDB + live overlay)
	h("GET "+prefix+"/sessions/active", func(w http.ResponseWriter, r *http.Request) (any, error) {
		out := []csms.LiveSession{}
		for _, l := range s.CSMS.LiveSessions() {
			out = append(out, l)
		}
		return out, nil
	})
	h("GET "+prefix+"/sessions/{id}/samples", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return s.Ops.SessionSamples(r.Context(), pathInt(r, "id"))
	})
	h("GET "+prefix+"/sessions", func(w http.ResponseWriter, r *http.Request) (any, error) {
		now := time.Now().UnixMilli()
		rows, err := s.Ops.SessionsInRange(r.Context(), qInt(r, "from", now-7*dayMs), qInt(r, "to", now+dayMs), "")
		if err != nil {
			return nil, err
		}
		live := s.CSMS.LiveSessions()
		out := make([]any, 0, len(rows))
		for _, row := range rows {
			if l, ok := live[row.TransactionID]; ok {
				out = append(out, l)
			} else {
				out = append(out, row)
			}
		}
		return out, nil
	})

	// Logs
	logFilter := func(r *http.Request) store.LogFilter {
		q := r.URL.Query()
		return store.LogFilter{ChargerID: q.Get("chargerId"), Action: q.Get("action"), Dir: q.Get("direction"), Kind: q.Get("kind"),
			Level: q.Get("level"), OnlyErrors: q.Get("errorsOnly") == "true", Text: q.Get("text"),
			From: qInt(r, "from", 0), To: qInt(r, "to", 0), Limit: qInt(r, "limit", 100), Asc: q.Get("order") == "asc"}
	}
	hourAgo := func(f store.LogFilter) int64 {
		if f.From > 0 {
			return f.From
		}
		return time.Now().Add(-time.Hour).UnixMilli()
	}
	h("GET "+prefix+"/logs/ocpp", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return s.Ops.SearchOcpp(r.Context(), logFilter(r))
	})
	h("GET "+prefix+"/logs/events", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return s.Ops.SearchEvents(r.Context(), logFilter(r))
	})
	h("GET "+prefix+"/logs/commands", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return s.Ops.SearchCommands(r.Context(), logFilter(r))
	})
	h("GET "+prefix+"/logs/command-stats", func(w http.ResponseWriter, r *http.Request) (any, error) {
		f := logFilter(r)
		return s.Ops.CommandStats(r.Context(), hourAgo(f), f.ChargerID)
	})
	h("GET "+prefix+"/logs/summary", func(w http.ResponseWriter, r *http.Request) (any, error) {
		f := logFilter(r)
		return s.Ops.OcppSummary(r.Context(), f.ChargerID, hourAgo(f), f.To)
	})
	h("GET "+prefix+"/logs/actions", func(w http.ResponseWriter, r *http.Request) (any, error) { return s.Ops.OcppActions(r.Context()) })

	// Energy indicators
	h("GET "+prefix+"/energy/groups", func(w http.ResponseWriter, r *http.Request) (any, error) {
		g, err := energy.Groups(r.Context(), s.Ops)
		return map[string]any{"groups": g, "assumptions": energy.Assumptions(s.CSMS.Config().Energy)}, err
	})
	h("GET "+prefix+"/energy/report", func(w http.ResponseWriter, r *http.Request) (any, error) {
		to := qInt(r, "to", time.Now().UnixMilli())
		var groups []string
		for _, g := range strings.Split(r.URL.Query().Get("groups"), ",") {
			if g = strings.TrimSpace(g); g != "" {
				groups = append(groups, g)
			}
		}
		cfg := s.CSMS.Config()
		return energy.Report(r.Context(), s.Ops, s.Master, cfg.SiteLimitKw, cfg.Energy, qInt(r, "from", to-dayMs), to, groups)
	})

	// Predictive maintenance
	h("GET "+prefix+"/maintenance/health", func(w http.ResponseWriter, r *http.Request) (any, error) {
		to := time.Now().UnixMilli()
		from := to - int64(qFloat(r, "days", 7)*float64(dayMs))
		ids, err := s.Analytics.ChargerIDs(r.Context(), r.URL.Query().Get("chargerId"))
		if err != nil {
			return nil, err
		}
		if len(ids) > 40 {
			ids = ids[:40]
		}
		list := make([]map[string]any, len(ids))
		if err := parallel(r.Context(), len(ids), func(i int) error {
			var e error
			list[i], e = s.Analytics.Health(r.Context(), ids[i], from, to)
			return e
		}); err != nil {
			return nil, err
		}
		sortBy(list, "risk", "")
		return map[string]any{"from": from, "to": to, "chargers": list}, nil
	})
	h("GET "+prefix+"/maintenance/history", func(w http.ResponseWriter, r *http.Request) (any, error) {
		id := r.URL.Query().Get("chargerId")
		if id == "" {
			return nil, store.Invalid("chargerId is required")
		}
		days := qFloat(r, "days", 14)
		bucket := "day"
		if r.URL.Query().Get("bucket") == "hour" && days <= 3 {
			bucket = "hour"
		}
		to := time.Now().UnixMilli()
		series, err := s.Analytics.Series(r.Context(), id, to-int64(days*float64(dayMs)), to, bucket)
		return map[string]any{"chargerId": id, "bucket": bucket, "series": series}, err
	})
	h("GET "+prefix+"/maintenance/forecast", func(w http.ResponseWriter, r *http.Request) (any, error) {
		h, win := qFloat(r, "horizonDays", 7), qFloat(r, "windowDays", 30)
		ids, err := s.Analytics.ChargerIDs(r.Context(), r.URL.Query().Get("chargerId"))
		if err != nil {
			return nil, err
		}
		if len(ids) > 30 {
			ids = ids[:30]
		}
		list := make([]map[string]any, len(ids))
		if err := parallel(r.Context(), len(ids), func(i int) error {
			var e error
			list[i], e = s.Analytics.Predict(r.Context(), ids[i], h, win)
			return e
		}); err != nil {
			return nil, err
		}
		sortBy(list, "faultProbability", "incidentProbability")
		return map[string]any{"horizonDays": h, "windowDays": win, "forecasts": list}, nil
	})

	// Read-only queries for the assistant
	h("POST "+prefix+"/queries/sql", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var q struct {
			SQL string `json:"sql"`
		}
		if err := decode(r, &q); err != nil {
			return nil, err
		}
		rows, truncated, err := s.Master.ReadOnlyQuery(r.Context(), q.SQL, 200)
		return map[string]any{"rows": rows, "truncated": truncated}, err
	})
	h("POST "+prefix+"/queries/mongo", func(w http.ResponseWriter, r *http.Request) (any, error) {
		var q struct {
			Collection string   `json:"collection"`
			Pipeline   []bson.M `json:"pipeline"`
		}
		var raw json.RawMessage
		if err := decode(r, &raw); err != nil {
			return nil, err
		}
		// Extended JSON so the assistant can use {"$date": ...} if needed
		if err := bson.UnmarshalExtJSON(raw, false, &q); err != nil {
			return nil, store.Invalid("Invalid pipeline: " + err.Error())
		}
		rows, truncated, err := s.Ops.ReadOnlyAggregate(r.Context(), q.Collection, q.Pipeline, 200)
		return map[string]any{"rows": rows, "truncated": truncated}, err
	})

	// Raw rows for exports (the BFF turns them into CSV)
	h("GET "+prefix+"/exports/{name}", func(w http.ResponseWriter, r *http.Request) (any, error) {
		ctx := r.Context()
		switch r.PathValue("name") {
		case "sessions":
			return s.Ops.ExportSessions(ctx)
		case "meter-samples":
			return s.Ops.ExportMeterSamples(ctx, qInt(r, "transactionId", 0))
		case "site-samples":
			return s.Ops.SiteSamplesFrom(ctx, qInt(r, "from", 0), math.MaxInt64)
		case "vehicles":
			return s.Master.ListVehicles(ctx)
		case "chargers":
			return s.Master.ListChargers(ctx)
		}
		return nil, store.ErrNotFound
	})

	h("GET "+prefix+"/health", func(w http.ResponseWriter, r *http.Request) (any, error) {
		return map[string]any{"status": "UP", "service": "motor", "connectedChargers": s.CSMS.ConnectedCount()}, nil
	})

	// Live events
	mux.HandleFunc("GET "+prefix+"/events", func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			problem(w, r, http.StatusUnauthorized, "Unauthorized", "Invalid motor token")
			return
		}
		s.hub.serve(w, r)
	})
	registerDocs(mux, "Motor API")
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"service": "motor (power control domain service)", "api": prefix, "events": prefix + "/events",
			"ocpp": "through the OCPP gateway", "openapi": prefix + "/openapi.yaml", "docs": prefix + "/docs"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		problem(w, r, http.StatusNotFound, "Not Found", "No route for "+r.Method+" "+r.URL.Path)
	})
	return withCORS(mux)
}

// withCORS lets test tools on another origin (the web emulator served by the gateway) read the vehicle list.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == prefix+"/vehicles" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		next.ServeHTTP(w, r)
	})
}

var noContent = struct{ noContent bool }{true}

func (s *Server) authorized(r *http.Request) bool {
	return s.Token == "" || r.Header.Get("X-Motor-Token") == s.Token
}

// wrap adds authentication, JSON encoding and error mapping to a handler.
func (s *Server) wrap(fn func(w http.ResponseWriter, r *http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			problem(w, r, http.StatusUnauthorized, "Unauthorized", "Invalid motor token")
			return
		}
		rw := &statusWriter{ResponseWriter: w}
		out, err := fn(rw, r)
		if err != nil {
			status, title := classify(err)
			if status >= 500 {
				slog.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
			}
			problem(w, r, status, title, err.Error())
			return
		}
		if out == noContent {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		status := rw.status
		if status == 0 {
			status = http.StatusOK
		}
		writeJSON(w, status, out)
	}
}

// statusWriter defers WriteHeader so handlers can choose 201 before the body is written.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) { s.status = code }

func classify(err error) (int, string) {
	var ve *store.ValidationError
	var oe *gateway.Error
	switch {
	case errors.As(err, &ve):
		return http.StatusBadRequest, "Bad Request"
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "Not Found"
	case errors.As(err, &oe):
		if oe.Code == "NotSupported" {
			return http.StatusBadRequest, "Bad Request"
		}
		if oe.Code == "GatewayUnavailable" {
			return http.StatusServiceUnavailable, "OCPP gateway unavailable"
		}
		if oe.Code == "NotConnected" {
			return http.StatusConflict, "Charger not connected"
		}
		if oe.Code == "Timeout" {
			return http.StatusGatewayTimeout, "Charger did not answer"
		}
		return http.StatusBadGateway, "Charger error"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "Timeout"
	}
	return http.StatusInternalServerError, "Internal Server Error"
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func problem(w http.ResponseWriter, r *http.Request, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "about:blank", "title": title, "status": status, "detail": detail, "instance": r.URL.Path})
}

func decode(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	if r.ContentLength == 0 {
		return nil
	}
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return store.Invalid("Invalid JSON body: " + err.Error())
	}
	return nil
}

func pathInt(r *http.Request, k string) int64 {
	n, _ := strconv.ParseInt(r.PathValue(k), 10, 64)
	return n
}

func qInt(r *http.Request, k string, def int64) int64 {
	if v := r.URL.Query().Get(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f)
		}
	}
	return def
}

func qFloat(r *http.Request, k string, def float64) float64 {
	if v := r.URL.Query().Get(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// parallel runs fn(0..n-1) with up to 6 workers and returns the first error.
func parallel(ctx context.Context, n int, fn func(i int) error) error {
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	sem := make(chan struct{}, 6)
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(i); err != nil {
				once.Do(func() { first = err })
			}
		}(i)
	}
	wg.Wait()
	return first
}

func num(v any) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case float64:
		return x
	case *float64:
		if x != nil {
			return *x
		}
	}
	return 0
}

func sortBy(list []map[string]any, k1, k2 string) {
	for i := 1; i < len(list); i++ {
		for j := i; j > 0; j-- {
			a, b := list[j-1], list[j]
			if num(a[k1]) > num(b[k1]) || (num(a[k1]) == num(b[k1]) && (k2 == "" || num(a[k2]) >= num(b[k2]))) {
				break
			}
			list[j-1], list[j] = b, a
		}
	}
}
