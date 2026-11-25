// Command motor is the power control engine (domain service) of the EV charging yard: central-system logic,
// power allocation, domain data (PostgreSQL + MongoDB) and domain REST API. OCPP transport lives in the separate
// OCPP gateway, which the motor uses through its command API and event stream.
//
//	http://<host>:9200/api/v1/...    domain REST API (consumed by the BFF)
//	ws://<host>:9200/api/v1/events   live state snapshots (~2 per second)
//
// Author: Fabián Hidalgo.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fhidalgo/emob/motor/internal/analytics"
	"github.com/fhidalgo/emob/motor/internal/api"
	"github.com/fhidalgo/emob/motor/internal/config"
	"github.com/fhidalgo/emob/motor/internal/csms"
	"github.com/fhidalgo/emob/motor/internal/gateway"
	"github.com/fhidalgo/emob/motor/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	config.LoadDotEnv(".env", "motor/.env", "../.env")
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	master, err := store.OpenMaster(startCtx, cfg.PostgresURL)
	if err != nil {
		fatal("PostgreSQL unavailable (start it with: docker compose up -d)", err)
	}
	ops, err := store.OpenOps(startCtx, cfg.MongoURI, cfg.MongoDB)
	if err != nil {
		fatal("MongoDB unavailable (start it with: docker compose up -d)", err)
	}
	gw := gateway.New(cfg.GatewayURL, cfg.GatewayToken)
	engine, err := csms.New(startCtx, master, ops, gw)
	cancel()
	if err != nil {
		fatal("could not start the CSMS", err)
	}
	go gw.Run(ctx, engine) // charger events and CALLs from the OCPP gateway

	srv := &api.Server{CSMS: engine, Master: master, Ops: ops, Analytics: analytics.New(ops, master), Gateway: gw, Token: cfg.Token}
	httpSrv := &http.Server{Addr: ":" + cfg.Port, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}

	go func() {
		slog.Info("power control engine listening", "port", cfg.Port, "api", "http://localhost:"+cfg.Port+"/api/v1",
			"events", "ws://localhost:"+cfg.Port+"/api/v1/events", "gateway", cfg.GatewayURL)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal("HTTP server", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
	ops.Close(shutCtx)
	master.Close()
}

func fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}
