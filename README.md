# ev-engine · CSMS and power allocation engine

Part of **EV yard power control** by FAHNLABS: real-time power allocation for an electric-vehicle charging yard.
Overview, architecture and deployment: [fahnlabs-ev-deploy](https://github.com/fabianhidalgo3/fahnlabs-ev-deploy).

The engine (called *motor* in the code) is the domain service: OCPP 1.6J central system logic, the power allocation
algorithm that keeps the yard under its contracted limit, charging plans, energy and maintenance analytics, and the
only owner of the data. It talks to chargers through [ev-gateway](https://github.com/fabianhidalgo3/fahnlabs-ev-gateway)
and serves [ev-bff](https://github.com/fabianhidalgo3/fahnlabs-ev-bff).

## Stack

Go 1.27 · gorilla/websocket · pgx 5 (PostgreSQL 19) · MongoDB Go driver 2 (MongoDB 9).

## Run

Start the databases with `docker compose up -d` in `ev-deploy`, then:

```bash
go test ./...                     # allocation engine tests
go build -o bin/motor ./cmd/motor
./bin/motor                       # :9200
```

| Variable | Default | Purpose |
|---|---|---|
| `MOTOR_PORT` | `9200` | HTTP port |
| `POSTGRES_URL` | `postgres://emob:emob-dev@127.0.0.1:5442/emob` | Master data: vehicles, chargers |
| `MONGO_URI` / `MONGO_DB` | `mongodb://emob:emob-dev@127.0.0.1:27027` / `emob` | Sessions, plans, meter and yard samples, OCPP log |
| `GATEWAY_URL` / `GATEWAY_TOKEN` | `http://localhost:9100` / empty | OCPP gateway |
| `MOTOR_TOKEN` | empty | Shared secret required from the BFF (`X-Motor-Token`) |

## Allocation engine

Four strategies: equal share (bisection water level), by priority, first come first served, and by energy need
(pending energy over hours to departure), with IEC 61851 minimum currents, a safety margin and safe transitions
(decreases first, increases re-checked after 2.5 s). Details:
[docs/power-engine.md](https://github.com/fabianhidalgo3/fahnlabs-ev-deploy/blob/main/docs/power-engine.md).

## Depot simulator

`cmd/depotsim` runs offline scenarios (JSON in `cmd/depotsim/scenarios`) and compares uncoordinated charging with
the coordinated strategies:

```bash
go run ./cmd/depotsim -out ../ev-deploy/docs/results
```

## API

REST under `/api/v1` (vehicles, chargers, OCPP commands, charging plans, sessions, logs, energy, maintenance,
read-only queries, exports), live events at `ws://…/api/v1/events`, Swagger UI at `/api/v1/docs`.
Reference: [docs/api.md](https://github.com/fabianhidalgo3/fahnlabs-ev-deploy/blob/main/docs/api.md).

## Code

| Path | Content |
|---|---|
| `cmd/motor` | Entry point |
| `cmd/depotsim` | Offline depot simulator |
| `internal/engine` | Allocation algorithm (pure functions) and tests |
| `internal/csms` | OCPP handlers, power manager, charging plans, live snapshot |
| `internal/gateway` | Client of the OCPP gateway |
| `internal/store` | PostgreSQL and MongoDB stores |
| `internal/analytics`, `internal/energy` | Maintenance analytics and energy indicators |
| `internal/api` | HTTP API, events hub, OpenAPI |
| `internal/config` | Environment configuration |

## Author

© 2026 Fabián Hidalgo · FAHNLABS. All rights reserved.
