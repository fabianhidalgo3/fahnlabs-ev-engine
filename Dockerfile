# syntax=docker/dockerfile:1
#
# Power control engine (motor, Go). Build context: motor/.
#
#   docker build -t emob/motor motor
#
# Runtime env: MOTOR_PORT (9200), POSTGRES_URL or POSTGRES_USER/PASSWORD/PORT/DB (host is fixed to 127.0.0.1 in that
# form, so in compose set POSTGRES_URL), MONGO_URI or MONGO_USER/PASSWORD/PORT (same), MONGO_DB, MOTOR_TOKEN,
# GATEWAY_URL (e.g. http://gateway:9100), GATEWAY_TOKEN. Listens on all interfaces (":$MOTOR_PORT").

ARG GO_VERSION=1.27.4

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=readonly
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "-s -w" -o /out/motor ./cmd/motor

# distroless static: CA certificates, tzdata, /etc/passwd with the nonroot user (65532), no shell.
FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=build /out/motor /app/motor
ENV MOTOR_PORT=9200
EXPOSE 9200
USER nonroot:nonroot
ENTRYPOINT ["/app/motor"]
