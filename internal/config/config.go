// Package config reads the motor settings from the environment (and an optional .env file).
package config

import (
	"bufio"
	"os"
	"strings"
)

// Config holds the runtime settings.
type Config struct {
	Port         string // HTTP port of the domain API (default 9200)
	PostgresURL  string // master data (vehicles, chargers)
	MongoURI     string // operational data
	MongoDB      string
	Token        string // optional shared secret with the BFF (X-Motor-Token)
	GatewayURL   string // OCPP gateway (default http://localhost:9100)
	GatewayToken string
}

// LoadDotEnv sets variables from KEY=VALUE lines of the given files without overriding the environment.
func LoadDotEnv(files ...string) {
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(fh)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			k, v = strings.TrimSpace(k), strings.Trim(strings.TrimSpace(v), `"'`)
			if _, set := os.LookupEnv(k); !set {
				_ = os.Setenv(k, v)
			}
		}
		_ = fh.Close()
	}
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

// Load reads the configuration; defaults match docker-compose.yml.
func Load() Config {
	return Config{
		Port:         env("MOTOR_PORT", "9200"),
		PostgresURL:  env("POSTGRES_URL", "postgres://"+env("POSTGRES_USER", "emob")+":"+env("POSTGRES_PASSWORD", "emob-dev")+"@127.0.0.1:"+env("POSTGRES_PORT", "5442")+"/"+env("POSTGRES_DB", "emob")+"?sslmode=disable"),
		MongoURI:     env("MONGO_URI", "mongodb://"+env("MONGO_USER", "emob")+":"+env("MONGO_PASSWORD", "emob-dev")+"@127.0.0.1:"+env("MONGO_PORT", "27027")+"/?authSource=admin"),
		MongoDB:      env("MONGO_DB", "emob"),
		Token:        env("MOTOR_TOKEN", ""),
		GatewayURL:   env("GATEWAY_URL", "http://localhost:9100"),
		GatewayToken: env("GATEWAY_TOKEN", ""),
	}
}
