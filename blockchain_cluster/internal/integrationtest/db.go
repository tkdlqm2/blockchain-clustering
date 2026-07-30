//go:build integration

// Package integrationtest provides a live Postgres connection for tests
// tagged `integration` (run with `go test -tags=integration ./...` while
// `docker compose up -d` is running). These are excluded from the default
// `go test ./...` run since they require a live database.
package integrationtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"github.com/tkdlqm2/blockchain-cluster/internal/config"
)

// Pool connects using the same .env + configs/config.yaml the app itself
// loads, resolved from the repo root regardless of which package's test
// directory this runs from.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	root := repoRoot(t)

	_ = godotenv.Load(filepath.Join(root, ".env"))
	cfg, err := config.Load(filepath.Join(root, "configs", "config.yaml"))
	if err != nil {
		t.Fatalf("integrationtest: load config: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), cfg.DSN())
	if err != nil {
		t.Fatalf("integrationtest: connect: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("integrationtest: ping (is `docker compose up -d` running?): %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("integrationtest: getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("integrationtest: could not locate repo root (no go.mod found above %s)", dir)
		}
		dir = parent
	}
}
