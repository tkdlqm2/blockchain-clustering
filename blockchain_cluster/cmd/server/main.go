package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/config"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
	"github.com/tkdlqm2/blockchain-cluster/internal/metrics"
	"github.com/tkdlqm2/blockchain-cluster/internal/queryservice"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
)

func main() {
	_ = godotenv.Load() // 로컬 개발용. 컨테이너/운영에서는 환경변수가 이미 주입되어 있으므로 무시해도 안전.

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DSN())
	if err != nil {
		slog.Error("db pool init failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("db ping failed", "error", err)
		os.Exit(1)
	}

	evidenceStore := evidence.NewStore(pool)
	labelStore := label.NewStore(pool)
	registryStore := registry.NewStore(pool)
	queryDeps := queryservice.Deps{
		Cluster:  cluster.NewStore(pool, evidenceStore),
		Evidence: evidenceStore,
		Label:    labelStore,
		Address:  address.NewStore(pool),
	}

	metricsRegistry := prometheus.NewRegistry()
	metricsRegistry.MustRegister(metrics.NewCollector(pool, registryStore))

	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
	r.Mount("/", queryservice.NewRouter(queryDeps))

	go runLabelMaintenanceLoop(cfg, labelStore, registryStore)

	addr := ":" + cfg.App.Port
	slog.Info("starting server", "addr", addr, "env", cfg.App.Env)
	if err := http.ListenAndServe(addr, r); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// runLabelMaintenanceLoop periodically runs LabelStore.Maintain (docs/03 §8)
// across every registered chain. Interval/staleTTL/decayFactor come from
// config (docs/03 §10: no hardcoded parameters) — see
// LABEL_MAINTENANCE_* in .env.
func runLabelMaintenanceLoop(cfg *config.Config, labelStore *label.Store, chains *registry.Store) {
	interval, err := cfg.LabelMaintenanceInterval()
	if err != nil {
		slog.Error("label maintenance: invalid interval, loop disabled", "error", err)
		return
	}
	staleTTL, err := cfg.LabelMaintenanceStaleTTL()
	if err != nil {
		slog.Error("label maintenance: invalid stale_ttl, loop disabled", "error", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		chainIDs, err := chains.ListChains(ctx)
		if err != nil {
			slog.Error("label maintenance: list_chains failed", "error", err)
			cancel()
			continue
		}
		for _, chainID := range chainIDs {
			result, err := labelStore.Maintain(ctx, chainID, staleTTL, cfg.LabelMaintenance.DecayFactor)
			if err != nil {
				slog.Error("label maintenance failed", "chain", chainID, "error", err)
				continue
			}
			slog.Info("label maintenance complete", "chain", chainID, "decayed", result.Decayed, "conflicted", result.Conflicted)
		}
		cancel()
	}
}
