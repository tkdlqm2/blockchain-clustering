// Command consumer is the Kafka consumer that bridges the indexer (a
// separate project — docs/08-indexer-contract.md) to this system's
// pipeline. It is deliberately a separate binary from cmd/server: the
// consumer is a long-running worker with its own failure/restart
// characteristics, while cmd/server is the request-serving API + scheduler.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/audit"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/config"
	"github.com/tkdlqm2/blockchain-cluster/internal/consumer"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/heuristic"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
	"github.com/tkdlqm2/blockchain-cluster/internal/merge"
	"github.com/tkdlqm2/blockchain-cluster/internal/pipeline"
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
	"github.com/tkdlqm2/blockchain-cluster/internal/reorg"
	"github.com/tkdlqm2/blockchain-cluster/internal/sweeptarget"
)

func main() {
	_ = godotenv.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load("configs/config.yaml")
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	flushInterval, err := cfg.ConsumerFlushInterval()
	if err != nil {
		slog.Error("invalid KAFKA_CONSUMER_FLUSH_INTERVAL", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	registryStore := registry.NewStore(pool)
	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	mergeEngine := merge.NewEngine(evidenceStore, addrStore)
	excludedStore := preprocessor.NewStore(pool)
	labelStore := label.NewStore(pool)
	hubDetector := preprocessor.NewHubDetector(pool, labelStore, addrStore)
	targetStore := sweeptarget.NewStore(pool)
	auditStore := audit.NewStore(pool)

	// All 6 engines are always wired in; each is a no-op for chains where
	// the registry doesn't enable it (docs/06 §2.2 model_type mapping) — the
	// consumer doesn't need to know which chains are UTXO vs account.
	engines := []heuristic.Engine{
		heuristic.NewCommonInputEngine(addrStore, excludedStore, registryStore),
		heuristic.NewSweepEngine(pool, targetStore, registryStore),
		heuristic.NewChangeEngine(pool, addrStore, excludedStore, addrStore, registryStore),
		heuristic.NewFundingEngine(addrStore, addrStore, registryStore),
		heuristic.NewDeployerEngine(addrStore, addrStore, registryStore),
		heuristic.NewBehavioralEngine(addrStore, heuristic.NewSQLInteractionCounter(pool), registryStore),
	}

	p := pipeline.New(
		ingestorStore, hubDetector, excludedStore, excludedStore, addrStore,
		registryStore, engines, mergeEngine, clusterStore,
	)
	reorgHandler := reorg.NewHandler(evidenceStore, clusterStore, auditStore)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokerList(),
		Topic:   cfg.Kafka.TopicBalanceDelta,
		GroupID: cfg.Kafka.ConsumerGroupID,
	})
	defer reader.Close()

	c := consumer.New(reader, p, reorgHandler, flushInterval)

	slog.Info("consumer starting",
		"brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.TopicBalanceDelta,
		"group_id", cfg.Kafka.ConsumerGroupID, "flush_interval", flushInterval.String())

	if err := c.Run(ctx); err != nil {
		slog.Error("consumer stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("consumer shut down cleanly")
}
