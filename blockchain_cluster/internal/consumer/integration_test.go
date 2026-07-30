//go:build integration

package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/heuristic"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
	"github.com/tkdlqm2/blockchain-cluster/internal/merge"
	"github.com/tkdlqm2/blockchain-cluster/internal/pipeline"
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
	"github.com/tkdlqm2/blockchain-cluster/internal/reorg"
	"github.com/tkdlqm2/blockchain-cluster/internal/sweeptarget"
)

// Requires: `docker compose up -d` (or the indexer project's docker-compose,
// which exposes the same balance-deltas topic on localhost:9092 — see
// docs/08-indexer-contract.md) and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/consumer/...
//
// This proves the real kafka-go wiring (not just the fakes in
// consumer_test.go): publish a genuine Kafka message, consume it with a
// real Consumer wired to the real Pipeline/ReorgHandler (same construction
// as cmd/consumer/main.go), and confirm the resulting cluster exists.
func TestConsumer_RealKafkaEndToEnd(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	brokers := []string{"localhost:9092"}
	topic := "balance-deltas"

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

	engines := []heuristic.Engine{
		heuristic.NewCommonInputEngine(addrStore, excludedStore, registryStore),
		heuristic.NewSweepEngine(pool, targetStore, registryStore),
		heuristic.NewChangeEngine(pool, addrStore, excludedStore, addrStore, registryStore),
	}
	p := pipeline.New(ingestorStore, hubDetector, excludedStore, excludedStore, addrStore, registryStore, engines, mergeEngine, clusterStore)
	reorgHandler := reorg.NewHandler(evidenceStore, clusterStore, nil)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	groupID := "test-" + run
	a, b := run+"-A", run+"-B"
	txid := "tx-" + run
	blockHash := "b-" + run

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	c := New(reader, p, reorgHandler, 2*time.Second)
	consumerErrCh := make(chan error, 1)
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()
	go func() { consumerErrCh <- c.Run(consumerCtx) }()

	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}}
	defer writer.Close()

	deltaJSON := func(addr string, idx int, amount string) []byte {
		b, _ := json.Marshal(map[string]any{
			"type": "balance_delta", "chain_id": "bitcoin", "txid": txid,
			"delta_index": idx, "address": addr, "amount": amount, "kind": "native",
			"block_height": 999, "block_hash": blockHash,
		})
		return b
	}
	// Two spends in the same tx — a common-input pair, same as every other
	// heuristic test in this codebase, just arriving over real Kafka this time.
	messages := []kafka.Message{
		{Key: []byte("bitcoin"), Value: deltaJSON(a, 0, "-1000")},
		{Key: []byte("bitcoin"), Value: deltaJSON(b, 1, "-2000")},
		{Key: []byte("bitcoin"), Value: deltaJSON(run+"-recipient", 2, "3000")},
		// A different block to force the batcher to flush the one above.
		{Key: []byte("bitcoin"), Value: mustJSON(map[string]any{
			"type": "balance_delta", "chain_id": "bitcoin", "txid": "tx-" + run + "-next",
			"delta_index": 0, "address": run + "-unrelated", "amount": "-1", "kind": "native",
			"block_height": 1000, "block_hash": "b-" + run + "-next",
		})},
	}
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		t.Fatalf("publish test messages: %v", err)
	}

	// Poll rather than assume timing: the consumer group may have backlog
	// to work through before it reaches our uniquely-tagged messages.
	deadline := time.Now().Add(30 * time.Second)
	var same bool
	var lastErr error
	for time.Now().Before(deadline) {
		var found bool
		found, _, lastErr = clusterStore.SameCluster(ctx, "bitcoin", a, b, 0)
		if lastErr == nil && found {
			same = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	stopConsumer()
	<-consumerErrCh

	if !same {
		t.Fatalf("expected %s and %s clustered via the real Kafka consumer within the deadline (lastErr=%v)", a, b, lastErr)
	}
}

func mustJSON(v map[string]any) []byte {
	b, _ := json.Marshal(v)
	return b
}
