//go:build integration

package pipeline

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/heuristic"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
	"github.com/tkdlqm2/blockchain-cluster/internal/merge"
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
	"github.com/tkdlqm2/blockchain-cluster/internal/sweeptarget"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/pipeline/...
//
// This is the whole system through its single real entrypoint: ingest a
// common-input transaction, run the pipeline once, and confirm the cluster
// exists — proving every M0-M8 piece composes correctly end to end, not
// just individually.
func TestPipeline_Run_CommonInputEndToEnd(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

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

	p := New(ingestorStore, hubDetector, excludedStore, excludedStore, addrStore, registryStore, engines, mergeEngine, clusterStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b, c := run+"-A", run+"-B", run+"-C"
	txid := "tx-" + run

	deltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 0, Address: a, Amount: big.NewInt(-1000), BlockHeight: 500, BlockHash: "b-" + run},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 1, Address: b, Amount: big.NewInt(-2000), BlockHeight: 500, BlockHash: "b-" + run},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 2, Address: c, Amount: big.NewInt(-500), BlockHeight: 500, BlockHash: "b-" + run},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 3, Address: run + "-recipient", Amount: big.NewInt(3450), BlockHeight: 500, BlockHash: "b-" + run},
	}

	// Run() now ingests internally (that's the fix) — no manual Ingest() call
	// before it, proving Run() alone is sufficient as the single entrypoint.
	result, err := p.Run(ctx, "bitcoin", deltas)
	if err != nil {
		t.Fatalf("pipeline run: %v", err)
	}
	if result.Recorded == 0 {
		t.Fatalf("expected at least one recorded merge, got %+v", result)
	}

	same, _, err := clusterStore.SameCluster(ctx, "bitcoin", b, c, 0)
	if err != nil {
		t.Fatalf("same_cluster: %v", err)
	}
	if !same {
		t.Fatalf("expected %s and %s clustered via the pipeline's single entrypoint", b, c)
	}
}
