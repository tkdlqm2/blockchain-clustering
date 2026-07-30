//go:build integration

package merge

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
	"github.com/tkdlqm2/blockchain-cluster/internal/preprocessor"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered (adds
// the common-input row to chain_heuristic that CommonInputEngine reads).
// Run with: go test -tags=integration ./internal/merge/...
//
// This is the M2 DoD end to end (docs/05 §1 M2): a transaction's multiple
// inputs land in one cluster, and each merge has evidence carrying
// source_block_hash.
func TestM2Pipeline_CommonInputMergesIntoOneCluster(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	ingestorStore := ingestor.NewStore(pool, addrStore)
	excludedStore := preprocessor.NewStore(pool)
	registryStore := registry.NewStore(pool)
	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)

	engine := heuristic.NewCommonInputEngine(addrStore, excludedStore, registryStore)
	mergeEngine := NewEngine(evidenceStore, addrStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b, c := run+"-A", run+"-B", run+"-C"
	txid := "tx-" + run
	blockHash := "block-" + run

	deltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 0, Address: a, Amount: big.NewInt(-1000), Kind: "native", BlockHeight: 500, BlockHash: blockHash},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 1, Address: b, Amount: big.NewInt(-2000), Kind: "native", BlockHeight: 500, BlockHash: blockHash},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 2, Address: c, Amount: big.NewInt(-500), Kind: "native", BlockHeight: 500, BlockHash: blockHash},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 3, Address: run + "-recipient", Amount: big.NewInt(3450), Kind: "native", BlockHeight: 500, BlockHash: blockHash},
	}
	if _, err := ingestorStore.Ingest(ctx, deltas); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	got, err := ingestorStore.GetDeltasByTx(ctx, "bitcoin", txid)
	if err != nil {
		t.Fatalf("get_deltas_by_tx: %v", err)
	}

	candidates, err := engine.Generate(ctx, got)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates (star around anchor), got %d: %+v", len(candidates), candidates)
	}

	batchResult, err := mergeEngine.RecordAndMergeBatch(ctx, candidates)
	if err != nil {
		t.Fatalf("record_and_merge_batch: %v", err)
	}
	if batchResult.Recorded != 2 || batchResult.Rejected != 0 {
		t.Fatalf("expected 2 recorded / 0 rejected, got %+v", batchResult)
	}

	if err := clusterStore.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	same, _, err := clusterStore.SameCluster(ctx, "bitcoin", b, c, 0)
	if err != nil {
		t.Fatalf("same_cluster: %v", err)
	}
	if !same {
		t.Fatalf("expected %s and %s to be in the same cluster (common-input via %s)", b, c, txid)
	}

	clusterID, _, found, err := clusterStore.ClusterOf(ctx, "bitcoin", a, 0)
	if err != nil || !found {
		t.Fatalf("cluster_of(%s): found=%v err=%v", a, found, err)
	}
	members, err := clusterStore.MembersOf(ctx, "bitcoin", clusterID, 0, 100, 0)
	if err != nil {
		t.Fatalf("members_of: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("expected 3 members (A,B,C), got %v", members)
	}

	// AC-6 (docs/05): every on-chain merge is explained by evidence carrying source_block_hash.
	ev, err := evidenceStore.ByAddressPair(ctx, "bitcoin", a, b)
	if err != nil {
		t.Fatalf("by_address_pair: %v", err)
	}
	if len(ev) != 1 {
		t.Fatalf("expected exactly 1 evidence record for A-B, got %d", len(ev))
	}
	if ev[0].SourceBlockHash == nil || *ev[0].SourceBlockHash != blockHash {
		t.Fatalf("expected evidence to carry source_block_hash=%s, got %+v", blockHash, ev[0])
	}
	if ev[0].HeuristicKey != "common-input" {
		t.Fatalf("expected heuristic_key=common-input, got %s", ev[0].HeuristicKey)
	}
}
