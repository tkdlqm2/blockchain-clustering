//go:build integration

package reorg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/audit"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/reorg/...
//
// This is the M5 DoD verbatim (docs/05 §1 M5 / AC-2): rolling back a
// specific block_hash removes only the merges anchored to it; everything
// else survives, and the result matches a fresh replay.
func TestOnReorg_DoD_OnlyRolledBackBlockMergesAreUndone(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	auditStore := audit.NewStore(pool)
	handler := NewHandler(evidenceStore, clusterStore, auditStore)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b, c, d := run+"-A", run+"-B", run+"-C", run+"-D"
	survivingBlock := "keep-" + run
	rolledBackBlock := "rollback-" + run

	// A-B merged via a block that will get rolled back; C-D merged via an
	// unrelated block that must survive untouched.
	if _, err := evidenceStore.Append(ctx, domain.MergeEvidence{
		ChainID: "bitcoin", AddressA: a, AddressB: b, HeuristicKey: "common-input",
		SourceBlockHash: &rolledBackBlock, Confidence: 0.95,
	}); err != nil {
		t.Fatalf("append A-B: %v", err)
	}
	if _, err := evidenceStore.Append(ctx, domain.MergeEvidence{
		ChainID: "bitcoin", AddressA: c, AddressB: d, HeuristicKey: "common-input",
		SourceBlockHash: &survivingBlock, Confidence: 0.95,
	}); err != nil {
		t.Fatalf("append C-D: %v", err)
	}
	if err := clusterStore.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	sameAB, _, err := clusterStore.SameCluster(ctx, "bitcoin", a, b, 0)
	if err != nil || !sameAB {
		t.Fatalf("expected A,B clustered before reorg: same=%v err=%v", sameAB, err)
	}
	sameCD, _, err := clusterStore.SameCluster(ctx, "bitcoin", c, d, 0)
	if err != nil || !sameCD {
		t.Fatalf("expected C,D clustered before reorg: same=%v err=%v", sameCD, err)
	}

	invalidated, err := handler.OnReorg(ctx, "bitcoin", []string{rolledBackBlock})
	if err != nil {
		t.Fatalf("on_reorg: %v", err)
	}
	if invalidated != 1 {
		t.Fatalf("expected exactly 1 evidence row invalidated, got %d", invalidated)
	}

	// A-B's only evidence is gone: neither should be in any cluster anymore.
	_, _, aInCluster, err := clusterStore.ClusterOf(ctx, "bitcoin", a, 0)
	if err != nil {
		t.Fatalf("cluster_of(a): %v", err)
	}
	if aInCluster {
		t.Fatalf("expected %s to have no cluster after its only evidence was rolled back", a)
	}

	// C-D's evidence is untouched: they must still be clustered together,
	// matching a fresh replay exactly (AC-1 determinism + AC-2 rollback).
	sameCDAfter, _, err := clusterStore.SameCluster(ctx, "bitcoin", c, d, 0)
	if err != nil || !sameCDAfter {
		t.Fatalf("expected C,D to remain clustered after an unrelated reorg: same=%v err=%v", sameCDAfter, err)
	}

	// A second OnReorg for the same block is a no-op (idempotent) — nothing
	// left to invalidate, no rebuild triggered.
	again, err := handler.OnReorg(ctx, "bitcoin", []string{rolledBackBlock})
	if err != nil {
		t.Fatalf("on_reorg (repeat): %v", err)
	}
	if again != 0 {
		t.Fatalf("expected repeat OnReorg for the same block to invalidate nothing new, got %d", again)
	}
}

func TestOnManualCorrection_DoD_Live(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	handler := NewHandler(evidenceStore, clusterStore, nil)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b := run+"-A", run+"-B"
	blockHash := "b-" + run

	opID, err := evidenceStore.Append(ctx, domain.MergeEvidence{
		ChainID: "bitcoin", AddressA: a, AddressB: b, HeuristicKey: "manual",
		SourceBlockHash: &blockHash, Confidence: 0.5,
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := clusterStore.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	same, _, err := clusterStore.SameCluster(ctx, "bitcoin", a, b, 0)
	if err != nil || !same {
		t.Fatalf("expected A,B clustered before correction: same=%v err=%v", same, err)
	}

	changed, err := handler.OnManualCorrection(ctx, "bitcoin", opID, "operator determined this was a false positive")
	if err != nil {
		t.Fatalf("on_manual_correction: %v", err)
	}
	if !changed {
		t.Fatalf("expected the correction to report a change")
	}

	sameAfter, _, err := clusterStore.SameCluster(ctx, "bitcoin", a, b, 0)
	if err != nil {
		t.Fatalf("same_cluster after correction: %v", err)
	}
	if sameAfter {
		t.Fatalf("expected A,B to no longer be clustered after manual correction")
	}
}
