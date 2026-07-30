//go:build integration

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered
// (see README §1) so the merge_evidence_bitcoin/cluster_bitcoin/
// cluster_membership_bitcoin partitions exist.
// Run with: go test -tags=integration ./internal/cluster/...

// TestRebuildFromEvidence_LiveDeterminismAndReorg exercises the M0 DoD end
// to end against a live database: append evidence, rebuild, read back
// through ClusterOf/MembersOf/SameCluster, then invalidate one op (as
// ReorgHandler/onManualCorrection would) and confirm the rebuild reflects
// exactly that change (docs/03 §9, docs/05 AC-1/AC-2).
func TestRebuildFromEvidence_LiveDeterminismAndReorg(t *testing.T) {
	pool := integrationtest.Pool(t)
	ev := evidence.NewStore(pool)
	store := NewStore(pool, ev)
	ctx := context.Background()

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b, c := run+"-A", run+"-B", run+"-C"

	blockHash := "blockhash-" + run
	opAB, err := ev.Append(ctx, domain.MergeEvidence{
		ChainID: "bitcoin", AddressA: a, AddressB: b, HeuristicKey: "common-input",
		SourceBlockHash: &blockHash, Confidence: 0.95,
	})
	if err != nil {
		t.Fatalf("append A-B: %v", err)
	}
	if _, err := ev.Append(ctx, domain.MergeEvidence{
		ChainID: "bitcoin", AddressA: a, AddressB: c, HeuristicKey: "common-input",
		SourceBlockHash: &blockHash, Confidence: 0.95,
	}); err != nil {
		t.Fatalf("append A-C: %v", err)
	}

	if err := store.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild (1st): %v", err)
	}

	clusterID, _, found, err := store.ClusterOf(ctx, "bitcoin", a, 0)
	if err != nil || !found {
		t.Fatalf("cluster_of(%s): found=%v err=%v", a, found, err)
	}
	same, _, err := store.SameCluster(ctx, "bitcoin", b, c, 0)
	if err != nil {
		t.Fatalf("same_cluster: %v", err)
	}
	if !same {
		t.Fatalf("expected %s and %s to be in the same cluster via common anchor %s", b, c, a)
	}

	members, err := store.MembersOf(ctx, "bitcoin", clusterID, 0, 100, 0)
	if err != nil {
		t.Fatalf("members_of: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("expected 3 members (A,B,C), got %v", members)
	}

	// AC-1: rebuilding again on unchanged evidence must not move the cluster.
	if err := store.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild (2nd, no-op): %v", err)
	}
	clusterIDAgain, _, _, err := store.ClusterOf(ctx, "bitcoin", a, 0)
	if err != nil {
		t.Fatalf("cluster_of after no-op rebuild: %v", err)
	}
	if clusterIDAgain != clusterID {
		t.Fatalf("cluster_id changed across a no-op rebuild: %s -> %s", clusterID, clusterIDAgain)
	}

	// Simulate onManualCorrection/onReorg (docs/03 §9): invalidate the A-B
	// edge and rebuild. A should be dropped from the cluster; B and C, whose
	// only surviving evidence is A-C... wait: invalidating A-B leaves only
	// A-C active, so B should be the one dropped and A,C remain clustered.
	if invalidated, err := ev.Invalidate(ctx, "bitcoin", opAB, "manual-correction"); err != nil {
		t.Fatalf("invalidate: %v", err)
	} else if !invalidated {
		t.Fatalf("expected invalidate to report a state change")
	}
	if err := store.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild (after invalidate): %v", err)
	}

	_, _, bStillMember, err := store.ClusterOf(ctx, "bitcoin", b, 0)
	if err != nil {
		t.Fatalf("cluster_of(%s) after invalidate: %v", b, err)
	}
	if bStillMember {
		t.Fatalf("%s should have been dropped after its only evidence (A-B) was invalidated", b)
	}

	sameAC, _, err := store.SameCluster(ctx, "bitcoin", a, c, 0)
	if err != nil {
		t.Fatalf("same_cluster(A,C) after invalidate: %v", err)
	}
	if !sameAC {
		t.Fatalf("%s and %s should remain clustered via the surviving A-C evidence", a, c)
	}
}

// TestClusterOf_ThresholdViewSplitsWeaklyBridgedClusters is the M6 DoD
// (docs/05 §1 M6, docs/03 §7): "그 임계치 이상 근거로만 재구성한 클러스터"
// — removing a weak bridge edge at a higher min_confidence must split what
// is one cluster at min_confidence=0 into two, not just prune members from
// a single precomputed view.
func TestClusterOf_ThresholdViewSplitsWeaklyBridgedClusters(t *testing.T) {
	pool := integrationtest.Pool(t)
	ev := evidence.NewStore(pool)
	store := NewStore(pool, ev)
	ctx := context.Background()

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b, c, d := run+"-A", run+"-B", run+"-C", run+"-D"
	blockHash := "blockhash-" + run

	strong := 0.9
	weak := 0.2
	for _, pair := range []struct {
		x, y string
		conf float64
	}{
		{a, b, strong}, // blob1
		{c, d, strong}, // blob2
		{b, c, weak},   // weak bridge between the two blobs
	} {
		if _, err := ev.Append(ctx, domain.MergeEvidence{
			ChainID: "bitcoin", AddressA: pair.x, AddressB: pair.y, HeuristicKey: "change",
			SourceBlockHash: &blockHash, Confidence: pair.conf,
		}); err != nil {
			t.Fatalf("append %s-%s: %v", pair.x, pair.y, err)
		}
	}
	if err := store.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	// At threshold 0 (the materialized full view), the weak bridge still
	// counts — all four are one cluster.
	sameFull, _, err := store.SameCluster(ctx, "bitcoin", a, d, 0)
	if err != nil {
		t.Fatalf("same_cluster (full view): %v", err)
	}
	if !sameFull {
		t.Fatalf("expected A,D in the same cluster at min_confidence=0 (weak bridge still counts)")
	}

	// At a threshold above the bridge's confidence but below the blobs'
	// internal confidence, the bridge evidence is excluded from
	// reconstruction entirely — the two blobs must come apart.
	sameThreshold, _, err := store.SameCluster(ctx, "bitcoin", a, d, 0.5)
	if err != nil {
		t.Fatalf("same_cluster (threshold view): %v", err)
	}
	if sameThreshold {
		t.Fatalf("expected A,D to split into separate clusters at min_confidence=0.5 (bridge confidence 0.2 excluded)")
	}

	// But each blob's own strong internal edge must still hold at that threshold.
	sameBlob1, _, err := store.SameCluster(ctx, "bitcoin", a, b, 0.5)
	if err != nil || !sameBlob1 {
		t.Fatalf("expected A,B to remain clustered at threshold 0.5: same=%v err=%v", sameBlob1, err)
	}
	sameBlob2, _, err := store.SameCluster(ctx, "bitcoin", c, d, 0.5)
	if err != nil || !sameBlob2 {
		t.Fatalf("expected C,D to remain clustered at threshold 0.5: same=%v err=%v", sameBlob2, err)
	}

	clusterA, _, foundA, err := store.ClusterOf(ctx, "bitcoin", a, 0.5)
	if err != nil || !foundA {
		t.Fatalf("cluster_of(A) at threshold: found=%v err=%v", foundA, err)
	}
	membersA, err := store.MembersOf(ctx, "bitcoin", clusterA, 0.5, 100, 0)
	if err != nil {
		t.Fatalf("members_of at threshold: %v", err)
	}
	if len(membersA) != 2 {
		t.Fatalf("expected exactly 2 members (A,B) in A's threshold-view cluster, got %v", membersA)
	}
}
