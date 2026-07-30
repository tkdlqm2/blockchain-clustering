package cluster

import (
	"sort"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

func ev(opID int64, a, b, heuristic string, confidence float64, status string) domain.MergeEvidence {
	return domain.MergeEvidence{
		ChainID:      "bitcoin",
		OpID:         opID,
		AddressA:     a,
		AddressB:     b,
		HeuristicKey: heuristic,
		Confidence:   confidence,
		Status:       status,
	}
}

func byClusterID(results []Result) map[string]Result {
	m := make(map[string]Result, len(results))
	for _, r := range results {
		m[r.ClusterID] = r
	}
	return m
}

func memberSet(r Result) map[string]bool {
	s := make(map[string]bool, len(r.Members))
	for _, m := range r.Members {
		s[m.Address] = true
	}
	return s
}

// AC-1 (docs/05 §2): replaying the same active evidence set always yields
// the same clusters.
func TestReplay_Deterministic(t *testing.T) {
	evidence := []domain.MergeEvidence{
		ev(1, "A", "B", "common-input", 0.95, domain.EvidenceStatusActive),
		ev(2, "A", "C", "common-input", 0.95, domain.EvidenceStatusActive),
		ev(3, "D", "E", "sweep-seed", 0.90, domain.EvidenceStatusActive),
	}

	first := Replay(evidence)
	second := Replay(evidence)

	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("expected 2 clusters, got %d and %d", len(first), len(second))
	}

	f, s := byClusterID(first), byClusterID(second)
	for id, rf := range f {
		rs, ok := s[id]
		if !ok {
			t.Fatalf("cluster_id %s present in first replay but not second — not deterministic", id)
		}
		if rf.AnchorAddress != rs.AnchorAddress || rf.Size != rs.Size {
			t.Fatalf("cluster %s differs across replays: %+v vs %+v", id, rf, rs)
		}
	}
}

func TestReplay_ABCMergeIntoOneCluster(t *testing.T) {
	evidence := []domain.MergeEvidence{
		ev(1, "A", "B", "common-input", 0.95, domain.EvidenceStatusActive),
		ev(2, "A", "C", "common-input", 0.95, domain.EvidenceStatusActive),
	}

	results := Replay(evidence)
	if len(results) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(results))
	}

	members := memberSet(results[0])
	for _, addr := range []string{"A", "B", "C"} {
		if !members[addr] {
			t.Fatalf("expected %s to be a member, got %v", addr, members)
		}
	}

	// A appeared first (op_id=1), so it is the canonical anchor (docs/02 §6).
	if results[0].AnchorAddress != "A" {
		t.Fatalf("expected anchor A, got %s", results[0].AnchorAddress)
	}
	if results[0].Members[indexOf(results[0].Members, "A")].Confidence != 1.0 {
		t.Fatalf("anchor's own confidence should be 1.0")
	}
}

func TestReplay_InvalidatedEvidenceIsIgnored(t *testing.T) {
	evidence := []domain.MergeEvidence{
		ev(1, "A", "B", "common-input", 0.95, domain.EvidenceStatusInvalidated),
	}

	results := Replay(evidence)
	if len(results) != 0 {
		t.Fatalf("expected invalidated-only evidence to produce no clusters, got %d", len(results))
	}
}

func TestReplay_ReorgInvalidationOnlyAffectsThatEdge(t *testing.T) {
	// Simulates docs/03 §9 onReorg: one op is invalidated, the rest survive.
	before := []domain.MergeEvidence{
		ev(1, "A", "B", "common-input", 0.95, domain.EvidenceStatusActive),
		ev(2, "B", "C", "common-input", 0.95, domain.EvidenceStatusActive),
	}
	afterRollback := []domain.MergeEvidence{
		ev(1, "A", "B", "common-input", 0.95, domain.EvidenceStatusInvalidated),
		ev(2, "B", "C", "common-input", 0.95, domain.EvidenceStatusActive),
	}

	beforeResults := Replay(before)
	if len(beforeResults) != 1 || beforeResults[0].Size != 3 {
		t.Fatalf("expected A,B,C in one cluster before rollback, got %+v", beforeResults)
	}

	afterResults := Replay(afterRollback)
	if len(afterResults) != 1 || afterResults[0].Size != 2 {
		t.Fatalf("expected only B,C left after rollback, got %+v", afterResults)
	}
	members := memberSet(afterResults[0])
	if members["A"] {
		t.Fatalf("A should have been dropped after its only evidence was invalidated")
	}
	if !members["B"] || !members["C"] {
		t.Fatalf("B and C should remain clustered, got %v", members)
	}
}

func TestReplay_SelfLoopIgnored(t *testing.T) {
	evidence := []domain.MergeEvidence{
		ev(1, "A", "A", "manual", 1.0, domain.EvidenceStatusActive),
	}
	results := Replay(evidence)
	if len(results) != 0 {
		t.Fatalf("expected self-loop evidence to produce no clusters, got %d", len(results))
	}
}

func TestReplay_RedundantEdgeDoesNotChangeMembership(t *testing.T) {
	evidence := []domain.MergeEvidence{
		ev(1, "A", "B", "common-input", 0.95, domain.EvidenceStatusActive),
		ev(2, "A", "B", "manual", 1.0, domain.EvidenceStatusActive), // same pair again
	}
	results := Replay(evidence)
	if len(results) != 1 || results[0].Size != 2 {
		t.Fatalf("expected a single 2-member cluster, got %+v", results)
	}
}

func indexOf(members []Member, addr string) int {
	for i, m := range members {
		if m.Address == addr {
			return i
		}
	}
	return -1
}

func TestReplay_SortedClusterIDsAreStableAcrossRuns(t *testing.T) {
	evidence := []domain.MergeEvidence{
		ev(1, "A", "B", "common-input", 0.95, domain.EvidenceStatusActive),
		ev(2, "D", "E", "common-input", 0.95, domain.EvidenceStatusActive),
	}

	idsOf := func(results []Result) []string {
		ids := make([]string, len(results))
		for i, r := range results {
			ids[i] = r.ClusterID
		}
		sort.Strings(ids)
		return ids
	}

	first := idsOf(Replay(evidence))
	second := idsOf(Replay(evidence))
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("expected 2 clusters each run")
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("cluster ids not stable across runs: %v vs %v", first, second)
		}
	}
}

func evTx(opID int64, a, b, heuristic string, confidence float64, txid string) domain.MergeEvidence {
	e := ev(opID, a, b, heuristic, confidence, domain.EvidenceStatusActive)
	e.SourceTxID = &txid
	return e
}

// docs/03 §7: independent evidence for the same pair (different source
// transactions) combines via noisy-OR, raising confidence above either
// individual source.
func TestReplay_IndependentEvidenceCombinesViaNoisyOR(t *testing.T) {
	evidence := []domain.MergeEvidence{
		evTx(1, "A", "B", "common-input", 0.5, "tx1"),
		evTx(2, "A", "B", "change", 0.5, "tx2"), // different tx, different heuristic — independent
	}

	results := Replay(evidence)
	if len(results) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(results))
	}
	b := results[0].Members[indexOf(results[0].Members, "B")]
	want := 1 - (1-0.5)*(1-0.5) // 0.75
	if diff := b.Confidence - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected noisy-OR combined confidence %.4f, got %v", want, b.Confidence)
	}
}

// docs/03 §7: evidence rows sharing the same source_txid are the same
// observation, not independent — they must NOT compound via noisy-OR.
func TestReplay_SameTxDuplicateEvidenceTakesMaxNotNoisyOR(t *testing.T) {
	evidence := []domain.MergeEvidence{
		evTx(1, "A", "B", "common-input", 0.6, "tx1"),
		evTx(2, "A", "B", "common-input", 0.9, "tx1"), // same tx re-observed (e.g. reprocessing) — take max, don't multiply
	}

	results := Replay(evidence)
	if len(results) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(results))
	}
	b := results[0].Members[indexOf(results[0].Members, "B")]
	if b.Confidence != 0.9 {
		t.Fatalf("expected max(0.6,0.9)=0.9 for same-tx duplicate evidence, got %v", b.Confidence)
	}
}

// A three-source combination: two independent (different tx) sources
// noisy-OR together; the manual assertion (no source_txid, itself unique
// per op_id) is a third independent source.
func TestReplay_ThreeIndependentSourcesCombine(t *testing.T) {
	evidence := []domain.MergeEvidence{
		evTx(1, "A", "B", "common-input", 0.5, "tx1"),
		evTx(2, "A", "B", "sweep-seed", 0.4, "tx2"),
		ev(3, "A", "B", "manual", 0.3, domain.EvidenceStatusActive), // no source_txid
	}

	results := Replay(evidence)
	b := results[0].Members[indexOf(results[0].Members, "B")]
	want := 1 - (1-0.5)*(1-0.4)*(1-0.3) // 0.79
	if diff := b.Confidence - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected combined confidence %.4f, got %v", want, b.Confidence)
	}
}
