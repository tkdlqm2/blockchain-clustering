package merge

import (
	"context"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

type fakeEvidenceStore struct {
	appended []domain.MergeEvidence
	nextOp   int64
}

func (f *fakeEvidenceStore) Append(_ context.Context, e domain.MergeEvidence) (int64, error) {
	f.nextOp++
	e.OpID = f.nextOp
	f.appended = append(f.appended, e)
	return f.nextOp, nil
}

type fakeHubChecker map[string]bool

func (f fakeHubChecker) IsHub(_ context.Context, _, address string) (bool, error) {
	return f[address], nil
}

func candidate(a, b string) domain.MergeCandidate {
	return domain.MergeCandidate{ChainID: "bitcoin", AddressA: a, AddressB: b, HeuristicKey: "common-input", Confidence: 0.95}
}

func TestRecordAndMerge_Records(t *testing.T) {
	store := &fakeEvidenceStore{}
	e := NewEngine(store, fakeHubChecker{})

	opID, rejected, err := e.RecordAndMerge(context.Background(), candidate("A", "B"))
	if err != nil {
		t.Fatalf("record_and_merge: %v", err)
	}
	if rejected {
		t.Fatalf("expected candidate to be recorded, not rejected")
	}
	if opID != 1 {
		t.Fatalf("expected op_id 1, got %d", opID)
	}
	if len(store.appended) != 1 {
		t.Fatalf("expected 1 evidence row appended, got %d", len(store.appended))
	}
}

func TestRecordAndMerge_SelfLoopRejected(t *testing.T) {
	store := &fakeEvidenceStore{}
	e := NewEngine(store, fakeHubChecker{})

	_, rejected, err := e.RecordAndMerge(context.Background(), candidate("A", "A"))
	if err != nil {
		t.Fatalf("record_and_merge: %v", err)
	}
	if !rejected {
		t.Fatalf("expected self-loop to be rejected")
	}
	if len(store.appended) != 0 {
		t.Fatalf("expected no evidence appended for a self-loop, got %d", len(store.appended))
	}
}

func TestRecordAndMerge_HubBlocksEvenIfCandidateSlippedThrough(t *testing.T) {
	// Defense in depth: even if the heuristic that emitted this candidate
	// already filtered hubs, RecordAndMerge re-checks (docs/03 §6).
	store := &fakeEvidenceStore{}
	e := NewEngine(store, fakeHubChecker{"B": true})

	_, rejected, err := e.RecordAndMerge(context.Background(), candidate("A", "B"))
	if err != nil {
		t.Fatalf("record_and_merge: %v", err)
	}
	if !rejected {
		t.Fatalf("expected hub-touching candidate to be rejected")
	}
	if len(store.appended) != 0 {
		t.Fatalf("expected no evidence appended when a hub is involved, got %d", len(store.appended))
	}
}

func TestRecordAndMergeBatch_TalliesRecordedAndRejected(t *testing.T) {
	store := &fakeEvidenceStore{}
	e := NewEngine(store, fakeHubChecker{"C": true})

	result, err := e.RecordAndMergeBatch(context.Background(), []domain.MergeCandidate{
		candidate("A", "B"), // recorded
		candidate("A", "C"), // rejected (hub)
		candidate("D", "D"), // rejected (self-loop)
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if result.Recorded != 1 || result.Rejected != 2 {
		t.Fatalf("expected 1 recorded / 2 rejected, got %+v", result)
	}
}
