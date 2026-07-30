package reorg

import (
	"context"
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

type fakeEvidence struct {
	rows            []domain.MergeEvidence
	invalidated     map[int64]string // op_id -> reason
	invalidateCalls int
}

func newFakeEvidence(rows ...domain.MergeEvidence) *fakeEvidence {
	return &fakeEvidence{rows: rows, invalidated: map[int64]string{}}
}

func (f *fakeEvidence) ByBlockHash(_ context.Context, _ string, blockHashes []string) ([]domain.MergeEvidence, error) {
	want := map[string]bool{}
	for _, h := range blockHashes {
		want[h] = true
	}
	var out []domain.MergeEvidence
	for _, e := range f.rows {
		if e.SourceBlockHash != nil && want[*e.SourceBlockHash] {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeEvidence) Invalidate(_ context.Context, _ string, opID int64, reason string) (bool, error) {
	f.invalidateCalls++
	for i, e := range f.rows {
		if e.OpID == opID {
			if f.rows[i].Status != domain.EvidenceStatusActive {
				return false, nil
			}
			f.rows[i].Status = domain.EvidenceStatusInvalidated
			f.invalidated[opID] = reason
			return true, nil
		}
	}
	return false, nil
}

type fakeCluster struct{ rebuildCalls int }

func (f *fakeCluster) RebuildFromEvidence(_ context.Context, _ string) error {
	f.rebuildCalls++
	return nil
}

type fakeAudit struct{ entries []string }

func (f *fakeAudit) Log(_ context.Context, actor, action string, _ any, rationale string) error {
	f.entries = append(f.entries, actor+":"+action+":"+rationale)
	return nil
}

func TestOnReorg_InvalidatesOnlyMatchingActiveEvidence(t *testing.T) {
	b1, b2 := "block1", "block2"
	ev := newFakeEvidence(
		domain.MergeEvidence{OpID: 1, SourceBlockHash: &b1, Status: domain.EvidenceStatusActive},
		domain.MergeEvidence{OpID: 2, SourceBlockHash: &b2, Status: domain.EvidenceStatusActive},      // different block, must survive
		domain.MergeEvidence{OpID: 3, SourceBlockHash: &b1, Status: domain.EvidenceStatusInvalidated}, // already gone, skip
	)
	cl := &fakeCluster{}
	au := &fakeAudit{}
	h := NewHandler(ev, cl, au)

	n, err := h.OnReorg(context.Background(), "bitcoin", []string{"block1"})
	if err != nil {
		t.Fatalf("on_reorg: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 invalidation, got %d", n)
	}
	if ev.rows[0].Status != domain.EvidenceStatusInvalidated {
		t.Fatalf("expected op 1 invalidated")
	}
	if ev.rows[1].Status != domain.EvidenceStatusActive {
		t.Fatalf("expected op 2 (different block) to survive")
	}
	if cl.rebuildCalls != 1 {
		t.Fatalf("expected exactly 1 rebuild call, got %d", cl.rebuildCalls)
	}
	if len(au.entries) != 1 {
		t.Fatalf("expected 1 audit entry, got %d: %v", len(au.entries), au.entries)
	}
}

func TestOnReorg_NoMatchesSkipsRebuild(t *testing.T) {
	b1 := "block1"
	ev := newFakeEvidence(domain.MergeEvidence{OpID: 1, SourceBlockHash: &b1, Status: domain.EvidenceStatusActive})
	cl := &fakeCluster{}
	h := NewHandler(ev, cl, nil)

	n, err := h.OnReorg(context.Background(), "bitcoin", []string{"some-other-block"})
	if err != nil {
		t.Fatalf("on_reorg: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 invalidations, got %d", n)
	}
	if cl.rebuildCalls != 0 {
		t.Fatalf("expected rebuild to be skipped when nothing changed, got %d calls", cl.rebuildCalls)
	}
}

func TestOnReorg_EmptyBlockListIsNoop(t *testing.T) {
	ev := newFakeEvidence()
	cl := &fakeCluster{}
	h := NewHandler(ev, cl, nil)

	n, err := h.OnReorg(context.Background(), "bitcoin", nil)
	if err != nil || n != 0 || cl.rebuildCalls != 0 {
		t.Fatalf("expected no-op for empty block list, got n=%d rebuilds=%d err=%v", n, cl.rebuildCalls, err)
	}
}

func TestOnManualCorrection_InvalidatesAndRebuilds(t *testing.T) {
	ev := newFakeEvidence(domain.MergeEvidence{OpID: 42, Status: domain.EvidenceStatusActive})
	cl := &fakeCluster{}
	au := &fakeAudit{}
	h := NewHandler(ev, cl, au)

	changed, err := h.OnManualCorrection(context.Background(), "bitcoin", 42, "false positive: unrelated exchange users")
	if err != nil {
		t.Fatalf("on_manual_correction: %v", err)
	}
	if !changed {
		t.Fatalf("expected the correction to report a change")
	}
	if ev.invalidated[42] != "manual-correction" {
		t.Fatalf("expected op 42 invalidated with reason manual-correction, got %q", ev.invalidated[42])
	}
	if cl.rebuildCalls != 1 {
		t.Fatalf("expected 1 rebuild, got %d", cl.rebuildCalls)
	}
	if len(au.entries) != 1 {
		t.Fatalf("expected 1 audit entry, got %v", au.entries)
	}
}

func TestOnManualCorrection_AlreadyInvalidatedIsNoopNotError(t *testing.T) {
	ev := newFakeEvidence(domain.MergeEvidence{OpID: 42, Status: domain.EvidenceStatusInvalidated})
	cl := &fakeCluster{}
	h := NewHandler(ev, cl, nil)

	changed, err := h.OnManualCorrection(context.Background(), "bitcoin", 42, "retry")
	if err != nil {
		t.Fatalf("expected no error for an idempotent retry, got %v", err)
	}
	if changed {
		t.Fatalf("expected changed=false for an already-invalidated op")
	}
	if cl.rebuildCalls != 0 {
		t.Fatalf("expected rebuild to be skipped, got %d calls", cl.rebuildCalls)
	}
}

func TestOnManualCorrection_UnknownOpIsNoopNotError(t *testing.T) {
	ev := newFakeEvidence()
	cl := &fakeCluster{}
	h := NewHandler(ev, cl, nil)

	changed, err := h.OnManualCorrection(context.Background(), "bitcoin", 9999, "typo'd op_id")
	if err != nil || changed {
		t.Fatalf("expected changed=false, err=nil for an unknown op_id, got changed=%v err=%v", changed, err)
	}
}
