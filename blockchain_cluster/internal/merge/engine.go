// Package merge implements MergeEngine (docs/04-architecture §2 [4]):
// turns a HeuristicEngine's MergeCandidate into MergeEvidence, after
// re-checking hub exclusion.
//
// docs/04 §2 [4] also lists "Union-Find union" and "membership confidence
// update" as MergeEngine responsibilities. This codebase deliberately does
// not give MergeEngine its own incremental Union-Find: M0 already decided
// that ClusterStore.RebuildFromEvidence() always fully replays
// merge_evidence (docs/03 §9: "정확성 기준 구현은 전체 재생"), and
// maintaining a second, incremental evidence-to-membership algorithm here
// risks it drifting from what replay actually computes. So MergeEngine's
// job stops at the evidence log; callers materialize the derived cache by
// calling ClusterStore.RebuildFromEvidence after a batch.
package merge

import (
	"context"
	"fmt"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

type HubChecker interface {
	IsHub(ctx context.Context, chainID, address string) (bool, error)
}

// EvidenceAppender is the slice of evidence.Store's API MergeEngine needs —
// narrowed to an interface so RecordAndMerge is unit-testable without a
// live database (evidence.Store satisfies this as-is, no changes needed).
type EvidenceAppender interface {
	Append(ctx context.Context, e domain.MergeEvidence) (int64, error)
}

type Engine struct {
	evidence EvidenceAppender
	hubs     HubChecker
}

func NewEngine(evidenceStore EvidenceAppender, hubs HubChecker) *Engine {
	return &Engine{evidence: evidenceStore, hubs: hubs}
}

// RecordAndMerge implements recordAndMerge (docs/03 §6): self-merges are a
// no-op, and a hub on either side blocks the merge even if the heuristic
// that emitted this candidate already checked (defense in depth — the hub
// flag may have changed between candidate generation and recording).
// rejected=true means "not an error, just not recorded" (self-loop or hub).
func (e *Engine) RecordAndMerge(ctx context.Context, candidate domain.MergeCandidate) (opID int64, rejected bool, err error) {
	if candidate.AddressA == candidate.AddressB {
		return 0, true, nil
	}

	hubA, err := e.hubs.IsHub(ctx, candidate.ChainID, candidate.AddressA)
	if err != nil {
		return 0, false, fmt.Errorf("merge: is_hub(%s): %w", candidate.AddressA, err)
	}
	hubB, err := e.hubs.IsHub(ctx, candidate.ChainID, candidate.AddressB)
	if err != nil {
		return 0, false, fmt.Errorf("merge: is_hub(%s): %w", candidate.AddressB, err)
	}
	if hubA || hubB {
		return 0, true, nil
	}

	opID, err = e.evidence.Append(ctx, domain.MergeEvidence{
		ChainID:           candidate.ChainID,
		AddressA:          candidate.AddressA,
		AddressB:          candidate.AddressB,
		HeuristicKey:      candidate.HeuristicKey,
		SourceTxID:        candidate.SourceTxID,
		SourceBlockHash:   candidate.SourceBlockHash,
		SourceBlockHeight: candidate.SourceBlockHeight,
		Confidence:        candidate.Confidence,
	})
	if err != nil {
		return 0, false, fmt.Errorf("merge: record_and_merge: %w", err)
	}
	return opID, false, nil
}

// BatchResult tallies a batch of RecordAndMerge calls.
type BatchResult struct {
	Recorded int
	Rejected int
}

// RecordAndMergeBatch records every candidate in order — the append order
// determines op_id order, which determines the canonical cluster anchor on
// replay (docs/02 §6), so candidates must already be in a deterministic
// order (HeuristicEngine.Generate guarantees this).
func (e *Engine) RecordAndMergeBatch(ctx context.Context, candidates []domain.MergeCandidate) (BatchResult, error) {
	var result BatchResult
	for _, c := range candidates {
		_, rejected, err := e.RecordAndMerge(ctx, c)
		if err != nil {
			return result, err
		}
		if rejected {
			result.Rejected++
		} else {
			result.Recorded++
		}
	}
	return result, nil
}
