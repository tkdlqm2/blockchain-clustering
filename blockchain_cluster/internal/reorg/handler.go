// Package reorg implements ReorgHandler (docs/04-architecture §2 [8]):
// invalidate the merge_evidence rows a rollback or a manual correction
// affects, then replay. Both paths are the same two steps (docs/03 §9) —
// this is why one Handler serves both onReorg and onManualCorrection.
package reorg

import (
	"context"
	"fmt"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// EvidenceInvalidator is the slice of EvidenceStore's API this package
// needs, narrowed to an interface for DB-free unit testing.
type EvidenceInvalidator interface {
	ByBlockHash(ctx context.Context, chainID string, blockHashes []string) ([]domain.MergeEvidence, error)
	Invalidate(ctx context.Context, chainID string, opID int64, reason string) (bool, error)
}

// ClusterRebuilder is ClusterStore's replay entrypoint.
type ClusterRebuilder interface {
	RebuildFromEvidence(ctx context.Context, chainID string) error
}

// AuditLogger is audit.Store's write path (FR-26).
type AuditLogger interface {
	Log(ctx context.Context, actor, action string, target any, rationale string) error
}

type Handler struct {
	evidence EvidenceInvalidator
	cluster  ClusterRebuilder
	audit    AuditLogger
}

func NewHandler(evidence EvidenceInvalidator, cluster ClusterRebuilder, audit AuditLogger) *Handler {
	return &Handler{evidence: evidence, cluster: cluster, audit: audit}
}

// OnReorg implements docs/03 §9's onReorg: invalidate every active merge
// whose evidence was anchored to one of the rolled-back blocks, then
// replay. Only merges anchored to those specific blocks are touched —
// everything else survives untouched, which is the whole point of keeping
// merge_evidence as the source of truth (docs/02 §3 invariant 3, AC-2).
func (h *Handler) OnReorg(ctx context.Context, chainID string, rolledBackBlockHashes []string) (invalidatedCount int, err error) {
	if len(rolledBackBlockHashes) == 0 {
		return 0, nil
	}

	candidates, err := h.evidence.ByBlockHash(ctx, chainID, rolledBackBlockHashes)
	if err != nil {
		return 0, fmt.Errorf("reorg: on_reorg: by_block_hash: %w", err)
	}

	for _, e := range candidates {
		if e.Status != domain.EvidenceStatusActive {
			continue // already invalidated by an earlier reorg/correction — nothing to do
		}
		invalidated, err := h.evidence.Invalidate(ctx, chainID, e.OpID, "reorg")
		if err != nil {
			return invalidatedCount, fmt.Errorf("reorg: on_reorg: invalidate(op_id=%d): %w", e.OpID, err)
		}
		if !invalidated {
			continue
		}
		invalidatedCount++
		if h.audit != nil {
			if err := h.audit.Log(ctx, "system", "invalidate", reorgTarget{ChainID: chainID, OpID: e.OpID, BlockHash: derefOrEmpty(e.SourceBlockHash)}, "reorg rollback"); err != nil {
				return invalidatedCount, fmt.Errorf("reorg: on_reorg: audit log: %w", err)
			}
		}
	}

	if invalidatedCount == 0 {
		return 0, nil // nothing changed — skip the full replay
	}
	if err := h.cluster.RebuildFromEvidence(ctx, chainID); err != nil {
		return invalidatedCount, fmt.Errorf("reorg: on_reorg: rebuild: %w", err)
	}
	return invalidatedCount, nil
}

// OnManualCorrection implements docs/03 §9's onManualCorrection (FR-24):
// an operator invalidating a single op they've determined is a false
// positive. Idempotent — invalidating an already-invalidated or
// nonexistent op is a no-op, not an error (matches Invalidate's contract).
func (h *Handler) OnManualCorrection(ctx context.Context, chainID string, opID int64, rationale string) (invalidated bool, err error) {
	invalidated, err = h.evidence.Invalidate(ctx, chainID, opID, "manual-correction")
	if err != nil {
		return false, fmt.Errorf("reorg: on_manual_correction: invalidate(op_id=%d): %w", opID, err)
	}
	if !invalidated {
		return false, nil
	}

	if h.audit != nil {
		if err := h.audit.Log(ctx, "operator", "invalidate", reorgTarget{ChainID: chainID, OpID: opID}, rationale); err != nil {
			return true, fmt.Errorf("reorg: on_manual_correction: audit log: %w", err)
		}
	}
	if err := h.cluster.RebuildFromEvidence(ctx, chainID); err != nil {
		return true, fmt.Errorf("reorg: on_manual_correction: rebuild: %w", err)
	}
	return true, nil
}

type reorgTarget struct {
	ChainID   string `json:"chain_id"`
	OpID      int64  `json:"op_id"`
	BlockHash string `json:"source_block_hash,omitempty"`
}

func derefOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
