package label

import (
	"context"
	"fmt"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// MaintainResult tallies one labelMaintenance pass (docs/03 §8).
type MaintainResult struct {
	Decayed    int // active labels past staleTTL, decayed and marked 'stale' (FR-20)
	Conflicted int // labels marked 'conflicted' this pass (FR-21)
}

// targetKey identifies what a label is attached to.
type targetKey struct {
	targetType string
	chainID    string
	targetID   string
}

func keyOf(l domain.Label) targetKey {
	targetID := ""
	switch l.TargetType {
	case "cluster":
		if l.TargetClusterID != nil {
			targetID = *l.TargetClusterID
		}
	case "address":
		if l.TargetAddress != nil {
			targetID = *l.TargetAddress
		}
	}
	return targetKey{targetType: l.TargetType, chainID: l.ChainID, targetID: targetID}
}

// DetectConflicts implements docs/03 §8's detectConflicts: labels sharing a
// target are in conflict if they disagree on category — two sources both
// saying "exchange" corroborate each other, but "exchange" vs "mixer" on
// the same cluster is a genuine contradiction that must not auto-resolve
// (FR-21). Only 'active'/'stale' labels are considered — a 'retired' label
// is dead, and an already-'conflicted' one doesn't need re-flagging.
// Pure and DB-free so it's unit-testable on its own.
func DetectConflicts(labels []domain.Label) map[int64]bool {
	byTarget := make(map[targetKey][]domain.Label)
	for _, l := range labels {
		if l.Status != "active" && l.Status != "stale" {
			continue
		}
		key := keyOf(l)
		byTarget[key] = append(byTarget[key], l)
	}

	conflicted := make(map[int64]bool)
	for _, ls := range byTarget {
		categories := make(map[string]bool, len(ls))
		for _, l := range ls {
			categories[l.Category] = true
		}
		if len(categories) <= 1 {
			continue
		}
		for _, l := range ls {
			conflicted[l.LabelID] = true
		}
	}
	return conflicted
}

// Maintain implements labelMaintenance (docs/03 §8, FR-20/FR-21): decay and
// flag labels that haven't been reverified within staleTTL, then detect and
// flag conflicting labels. staleTTL and decayFactor are caller-supplied
// (not hardcoded, docs/03 §10) — there's no chain.config-style registry
// entry for label policy yet, so the caller (a scheduled job, once one
// exists) is expected to source sensible defaults itself.
func (s *Store) Maintain(ctx context.Context, chainID string, staleTTL time.Duration, decayFactor float64) (MaintainResult, error) {
	var result MaintainResult
	now := time.Now()

	rows, err := s.pool.Query(ctx, `
		SELECT label_id, target_type, chain_id, target_cluster_id, target_address,
		       label, category, source, source_confidence, collected_at, last_verified_at, status
		FROM clustering.label
		WHERE chain_id = $1 AND status IN ('active', 'stale')
	`, chainID)
	if err != nil {
		return result, fmt.Errorf("label: maintain: query: %w", err)
	}
	var labels []domain.Label
	for rows.Next() {
		var l domain.Label
		if err := rows.Scan(
			&l.LabelID, &l.TargetType, &l.ChainID, &l.TargetClusterID, &l.TargetAddress,
			&l.Label, &l.Category, &l.Source, &l.SourceConfidence, &l.CollectedAt, &l.LastVerifiedAt, &l.Status,
		); err != nil {
			rows.Close()
			return result, fmt.Errorf("label: maintain: scan: %w", err)
		}
		labels = append(labels, l)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("label: maintain: rows: %w", err)
	}

	for _, l := range labels {
		if l.Status != "active" {
			continue
		}
		if now.Sub(l.LastVerifiedAt) <= staleTTL {
			continue
		}
		decayed := l.SourceConfidence * decayFactor
		if _, err := s.pool.Exec(ctx, `
			UPDATE clustering.label SET status = 'stale', source_confidence = $2 WHERE label_id = $1
		`, l.LabelID, decayed); err != nil {
			return result, fmt.Errorf("label: maintain: decay(label_id=%d): %w", l.LabelID, err)
		}
		result.Decayed++
	}

	for labelID := range DetectConflicts(labels) {
		if _, err := s.pool.Exec(ctx, `
			UPDATE clustering.label SET status = 'conflicted' WHERE label_id = $1 AND status <> 'conflicted'
		`, labelID); err != nil {
			return result, fmt.Errorf("label: maintain: conflict(label_id=%d): %w", labelID, err)
		}
		result.Conflicted++
	}

	return result, nil
}
