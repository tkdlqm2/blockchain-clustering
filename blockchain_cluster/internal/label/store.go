// Package label implements LabelStore (docs/02-data-model.md §7): CRUD
// (this file) plus labelMaintenance — freshness decay and conflict
// detection (docs/03 §8, maintain.go).
package label

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// AddLabel inserts a new label claim. Conflicting labels on the same target
// are not resolved here — that is labelMaintenance's job (M6, FR-21).
func (s *Store) AddLabel(ctx context.Context, l domain.Label) (int64, error) {
	if l.TargetType != "cluster" && l.TargetType != "address" {
		return 0, fmt.Errorf("label: invalid target_type %q", l.TargetType)
	}

	status := l.Status
	if status == "" {
		status = "active"
	}

	var labelID int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO clustering.label
			(target_type, chain_id, target_cluster_id, target_address,
			 label, category, source, source_confidence, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING label_id
	`, l.TargetType, l.ChainID, l.TargetClusterID, l.TargetAddress,
		l.Label, l.Category, l.Source, l.SourceConfidence, status,
	).Scan(&labelID)
	if err != nil {
		return 0, fmt.Errorf("label: add: %w", err)
	}
	return labelID, nil
}

// LabelsOf returns labels for a cluster or an address.
func (s *Store) LabelsOf(ctx context.Context, chainID, targetType, targetID string) ([]domain.Label, error) {
	var column string
	switch targetType {
	case "cluster":
		column = "target_cluster_id"
	case "address":
		column = "target_address"
	default:
		return nil, fmt.Errorf("label: invalid target_type %q", targetType)
	}

	// column is interpolated, not targetType itself — the switch above
	// restricts it to one of two fixed literals, so this stays injection-safe.
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT label_id, target_type, chain_id, target_cluster_id, target_address,
		       label, category, source, source_confidence, collected_at, last_verified_at, status
		FROM clustering.label
		WHERE chain_id = $1 AND %s = $2
	`, column), chainID, targetID)
	if err != nil {
		return nil, fmt.Errorf("label: labels_of: %w", err)
	}
	defer rows.Close()

	var out []domain.Label
	for rows.Next() {
		var l domain.Label
		if err := rows.Scan(
			&l.LabelID, &l.TargetType, &l.ChainID, &l.TargetClusterID, &l.TargetAddress,
			&l.Label, &l.Category, &l.Source, &l.SourceConfidence, &l.CollectedAt, &l.LastVerifiedAt, &l.Status,
		); err != nil {
			return nil, fmt.Errorf("label: labels_of: scan: %w", err)
		}
		out = append(out, l)
	}
	return out, rows.Err()
}
