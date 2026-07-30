// Package sweeptarget stores sweep_target rows (docs/02-data-model.md
// §8.2): known collection destinations that anchor sweepHeuristic
// (docs/03 §5) and expandFromSeeds. Operators seed these from known-deposit
// experiments (FR-25); this package is just the CRUD, not the seeding
// experiment itself.
package sweeptarget

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Add registers (or re-registers) a sweep target anchor.
func (s *Store) Add(ctx context.Context, target domain.SweepTarget) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO clustering.sweep_target (chain_id, address, entity_hint, source, confidence)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (chain_id, address) DO UPDATE
		SET entity_hint = EXCLUDED.entity_hint, source = EXCLUDED.source, confidence = EXCLUDED.confidence
	`, target.ChainID, target.Address, target.EntityHint, target.Source, target.Confidence)
	if err != nil {
		return fmt.Errorf("sweeptarget: add: %w", err)
	}
	return nil
}

// Get reports whether address is a known sweep target, and its anchor data
// if so — this is sweepHeuristic's isKnownSweepTarget(dst) check (docs/03 §5).
func (s *Store) Get(ctx context.Context, chainID, address string) (domain.SweepTarget, bool, error) {
	t := domain.SweepTarget{ChainID: chainID, Address: address}
	err := s.pool.QueryRow(ctx, `
		SELECT entity_hint, source, confidence
		FROM clustering.sweep_target
		WHERE chain_id = $1 AND address = $2
	`, chainID, address).Scan(&t.EntityHint, &t.Source, &t.Confidence)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.SweepTarget{}, false, nil
		}
		return domain.SweepTarget{}, false, fmt.Errorf("sweeptarget: get: %w", err)
	}
	return t, true, nil
}

// ListAll returns every registered target for a chain — expandFromSeeds
// (docs/03 §5) walks this list to re-scan for newly-observed deposit
// addresses converging on each one.
func (s *Store) ListAll(ctx context.Context, chainID string) ([]domain.SweepTarget, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT address, entity_hint, source, confidence
		FROM clustering.sweep_target WHERE chain_id = $1
	`, chainID)
	if err != nil {
		return nil, fmt.Errorf("sweeptarget: list_all: %w", err)
	}
	defer rows.Close()

	var out []domain.SweepTarget
	for rows.Next() {
		t := domain.SweepTarget{ChainID: chainID}
		if err := rows.Scan(&t.Address, &t.EntityHint, &t.Source, &t.Confidence); err != nil {
			return nil, fmt.Errorf("sweeptarget: list_all: scan: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
