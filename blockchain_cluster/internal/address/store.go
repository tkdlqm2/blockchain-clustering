// Package address implements the address registry (docs/02-data-model.md §2).
// It is a skeleton for M0: upsert/read/hub-flag plumbing only. The actual
// hub/dust detection logic (markHubs, markDust — docs/03 §1, §3) is a
// Preprocessor concern from milestone M3.
package address

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

// Upsert registers an address if unseen, or bumps last_seen_height if it
// already exists. Idempotent (FR-3).
func (s *Store) Upsert(ctx context.Context, chainID, addr string, blockHeight int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO clustering.address (chain_id, address, first_seen_height, last_seen_height)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (chain_id, address) DO UPDATE
		SET last_seen_height = GREATEST(clustering.address.last_seen_height, EXCLUDED.last_seen_height)
	`, chainID, addr, blockHeight)
	if err != nil {
		return fmt.Errorf("address: upsert: %w", err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, chainID, addr string) (domain.Address, bool, error) {
	var a domain.Address
	err := s.pool.QueryRow(ctx, `
		SELECT chain_id, address, first_seen_height, last_seen_height, is_hub, hub_type, hub_confidence, dust_flag
		FROM clustering.address
		WHERE chain_id = $1 AND address = $2
	`, chainID, addr).Scan(
		&a.ChainID, &a.Address, &a.FirstSeenHeight, &a.LastSeenHeight,
		&a.IsHub, &a.HubType, &a.HubConfidence, &a.DustFlag,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.Address{}, false, nil
		}
		return domain.Address{}, false, fmt.Errorf("address: get: %w", err)
	}
	return a, true, nil
}

// SetHub records a hub determination (docs/03 §1 markHubs output).
func (s *Store) SetHub(ctx context.Context, chainID, addr, hubType string, confidence float64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE clustering.address
		SET is_hub = TRUE, hub_type = $3, hub_confidence = $4
		WHERE chain_id = $1 AND address = $2
	`, chainID, addr, hubType, confidence)
	if err != nil {
		return fmt.Errorf("address: set_hub: %w", err)
	}
	return nil
}

// SetDustFlag marks an address as a dust-inflow target (docs/03 §3 markDust output).
func (s *Store) SetDustFlag(ctx context.Context, chainID, addr string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE clustering.address SET dust_flag = TRUE
		WHERE chain_id = $1 AND address = $2
	`, chainID, addr)
	if err != nil {
		return fmt.Errorf("address: set_dust_flag: %w", err)
	}
	return nil
}

// IsHub is the read path recordAndMerge uses to block merges through a hub
// (docs/03 §6).
func (s *Store) IsHub(ctx context.Context, chainID, addr string) (bool, error) {
	a, found, err := s.Get(ctx, chainID, addr)
	if err != nil || !found {
		return false, err
	}
	return a.IsHub, nil
}
