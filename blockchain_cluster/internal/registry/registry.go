// Package registry reads the chain/heuristic/chain_heuristic tables
// (docs/06-multichain-extensibility.md §2.2) — the mechanism that lets
// per-heuristic confidence and enablement be configured per chain instead
// of hardcoded (docs/03-clustering-algorithms.md §10: "모든 파라미터는
// 설정 가능해야 하며 하드코딩 금지").
package registry

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

// ConfigFor resolves add_chain()'s default mapping plus any per-chain
// override (chain_heuristic.confidence_override falls back to
// heuristic.default_confidence). found=false means this chain was never
// registered for this heuristic (e.g. chain not added yet, or the
// heuristic doesn't apply to this chain's model_type).
func (s *Store) ConfigFor(ctx context.Context, chainID, heuristicKey string) (cfg domain.HeuristicConfig, found bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT ch.enabled,
		       COALESCE(ch.confidence_override, h.default_confidence),
		       ch.params
		FROM clustering.chain_heuristic ch
		JOIN clustering.heuristic h ON h.heuristic_key = ch.heuristic_key
		WHERE ch.chain_id = $1 AND ch.heuristic_key = $2
	`, chainID, heuristicKey).Scan(&cfg.Enabled, &cfg.Confidence, &cfg.Params)
	if err != nil {
		if err == pgx.ErrNoRows {
			return domain.HeuristicConfig{}, false, nil
		}
		return domain.HeuristicConfig{}, false, fmt.Errorf("registry: config_for: %w", err)
	}
	return cfg, true, nil
}

// ConfidenceFor is the narrow view of ConfigFor that engines needing only
// enablement + confidence depend on (heuristic.ConfidenceProvider) — most
// engines don't need chain_heuristic.params.
func (s *Store) ConfidenceFor(ctx context.Context, chainID, heuristicKey string) (confidence float64, enabled bool, err error) {
	cfg, found, err := s.ConfigFor(ctx, chainID, heuristicKey)
	if err != nil || !found {
		return 0, false, err
	}
	return cfg.Confidence, cfg.Enabled, nil
}

// ListChains returns every enabled chain_id — used wherever a component
// needs to iterate "every chain we operate on" without hardcoding a list
// (the metrics collector and the label-maintenance scheduler, M8).
func (s *Store) ListChains(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT chain_id FROM clustering.chain WHERE enabled = TRUE`)
	if err != nil {
		return nil, fmt.Errorf("registry: list_chains: %w", err)
	}
	defer rows.Close()

	var chains []string
	for rows.Next() {
		var chainID string
		if err := rows.Scan(&chainID); err != nil {
			return nil, fmt.Errorf("registry: list_chains: scan: %w", err)
		}
		chains = append(chains, chainID)
	}
	return chains, rows.Err()
}
