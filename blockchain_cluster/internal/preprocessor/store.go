package preprocessor

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/ingestor"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// IsExcluded reports whether a transaction has been marked excluded
// (coinjoin/hub-touch/dust-only — docs/02-data-model.md §8.1).
func (s *Store) IsExcluded(ctx context.Context, chainID, txid string) (bool, error) {
	var reason string
	err := s.pool.QueryRow(ctx, `
		SELECT reason FROM clustering.excluded_tx WHERE chain_id = $1 AND txid = $2
	`, chainID, txid).Scan(&reason)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("preprocessor: is_excluded: %w", err)
	}
	return true, nil
}

// Exclude records a transaction as excluded from merge heuristics. The
// detectors that call this (markCollaborativeTx, markDust) are M3.
func (s *Store) Exclude(ctx context.Context, chainID, txid, reason string, detectorConfidence float64, signal *string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO clustering.excluded_tx (chain_id, txid, reason, detector_confidence, signal)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (chain_id, txid) DO UPDATE
		SET reason = EXCLUDED.reason, detector_confidence = EXCLUDED.detector_confidence, signal = EXCLUDED.signal
	`, chainID, txid, reason, detectorConfidence, signal)
	if err != nil {
		return fmt.Errorf("preprocessor: exclude: %w", err)
	}
	return nil
}

// MarkCollaborativeTx runs DetectCollaborativeTx and records each hit.
func (s *Store) MarkCollaborativeTx(ctx context.Context, deltas []domain.BalanceDelta, params domain.PreprocessingParams) error {
	for _, det := range DetectCollaborativeTx(deltas, params) {
		if err := s.Exclude(ctx, det.ChainID, det.TxID, "coinjoin", det.Confidence, nil); err != nil {
			return fmt.Errorf("preprocessor: mark_collaborative_tx(%s): %w", det.TxID, err)
		}
	}
	return nil
}

// AddressDustStore is the address registry access markDust needs: flag new
// dust inflows, then read those flags back to decide tx exclusion. Whether
// "all inputs are dust-tainted" can depend on flags set in earlier batches
// (dust_flag is persisted, not batch-scoped), so this step cannot be pure.
type AddressDustStore interface {
	SetDustFlag(ctx context.Context, chainID, address string) error
	Get(ctx context.Context, chainID, address string) (domain.Address, bool, error)
}

// MarkDust implements markDust (docs/03 §3): flag every dust inflow in this
// batch, then exclude any transaction whose spending addresses are *all*
// dust-tainted (the only case allInputsAreDustTainted matters for — a
// single non-tainted spender is enough real signal to keep the tx eligible
// for commonInputHeuristic).
func (s *Store) MarkDust(ctx context.Context, deltas []domain.BalanceDelta, params domain.PreprocessingParams, addrs AddressDustStore) error {
	for _, ref := range DustInflows(deltas, params.DustThreshold) {
		if err := addrs.SetDustFlag(ctx, ref.ChainID, ref.Address); err != nil {
			return fmt.Errorf("preprocessor: mark_dust: set_dust_flag(%s): %w", ref.Address, err)
		}
	}

	for _, tx := range ingestor.GroupByTx(deltas) {
		ins := ingestor.SpentAddresses(tx.Deltas)
		if len(ins) == 0 {
			continue
		}
		allTainted := true
		for _, addr := range ins {
			a, found, err := addrs.Get(ctx, tx.Key.ChainID, addr)
			if err != nil {
				return fmt.Errorf("preprocessor: mark_dust: get(%s): %w", addr, err)
			}
			if !found || !a.DustFlag {
				allTainted = false
				break
			}
		}
		if allTainted {
			if err := s.Exclude(ctx, tx.Key.ChainID, tx.Key.TxID, "dust-only", params.DustExclusionConfidence, nil); err != nil {
				return fmt.Errorf("preprocessor: mark_dust: exclude(%s): %w", tx.Key.TxID, err)
			}
		}
	}
	return nil
}
