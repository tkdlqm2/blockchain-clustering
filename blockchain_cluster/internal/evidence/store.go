// Package evidence implements EvidenceStore — the append-only source of
// truth for merges (docs/02-data-model.md §3; docs/04-architecture §2 [5]).
package evidence

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

// Append records new evidence as active. This is the only way rows enter
// merge_evidence — the table is append-only (docs/02 §3 invariant 1); the
// app DB role has no DELETE grant (migrations/0002_create_app_user.sh).
func (s *Store) Append(ctx context.Context, e domain.MergeEvidence) (int64, error) {
	if e.AddressA == e.AddressB {
		return 0, fmt.Errorf("evidence: address_a and address_b must differ (got %q)", e.AddressA)
	}
	createdBy := e.CreatedBy
	if createdBy == "" {
		createdBy = "system"
	}

	var opID int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO clustering.merge_evidence
			(chain_id, address_a, address_b, heuristic_key,
			 source_txid, source_block_hash, source_block_height,
			 confidence, status, created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'active',$9)
		RETURNING op_id
	`, e.ChainID, e.AddressA, e.AddressB, e.HeuristicKey,
		e.SourceTxID, e.SourceBlockHash, e.SourceBlockHeight,
		e.Confidence, createdBy,
	).Scan(&opID)
	if err != nil {
		return 0, fmt.Errorf("evidence: append: %w", err)
	}
	return opID, nil
}

// Invalidate transitions a record to invalidated — never a physical
// delete/update of the merge fields (docs/02 §3 invariant 1). invalidated
// reports whether this call actually changed anything; it is false (not an
// error) if the op is already invalidated or does not exist — callers
// (ReorgHandler) use this to skip an unnecessary rebuild.
func (s *Store) Invalidate(ctx context.Context, chainID string, opID int64, reason string) (invalidated bool, err error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE clustering.merge_evidence
		SET status = 'invalidated', invalidated_reason = $3
		WHERE chain_id = $1 AND op_id = $2 AND status = 'active'
	`, chainID, opID, reason)
	if err != nil {
		return false, fmt.Errorf("evidence: invalidate: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ScanActive returns active evidence ordered by op_id — the exact input
// contract that cluster.Replay requires for deterministic replay.
func (s *Store) ScanActive(ctx context.Context, chainID string) ([]domain.MergeEvidence, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_id, op_id, address_a, address_b, heuristic_key,
		       source_txid, source_block_hash, source_block_height,
		       confidence, status, invalidated_reason, created_at, created_by
		FROM clustering.merge_evidence
		WHERE chain_id = $1 AND status = 'active'
		ORDER BY op_id ASC
	`, chainID)
	if err != nil {
		return nil, fmt.Errorf("evidence: scan active: %w", err)
	}
	defer rows.Close()
	return collect(rows)
}

// ByBlockHash returns evidence anchored to any of the given source block
// hashes — used by ReorgHandler to find what to invalidate (docs/04 §2 [8]).
func (s *Store) ByBlockHash(ctx context.Context, chainID string, blockHashes []string) ([]domain.MergeEvidence, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_id, op_id, address_a, address_b, heuristic_key,
		       source_txid, source_block_hash, source_block_height,
		       confidence, status, invalidated_reason, created_at, created_by
		FROM clustering.merge_evidence
		WHERE chain_id = $1 AND source_block_hash = ANY($2)
	`, chainID, blockHashes)
	if err != nil {
		return nil, fmt.Errorf("evidence: by block hash: %w", err)
	}
	defer rows.Close()
	return collect(rows)
}

// ByAddressPair returns all evidence (active and invalidated) recorded
// between two addresses, for audit (docs/01 FR-26).
func (s *Store) ByAddressPair(ctx context.Context, chainID, a, b string) ([]domain.MergeEvidence, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_id, op_id, address_a, address_b, heuristic_key,
		       source_txid, source_block_hash, source_block_height,
		       confidence, status, invalidated_reason, created_at, created_by
		FROM clustering.merge_evidence
		WHERE chain_id = $1 AND ((address_a = $2 AND address_b = $3) OR (address_a = $3 AND address_b = $2))
		ORDER BY op_id ASC
	`, chainID, a, b)
	if err != nil {
		return nil, fmt.Errorf("evidence: by address pair: %w", err)
	}
	defer rows.Close()
	return collect(rows)
}

// ByAddresses returns all evidence touching any of the given addresses.
// docs/04 §2 [5] specifies a byCluster(cluster_id) capability; that is
// composed at a higher layer as ByAddresses(ClusterStore.MembersOf(...))
// to avoid EvidenceStore depending on ClusterStore (they are peer
// components per docs/04 §1).
func (s *Store) ByAddresses(ctx context.Context, chainID string, addresses []string) ([]domain.MergeEvidence, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_id, op_id, address_a, address_b, heuristic_key,
		       source_txid, source_block_hash, source_block_height,
		       confidence, status, invalidated_reason, created_at, created_by
		FROM clustering.merge_evidence
		WHERE chain_id = $1 AND (address_a = ANY($2) OR address_b = ANY($2))
		ORDER BY op_id ASC
	`, chainID, addresses)
	if err != nil {
		return nil, fmt.Errorf("evidence: by addresses: %w", err)
	}
	defer rows.Close()
	return collect(rows)
}

func collect(rows pgx.Rows) ([]domain.MergeEvidence, error) {
	var out []domain.MergeEvidence
	for rows.Next() {
		var e domain.MergeEvidence
		if err := rows.Scan(
			&e.ChainID, &e.OpID, &e.AddressA, &e.AddressB, &e.HeuristicKey,
			&e.SourceTxID, &e.SourceBlockHash, &e.SourceBlockHeight,
			&e.Confidence, &e.Status, &e.InvalidatedReason, &e.CreatedAt, &e.CreatedBy,
		); err != nil {
			return nil, fmt.Errorf("evidence: scan row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("evidence: rows: %w", err)
	}
	return out, nil
}
