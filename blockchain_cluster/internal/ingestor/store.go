package ingestor

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/pgnumeric"
)

// AddressUpserter is address.Store's registration path — narrowed to an
// interface so Ingestor doesn't need a hard dependency on the address
// package's concrete type.
type AddressUpserter interface {
	Upsert(ctx context.Context, chainID, address string, blockHeight int64) error
}

type Store struct {
	pool      *pgxpool.Pool
	addresses AddressUpserter
}

func NewStore(pool *pgxpool.Pool, addresses AddressUpserter) *Store {
	return &Store{pool: pool, addresses: addresses}
}

// IngestResult reports how many rows a batch actually added versus how many
// were already present (FR-3: re-ingesting a duplicate must not change state).
type IngestResult struct {
	Inserted int
	Skipped  int
}

// Ingest idempotently appends deltas. The (chain_id, txid, delta_index)
// primary key is the natural idempotency key — re-ingesting an already-seen
// delta is a no-op (ON CONFLICT DO NOTHING), never an update, since a given
// delta_index within a txid is an immutable fact reported by the indexer.
func (s *Store) Ingest(ctx context.Context, deltas []domain.BalanceDelta) (IngestResult, error) {
	if len(deltas) == 0 {
		return IngestResult{}, nil
	}

	batch := &pgx.Batch{}
	for _, d := range deltas {
		amount, err := pgnumeric.FromBigInt(d.Amount)
		if err != nil {
			return IngestResult{}, fmt.Errorf("ingestor: ingest: %s/%s#%d: %w", d.ChainID, d.TxID, d.DeltaIndex, err)
		}
		var meta any
		if len(d.Meta) > 0 {
			meta = d.Meta
		}
		batch.Queue(`
			INSERT INTO clustering.balance_delta
				(chain_id, txid, delta_index, address, amount, kind, block_height, block_hash, meta)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (chain_id, txid, delta_index) DO NOTHING
		`, d.ChainID, d.TxID, d.DeltaIndex, d.Address, amount, d.Kind, d.BlockHeight, d.BlockHash, meta)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	var result IngestResult
	for range deltas {
		tag, err := br.Exec()
		if err != nil {
			return result, fmt.Errorf("ingestor: ingest: %w", err)
		}
		if tag.RowsAffected() > 0 {
			result.Inserted++
		} else {
			result.Skipped++
		}
	}

	if err := s.upsertAddresses(ctx, deltas); err != nil {
		return result, fmt.Errorf("ingestor: ingest: %w", err)
	}
	return result, nil
}

// upsertAddresses registers every address touched by this batch (docs/04 §2
// [1]: "정규화·적재"). Preprocessor's hub/dust flags (M3) live on this table,
// so a row must exist here for every address that shows up in balance_delta
// — without this, SetHub/SetDustFlag would silently no-op on a missing row.
// One Upsert call per distinct address (deduped, keeping the max block
// height observed in this batch) rather than one per delta.
func (s *Store) upsertAddresses(ctx context.Context, deltas []domain.BalanceDelta) error {
	type addressKey struct{ chainID, address string }
	maxHeight := make(map[addressKey]int64, len(deltas))
	for _, d := range deltas {
		k := addressKey{d.ChainID, d.Address}
		if h, ok := maxHeight[k]; !ok || d.BlockHeight > h {
			maxHeight[k] = d.BlockHeight
		}
	}
	// Map iteration order is random, but Upsert is commutative and
	// idempotent (first_seen_height only set once, last_seen_height only
	// ever grows via GREATEST), so unlike op_id-ordered evidence, call order
	// here does not affect the resulting state.
	for k, height := range maxHeight {
		if err := s.addresses.Upsert(ctx, k.chainID, k.address, height); err != nil {
			return fmt.Errorf("upsert address %s/%s: %w", k.chainID, k.address, err)
		}
	}
	return nil
}

// GetDeltasByTx returns a transaction's deltas ordered by delta_index — the
// order commonInputHeuristic (M2) and the derive.go helpers expect.
func (s *Store) GetDeltasByTx(ctx context.Context, chainID, txid string) ([]domain.BalanceDelta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_id, txid, delta_index, address, amount, kind, block_height, block_hash, meta
		FROM clustering.balance_delta
		WHERE chain_id = $1 AND txid = $2
		ORDER BY delta_index ASC
	`, chainID, txid)
	if err != nil {
		return nil, fmt.Errorf("ingestor: get_deltas_by_tx: %w", err)
	}
	defer rows.Close()
	return collect(rows)
}

// GetDeltasByBlock returns every delta observed in a block — used by
// ReorgHandler (M5) to correlate rolled-back blocks with affected evidence.
func (s *Store) GetDeltasByBlock(ctx context.Context, chainID, blockHash string) ([]domain.BalanceDelta, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT chain_id, txid, delta_index, address, amount, kind, block_height, block_hash, meta
		FROM clustering.balance_delta
		WHERE chain_id = $1 AND block_hash = $2
		ORDER BY txid ASC, delta_index ASC
	`, chainID, blockHash)
	if err != nil {
		return nil, fmt.Errorf("ingestor: get_deltas_by_block: %w", err)
	}
	defer rows.Close()
	return collect(rows)
}

// GetCursor reads the last recorded consumption position for a chain+source
// (e.g. a Kafka partition offset), backed by the ingest_cursor table.
func (s *Store) GetCursor(ctx context.Context, chainID, source string) (position string, found bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT position FROM clustering.ingest_cursor WHERE chain_id = $1 AND source = $2
	`, chainID, source).Scan(&position)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return "", false, fmt.Errorf("ingestor: get_cursor: %w", err)
	}
	return position, true, nil
}

// SetCursor records how far consumption has progressed.
func (s *Store) SetCursor(ctx context.Context, chainID, source, position string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO clustering.ingest_cursor (chain_id, source, position, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (chain_id, source) DO UPDATE
		SET position = EXCLUDED.position, updated_at = now()
	`, chainID, source, position)
	if err != nil {
		return fmt.Errorf("ingestor: set_cursor: %w", err)
	}
	return nil
}

func collect(rows pgx.Rows) ([]domain.BalanceDelta, error) {
	var out []domain.BalanceDelta
	for rows.Next() {
		var d domain.BalanceDelta
		var amount pgtype.Numeric
		if err := rows.Scan(
			&d.ChainID, &d.TxID, &d.DeltaIndex, &d.Address, &amount,
			&d.Kind, &d.BlockHeight, &d.BlockHash, &d.Meta,
		); err != nil {
			return nil, fmt.Errorf("ingestor: scan row: %w", err)
		}
		v, err := pgnumeric.ToBigInt(amount)
		if err != nil {
			return nil, fmt.Errorf("ingestor: %s/%s#%d: %w", d.ChainID, d.TxID, d.DeltaIndex, err)
		}
		d.Amount = v
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ingestor: rows: %w", err)
	}
	return out, nil
}
