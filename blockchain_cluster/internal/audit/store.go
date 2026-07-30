// Package audit implements the audit_log write path (docs/02-data-model.md
// §9, FR-26: "누가·언제·무엇을·왜"). Any component that mutates state in a
// way an operator might need to explain later — ReorgHandler's
// invalidations first, more later — logs through this.
package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Log records one audit event. target is marshaled to JSON as-is — callers
// pass whatever identifies what was acted on (e.g. a small struct or map
// with chain_id/op_id).
func (s *Store) Log(ctx context.Context, actor, action string, target any, rationale string) error {
	targetJSON, err := json.Marshal(target)
	if err != nil {
		return fmt.Errorf("audit: marshal target: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO clustering.audit_log (actor, action, target, rationale)
		VALUES ($1, $2, $3, $4)
	`, actor, action, targetJSON, rationale)
	if err != nil {
		return fmt.Errorf("audit: log: %w", err)
	}
	return nil
}
