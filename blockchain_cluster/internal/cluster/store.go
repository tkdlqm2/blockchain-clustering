package cluster

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
)

type Store struct {
	pool     *pgxpool.Pool
	evidence *evidence.Store
}

func NewStore(pool *pgxpool.Pool, evidenceStore *evidence.Store) *Store {
	return &Store{pool: pool, evidence: evidenceStore}
}

// RebuildFromEvidence replays merge_evidence(active) for chainID and
// atomically replaces the derived cluster/cluster_membership rows for that
// chain. cluster/cluster_membership are caches: if they ever disagree with
// merge_evidence, replay is what's correct (docs/02 §5 invariant), so this
// always does a full replace rather than an incremental diff — the
// "정확성 기준 구현은 전체 재생" guidance in docs/03 §9.
func (s *Store) RebuildFromEvidence(ctx context.Context, chainID string) error {
	active, err := s.evidence.ScanActive(ctx, chainID)
	if err != nil {
		return fmt.Errorf("cluster: rebuild: scan evidence: %w", err)
	}
	results := Replay(active)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cluster: rebuild: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op if committed

	if _, err := tx.Exec(ctx, `DELETE FROM clustering.cluster_membership WHERE chain_id = $1`, chainID); err != nil {
		return fmt.Errorf("cluster: rebuild: clear membership: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM clustering.cluster WHERE chain_id = $1`, chainID); err != nil {
		return fmt.Errorf("cluster: rebuild: clear clusters: %w", err)
	}

	now := time.Now()
	for _, r := range results {
		if _, err := tx.Exec(ctx, `
			INSERT INTO clustering.cluster
				(chain_id, cluster_id, size, entity_type, representative_confidence, updated_at)
			VALUES ($1,$2,$3,'unknown',$4,$5)
		`, chainID, r.ClusterID, r.Size, r.RepresentativeConfidence, now); err != nil {
			return fmt.Errorf("cluster: rebuild: insert cluster %s: %w", r.ClusterID, err)
		}
	}

	rows := make([][]interface{}, 0)
	for _, r := range results {
		for _, m := range r.Members {
			rows = append(rows, []interface{}{chainID, m.Address, r.ClusterID, m.Confidence})
		}
	}
	if len(rows) > 0 {
		if _, err := tx.CopyFrom(ctx,
			pgx.Identifier{"clustering", "cluster_membership"},
			[]string{"chain_id", "address", "cluster_id", "membership_confidence"},
			pgx.CopyFromRows(rows),
		); err != nil {
			return fmt.Errorf("cluster: rebuild: copy membership: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("cluster: rebuild: commit: %w", err)
	}
	return nil
}

// thresholdEpsilon: minConfidence at or below this is treated as "the full
// view" (equivalent to 0) — the materialized cluster/cluster_membership
// tables already represent exactly that view, so there's no need to pay
// for an on-demand replay.
const thresholdEpsilon = 1e-9

// ClusterOf returns the cluster containing address, if its membership
// confidence meets minConfidence.
//
// minConfidence > 0 does NOT just filter the materialized table — it
// recomputes the clustering from only evidence at or above that confidence
// (docs/03 §7: "그 임계치 이상 근거로만 재구성한 클러스터"). This matters
// because removing a weak bridge edge can split what's one cluster at
// min_confidence=0 into two entirely separate clusters at a higher
// threshold; a stored per-address confidence column can't express that —
// only replaying the filtered evidence set can (see thresholdView).
func (s *Store) ClusterOf(ctx context.Context, chainID, address string, minConfidence float64) (clusterID string, confidence float64, found bool, err error) {
	if minConfidence <= thresholdEpsilon {
		return s.clusterOfMaterialized(ctx, chainID, address)
	}
	results, err := s.thresholdView(ctx, chainID, minConfidence)
	if err != nil {
		return "", 0, false, err
	}
	for _, r := range results {
		for _, m := range r.Members {
			if m.Address == address {
				return r.ClusterID, m.Confidence, true, nil
			}
		}
	}
	return "", 0, false, nil
}

func (s *Store) clusterOfMaterialized(ctx context.Context, chainID, address string) (clusterID string, confidence float64, found bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT cluster_id, membership_confidence
		FROM clustering.cluster_membership
		WHERE chain_id = $1 AND address = $2
	`, chainID, address).Scan(&clusterID, &confidence)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", 0, false, nil
		}
		return "", 0, false, fmt.Errorf("cluster: cluster_of: %w", err)
	}
	return clusterID, confidence, true, nil
}

// MembersOf pages through a cluster's addresses at or above minConfidence.
// Like ClusterOf, minConfidence > 0 triggers on-demand reconstruction
// rather than filtering the materialized cache — see ClusterOf's doc.
func (s *Store) MembersOf(ctx context.Context, chainID, clusterID string, minConfidence float64, limit, offset int) ([]string, error) {
	if minConfidence <= thresholdEpsilon {
		return s.membersOfMaterialized(ctx, chainID, clusterID, limit, offset)
	}

	results, err := s.thresholdView(ctx, chainID, minConfidence)
	if err != nil {
		return nil, err
	}
	var addrs []string
	for _, r := range results {
		if r.ClusterID != clusterID {
			continue
		}
		for _, m := range r.Members {
			addrs = append(addrs, m.Address)
		}
		break
	}
	sort.Strings(addrs)
	if offset >= len(addrs) {
		return nil, nil
	}
	end := offset + limit
	if limit <= 0 || end > len(addrs) {
		end = len(addrs)
	}
	return addrs[offset:end], nil
}

func (s *Store) membersOfMaterialized(ctx context.Context, chainID, clusterID string, limit, offset int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT address
		FROM clustering.cluster_membership
		WHERE chain_id = $1 AND cluster_id = $2
		ORDER BY address
		LIMIT $3 OFFSET $4
	`, chainID, clusterID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("cluster: members_of: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, fmt.Errorf("cluster: members_of: scan: %w", err)
		}
		out = append(out, addr)
	}
	return out, rows.Err()
}

// SameCluster reports whether a and b are in the same cluster at
// minConfidence, taking the more conservative (lower) of the two
// membership confidences.
func (s *Store) SameCluster(ctx context.Context, chainID, a, b string, minConfidence float64) (same bool, confidence float64, err error) {
	clusterA, confA, foundA, err := s.ClusterOf(ctx, chainID, a, minConfidence)
	if err != nil || !foundA {
		return false, 0, err
	}
	clusterB, confB, foundB, err := s.ClusterOf(ctx, chainID, b, minConfidence)
	if err != nil || !foundB {
		return false, 0, err
	}
	if clusterA != clusterB {
		return false, 0, nil
	}
	if confA < confB {
		return true, confA, nil
	}
	return true, confB, nil
}

// thresholdView re-derives clusters from only active evidence whose own
// confidence is at or above minConfidence, then replays. This reads and
// replays the chain's *entire* active evidence set per call — deliberately
// correctness-first (matching RebuildFromEvidence's full-replay philosophy,
// docs/03 §9) rather than caching per-threshold views; if this becomes a
// hot path at scale, memoizing per (chainID, minConfidence) is the natural
// next optimization, but only once it's verified to produce identical
// results to a fresh replay.
func (s *Store) thresholdView(ctx context.Context, chainID string, minConfidence float64) ([]Result, error) {
	active, err := s.evidence.ScanActive(ctx, chainID)
	if err != nil {
		return nil, fmt.Errorf("cluster: threshold_view: scan evidence: %w", err)
	}
	filtered := make([]domain.MergeEvidence, 0, len(active))
	for _, e := range active {
		if e.Confidence >= minConfidence {
			filtered = append(filtered, e)
		}
	}
	return Replay(filtered), nil
}
