// Package metrics implements the NFR-6 observability surface (docs/01 §NFR-6:
// "supernode 발생, 병합률, 오탐 정정 빈도, 라벨 신선도 등을 지표로 노출한다";
// docs/05 M8 DoD: "핵심 지표가 노출된다").
//
// This is a pull-based prometheus.Collector: every /metrics scrape runs a
// handful of read-only queries per registered chain (discovered via
// registry.Store.ListChains, never hardcoded). That's simpler than wiring a
// metrics recorder into every component that could affect these numbers
// (MergeEngine, ReorgHandler, LabelStore) — the numbers are fundamentally
// "what does the database say right now", which a live query answers
// directly and correctly, at the cost of doing that query on every scrape
// rather than incrementally maintaining a counter in memory.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// ChainLister is registry.Store's chain-discovery path.
type ChainLister interface {
	ListChains(ctx context.Context) ([]string, error)
}

type Collector struct {
	pool   *pgxpool.Pool
	chains ChainLister

	clusterMaxSize       *prometheus.Desc
	clusterCount         *prometheus.Desc
	mergeEvidenceActive  *prometheus.Desc
	mergeEvidenceInvalid *prometheus.Desc
	labelStatusTotal     *prometheus.Desc
	scrapeErrorsTotal    prometheus.Counter
}

func NewCollector(pool *pgxpool.Pool, chains ChainLister) *Collector {
	return &Collector{
		pool:   pool,
		chains: chains,
		clusterMaxSize: prometheus.NewDesc(
			"clustering_cluster_max_size",
			"Largest cluster size per chain — a sudden jump is the AC-4/T-3 supernode signal (docs/05 §4).",
			[]string{"chain"}, nil,
		),
		clusterCount: prometheus.NewDesc(
			"clustering_cluster_count",
			"Number of materialized clusters per chain.",
			[]string{"chain"}, nil,
		),
		mergeEvidenceActive: prometheus.NewDesc(
			"clustering_merge_evidence_active",
			"Active merge_evidence rows per chain and heuristic — the merge rate (docs/05 §4).",
			[]string{"chain", "heuristic_key"}, nil,
		),
		mergeEvidenceInvalid: prometheus.NewDesc(
			"clustering_merge_evidence_invalidated",
			"Invalidated merge_evidence rows per chain and reason — reorg vs manual-correction frequency (docs/05 §4 정정율).",
			[]string{"chain", "reason"}, nil,
		),
		labelStatusTotal: prometheus.NewDesc(
			"clustering_label_status_total",
			"Labels per chain and status (active/stale/conflicted/retired) — label freshness (NFR-6).",
			[]string{"chain", "status"}, nil,
		),
		scrapeErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "clustering_metrics_scrape_errors_total",
			Help: "Errors encountered while collecting clustering metrics.",
		}),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.clusterMaxSize
	ch <- c.clusterCount
	ch <- c.mergeEvidenceActive
	ch <- c.mergeEvidenceInvalid
	ch <- c.labelStatusTotal
	c.scrapeErrorsTotal.Describe(ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	chains, err := c.chains.ListChains(ctx)
	if err != nil {
		slog.Error("metrics: list_chains failed", "error", err)
		c.scrapeErrorsTotal.Inc()
		ch <- c.scrapeErrorsTotal
		return
	}

	for _, chainID := range chains {
		if err := c.collectClusterStats(ctx, ch, chainID); err != nil {
			slog.Error("metrics: cluster stats failed", "chain", chainID, "error", err)
			c.scrapeErrorsTotal.Inc()
		}
		if err := c.collectMergeEvidenceStats(ctx, ch, chainID); err != nil {
			slog.Error("metrics: merge evidence stats failed", "chain", chainID, "error", err)
			c.scrapeErrorsTotal.Inc()
		}
		if err := c.collectLabelStats(ctx, ch, chainID); err != nil {
			slog.Error("metrics: label stats failed", "chain", chainID, "error", err)
			c.scrapeErrorsTotal.Inc()
		}
	}
	ch <- c.scrapeErrorsTotal
}

func (c *Collector) collectClusterStats(ctx context.Context, ch chan<- prometheus.Metric, chainID string) error {
	var maxSize, count int64
	err := c.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(size), 0), COUNT(*) FROM clustering.cluster WHERE chain_id = $1
	`, chainID).Scan(&maxSize, &count)
	if err != nil {
		return err
	}
	ch <- prometheus.MustNewConstMetric(c.clusterMaxSize, prometheus.GaugeValue, float64(maxSize), chainID)
	ch <- prometheus.MustNewConstMetric(c.clusterCount, prometheus.GaugeValue, float64(count), chainID)
	return nil
}

func (c *Collector) collectMergeEvidenceStats(ctx context.Context, ch chan<- prometheus.Metric, chainID string) error {
	activeRows, err := c.pool.Query(ctx, `
		SELECT heuristic_key, COUNT(*) FROM clustering.merge_evidence
		WHERE chain_id = $1 AND status = 'active' GROUP BY heuristic_key
	`, chainID)
	if err != nil {
		return err
	}
	defer activeRows.Close()
	for activeRows.Next() {
		var heuristicKey string
		var count int64
		if err := activeRows.Scan(&heuristicKey, &count); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(c.mergeEvidenceActive, prometheus.GaugeValue, float64(count), chainID, heuristicKey)
	}
	if err := activeRows.Err(); err != nil {
		return err
	}

	invalidRows, err := c.pool.Query(ctx, `
		SELECT COALESCE(invalidated_reason, 'unknown'), COUNT(*) FROM clustering.merge_evidence
		WHERE chain_id = $1 AND status = 'invalidated' GROUP BY invalidated_reason
	`, chainID)
	if err != nil {
		return err
	}
	defer invalidRows.Close()
	for invalidRows.Next() {
		var reason string
		var count int64
		if err := invalidRows.Scan(&reason, &count); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(c.mergeEvidenceInvalid, prometheus.GaugeValue, float64(count), chainID, reason)
	}
	return invalidRows.Err()
}

func (c *Collector) collectLabelStats(ctx context.Context, ch chan<- prometheus.Metric, chainID string) error {
	rows, err := c.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM clustering.label WHERE chain_id = $1 GROUP BY status
	`, chainID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(c.labelStatusTotal, prometheus.GaugeValue, float64(count), chainID, status)
	}
	return rows.Err()
}
