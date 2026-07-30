//go:build integration

package metrics

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
	"github.com/tkdlqm2/blockchain-cluster/internal/registry"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/metrics/...
//
// M8 DoD (docs/05 §1): "핵심 지표가 노출된다" — exercises the collector
// through a real prometheus.Registry (the same path promhttp.Handler uses).
func TestCollector_ExposesClusterAndLabelMetrics(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	labelStore := label.NewStore(pool)
	registryStore := registry.NewStore(pool)

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b := run+"-A", run+"-B"
	blockHash := "b-" + run

	if _, err := evidenceStore.Append(ctx, domain.MergeEvidence{
		ChainID: "bitcoin", AddressA: a, AddressB: b, HeuristicKey: "common-input",
		SourceBlockHash: &blockHash, Confidence: 0.9,
	}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := clusterStore.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	clusterID := run + "-metrics-cluster"
	if _, err := labelStore.AddLabel(ctx, domain.Label{
		TargetType: "cluster", ChainID: "bitcoin", TargetClusterID: &clusterID,
		Label: "Metrics Test Label", Category: "exchange", Source: "official", SourceConfidence: 0.9,
	}); err != nil {
		t.Fatalf("add_label: %v", err)
	}

	promReg := prometheus.NewRegistry()
	promReg.MustRegister(NewCollector(pool, registryStore))

	families, err := promReg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	if !hasMatchingMetric(families, "clustering_merge_evidence_active", map[string]string{"chain": "bitcoin", "heuristic_key": "common-input"}, func(v float64) bool { return v > 0 }) {
		t.Fatalf("expected clustering_merge_evidence_active{chain=bitcoin,heuristic_key=common-input} > 0")
	}
	if !hasMatchingMetric(families, "clustering_label_status_total", map[string]string{"chain": "bitcoin", "status": "active"}, func(v float64) bool { return v > 0 }) {
		t.Fatalf("expected clustering_label_status_total{chain=bitcoin,status=active} > 0")
	}
	if !hasMatchingMetric(families, "clustering_cluster_max_size", map[string]string{"chain": "bitcoin"}, func(v float64) bool { return v >= 2 }) {
		t.Fatalf("expected clustering_cluster_max_size{chain=bitcoin} >= 2")
	}
}

func hasMatchingMetric(families []*dto.MetricFamily, name string, wantLabels map[string]string, valueOK func(float64) bool) bool {
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			matched := true
			for k, v := range wantLabels {
				if labels[k] != v {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			if valueOK(m.GetGauge().GetValue()) {
				return true
			}
		}
	}
	return false
}
