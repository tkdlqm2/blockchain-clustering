// Package metrics는 NFR-7(관찰성)이 요구하는 지표를 Prometheus /metrics로 노출한다:
// 인덱싱 지연, reorg 빈도·깊이, 발행 실패율.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	IndexingLagBlocks = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "indexer_lag_blocks",
		Help: "노드 최신 높이 - 커서 (체인별 인덱싱 지연)",
	}, []string{"chain_id"})

	ReorgTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_reorg_total",
		Help: "감지된 reorg 총 횟수",
	}, []string{"chain_id"})

	ReorgDepthBlocks = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "indexer_reorg_depth_blocks",
		Help:    "reorg 롤백 깊이(블록 수) 분포",
		Buckets: prometheus.LinearBuckets(1, 1, 10),
	}, []string{"chain_id"})

	PublishFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_publish_failures_total",
		Help: "Kafka 발행 실패 총 횟수",
	}, []string{"chain_id"})
)

// Handler는 promhttp 표준 핸들러를 반환한다. cmd/indexer에서 listenAddr에 마운트한다.
func Handler() http.Handler {
	return promhttp.Handler()
}
