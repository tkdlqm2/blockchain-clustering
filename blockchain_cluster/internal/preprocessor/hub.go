package preprocessor

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// hubLabelCategories are the label categories markHubs treats as an
// authoritative hub signal (docs/03 §1(a): "known 라벨 우선"). A labeled
// exchange/mixer/bridge address is blocked from merges regardless of its
// behavioral score.
var hubLabelCategories = map[string]string{
	"exchange": "exchange",
	"mixer":    "mixer",
	"bridge":   "bridge",
}

// LabelReader is label.Store's read path.
type LabelReader interface {
	LabelsOf(ctx context.Context, chainID, targetType, targetID string) ([]domain.Label, error)
}

// HubSetter is address.Store's hub-flag write path.
type HubSetter interface {
	SetHub(ctx context.Context, chainID, address, hubType string, confidence float64) error
}

// HubDetector implements markHubs (docs/03 §1). See package doc for why the
// behavioral score only uses counterparty degree.
type HubDetector struct {
	pool   *pgxpool.Pool
	labels LabelReader
	hubs   HubSetter
}

func NewHubDetector(pool *pgxpool.Pool, labels LabelReader, hubs HubSetter) *HubDetector {
	return &HubDetector{pool: pool, labels: labels, hubs: hubs}
}

// MarkHubs evaluates each address once: an active hub-category label wins
// outright (confidence 0.99, docs/03 §1(a)); otherwise it falls back to the
// counterparty-degree score against params.HubThreshold.
func (h *HubDetector) MarkHubs(ctx context.Context, chainID string, addresses []string, params domain.PreprocessingParams) error {
	for _, addr := range dedupeStrings(addresses) {
		labels, err := h.labels.LabelsOf(ctx, chainID, "address", addr)
		if err != nil {
			return fmt.Errorf("preprocessor: mark_hubs: labels_of(%s): %w", addr, err)
		}
		if hubType, ok := hubTypeFromLabels(labels); ok {
			if err := h.hubs.SetHub(ctx, chainID, addr, hubType, 0.99); err != nil {
				return fmt.Errorf("preprocessor: mark_hubs: set_hub(%s): %w", addr, err)
			}
			continue
		}

		degree, err := h.counterpartyDegree(ctx, chainID, addr)
		if err != nil {
			return fmt.Errorf("preprocessor: mark_hubs: counterparty_degree(%s): %w", addr, err)
		}
		score := hubScoreFromDegree(degree, params.HubDegreeSaturation)
		if score >= params.HubThreshold {
			if err := h.hubs.SetHub(ctx, chainID, addr, "unknown", score); err != nil {
				return fmt.Errorf("preprocessor: mark_hubs: set_hub(%s): %w", addr, err)
			}
		}
	}
	return nil
}

// counterpartyDegree counts distinct addresses that co-occur with address
// in any transaction, across all of balance_delta for this chain — a hub's
// defining trait is behavioral history, not just what's in the current
// batch, so this deliberately isn't scoped to the batch being processed.
func (h *HubDetector) counterpartyDegree(ctx context.Context, chainID, address string) (int, error) {
	var degree int
	err := h.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT other.address)
		FROM clustering.balance_delta self
		JOIN clustering.balance_delta other
		  ON other.chain_id = self.chain_id AND other.txid = self.txid AND other.address <> self.address
		WHERE self.chain_id = $1 AND self.address = $2
	`, chainID, address).Scan(&degree)
	if err != nil {
		return 0, err
	}
	return degree, nil
}

func hubScoreFromDegree(degree int, saturation float64) float64 {
	if saturation <= 0 {
		return 0
	}
	score := float64(degree) / saturation
	if score > 1 {
		score = 1
	}
	return score
}

func hubTypeFromLabels(labels []domain.Label) (string, bool) {
	for _, l := range labels {
		if l.Status != "active" {
			continue
		}
		if hubType, ok := hubLabelCategories[l.Category]; ok {
			return hubType, true
		}
	}
	return "", false
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
