//go:build integration

package label

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/label/...
//
// M6 DoD (docs/05 §1): a stale label decays and gets flagged, and
// conflicting labels on the same target are marked 'conflicted' rather
// than auto-resolved (FR-20/FR-21).
func TestMaintain_DecaysStaleLabelAndFlagsConflict(t *testing.T) {
	pool := integrationtest.Pool(t)
	store := NewStore(pool)
	ctx := context.Background()

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	clusterID := run + "-cluster"

	staleID, err := store.AddLabel(ctx, domain.Label{
		TargetType: "cluster", ChainID: "bitcoin", TargetClusterID: &clusterID,
		Label: "Old Exchange Label", Category: "exchange", Source: "crowdsourced", SourceConfidence: 0.6,
	})
	if err != nil {
		t.Fatalf("add stale label: %v", err)
	}
	// Backdate last_verified_at past any reasonable staleTTL.
	if _, err := pool.Exec(ctx, `UPDATE clustering.label SET last_verified_at = now() - interval '365 days' WHERE label_id = $1`, staleID); err != nil {
		t.Fatalf("backdate label: %v", err)
	}

	conflictClusterID := run + "-conflict-cluster"
	if _, err := store.AddLabel(ctx, domain.Label{
		TargetType: "cluster", ChainID: "bitcoin", TargetClusterID: &conflictClusterID,
		Label: "Some Exchange", Category: "exchange", Source: "official", SourceConfidence: 0.9,
	}); err != nil {
		t.Fatalf("add label A: %v", err)
	}
	if _, err := store.AddLabel(ctx, domain.Label{
		TargetType: "cluster", ChainID: "bitcoin", TargetClusterID: &conflictClusterID,
		Label: "Some Mixer", Category: "mixer", Source: "investigation", SourceConfidence: 0.7,
	}); err != nil {
		t.Fatalf("add label B: %v", err)
	}

	result, err := store.Maintain(ctx, "bitcoin", 30*24*time.Hour, 0.5)
	if err != nil {
		t.Fatalf("maintain: %v", err)
	}
	if result.Decayed < 1 {
		t.Fatalf("expected at least 1 decayed label, got %+v", result)
	}
	if result.Conflicted != 2 {
		t.Fatalf("expected exactly 2 conflicted labels, got %+v", result)
	}

	staleLabels, err := store.LabelsOf(ctx, "bitcoin", "cluster", clusterID)
	if err != nil {
		t.Fatalf("labels_of (stale): %v", err)
	}
	if len(staleLabels) != 1 || staleLabels[0].Status != "stale" {
		t.Fatalf("expected the stale label marked 'stale', got %+v", staleLabels)
	}
	if staleLabels[0].SourceConfidence >= 0.6 {
		t.Fatalf("expected source_confidence to decay below 0.6, got %v", staleLabels[0].SourceConfidence)
	}

	conflictLabels, err := store.LabelsOf(ctx, "bitcoin", "cluster", conflictClusterID)
	if err != nil {
		t.Fatalf("labels_of (conflict): %v", err)
	}
	for _, l := range conflictLabels {
		if l.Status != "conflicted" {
			t.Fatalf("expected label_id=%d marked conflicted, got status=%q", l.LabelID, l.Status)
		}
	}
}
