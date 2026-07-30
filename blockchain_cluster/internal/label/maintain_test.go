package label

import (
	"testing"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

func clusterLabel(id int64, clusterID, category, status string) domain.Label {
	c := clusterID
	return domain.Label{LabelID: id, TargetType: "cluster", ChainID: "bitcoin", TargetClusterID: &c, Category: category, Status: status}
}

func TestDetectConflicts_DisagreeingCategoriesFlagged(t *testing.T) {
	labels := []domain.Label{
		clusterLabel(1, "c1", "exchange", "active"),
		clusterLabel(2, "c1", "mixer", "active"),
	}
	got := DetectConflicts(labels)
	if !got[1] || !got[2] {
		t.Fatalf("expected both disagreeing labels flagged, got %v", got)
	}
}

func TestDetectConflicts_AgreeingCategoriesNotFlagged(t *testing.T) {
	labels := []domain.Label{
		clusterLabel(1, "c1", "exchange", "active"),
		clusterLabel(2, "c1", "exchange", "active"), // corroboration, not conflict
	}
	got := DetectConflicts(labels)
	if len(got) != 0 {
		t.Fatalf("expected no conflicts for agreeing labels, got %v", got)
	}
}

func TestDetectConflicts_DifferentTargetsIndependent(t *testing.T) {
	labels := []domain.Label{
		clusterLabel(1, "c1", "exchange", "active"),
		clusterLabel(2, "c2", "mixer", "active"), // different cluster, unrelated
	}
	got := DetectConflicts(labels)
	if len(got) != 0 {
		t.Fatalf("expected no conflicts across different targets, got %v", got)
	}
}

func TestDetectConflicts_RetiredLabelsIgnored(t *testing.T) {
	labels := []domain.Label{
		clusterLabel(1, "c1", "exchange", "active"),
		clusterLabel(2, "c1", "mixer", "retired"), // retired, shouldn't count
	}
	got := DetectConflicts(labels)
	if len(got) != 0 {
		t.Fatalf("expected retired labels excluded from conflict detection, got %v", got)
	}
}

func TestDetectConflicts_StaleLabelsStillEligible(t *testing.T) {
	labels := []domain.Label{
		clusterLabel(1, "c1", "exchange", "stale"),
		clusterLabel(2, "c1", "mixer", "active"),
	}
	got := DetectConflicts(labels)
	if !got[1] || !got[2] {
		t.Fatalf("expected stale labels to still participate in conflict detection, got %v", got)
	}
}

func TestDetectConflicts_ThreeWayAgreementNoConflict(t *testing.T) {
	labels := []domain.Label{
		clusterLabel(1, "c1", "exchange", "active"),
		clusterLabel(2, "c1", "exchange", "active"),
		clusterLabel(3, "c1", "exchange", "active"),
	}
	got := DetectConflicts(labels)
	if len(got) != 0 {
		t.Fatalf("expected three-way agreement to have no conflicts, got %v", got)
	}
}
