package queryservice

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// cluster_of (docs/01 §5): which cluster an address belongs to.
func handleClusterOf(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chainID := chi.URLParam(r, "chain")
		address := chi.URLParam(r, "address")
		minConfidence, err := minConfidenceParam(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid min_confidence")
			return
		}

		clusterID, confidence, found, err := deps.Cluster.ClusterOf(r.Context(), chainID, address, minConfidence)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "address has no cluster at this confidence threshold")
			return
		}
		writeJSON(w, http.StatusOK, struct {
			ClusterID  string  `json:"cluster_id"`
			Confidence float64 `json:"confidence"`
		}{clusterID, confidence})
	}
}

// members_of (docs/01 §5): paginated cluster membership.
func handleMembersOf(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chainID := chi.URLParam(r, "chain")
		clusterID := chi.URLParam(r, "clusterID")
		minConfidence, err := minConfidenceParam(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid min_confidence")
			return
		}
		limit, offset, err := paginationParams(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid limit/offset")
			return
		}

		members, err := deps.Cluster.MembersOf(r.Context(), chainID, clusterID, minConfidence, limit, offset)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, struct {
			ClusterID string   `json:"cluster_id"`
			Members   []string `json:"members"`
		}{clusterID, members})
	}
}

// same_cluster (docs/01 §5): whether two addresses share a cluster.
func handleSameCluster(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chainID := chi.URLParam(r, "chain")
		a := r.URL.Query().Get("a")
		b := r.URL.Query().Get("b")
		if a == "" || b == "" {
			writeError(w, http.StatusBadRequest, "both ?a= and ?b= are required")
			return
		}
		minConfidence, err := minConfidenceParam(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid min_confidence")
			return
		}

		same, confidence, err := deps.Cluster.SameCluster(r.Context(), chainID, a, b, minConfidence)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, struct {
			Same       bool    `json:"same_cluster"`
			Confidence float64 `json:"confidence"`
		}{same, confidence})
	}
}

// evidence_of (docs/01 §5): the audit trail behind a cluster or an address
// pair — cluster_id resolves via ClusterStore.MembersOf then
// EvidenceStore.ByAddresses, composing the two peer components rather than
// EvidenceStore depending on ClusterStore directly (docs/04 §2 [5] note).
func handleEvidenceOf(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chainID := chi.URLParam(r, "chain")
		q := r.URL.Query()
		clusterID := q.Get("cluster_id")
		addressA := q.Get("address_a")
		addressB := q.Get("address_b")

		var rows []domain.MergeEvidence
		var err error
		switch {
		case clusterID != "":
			var members []string
			members, err = deps.Cluster.MembersOf(r.Context(), chainID, clusterID, 0, 100000, 0)
			if err == nil {
				rows, err = deps.Evidence.ByAddresses(r.Context(), chainID, members)
			}
		case addressA != "" && addressB != "":
			rows, err = deps.Evidence.ByAddressPair(r.Context(), chainID, addressA, addressB)
		default:
			writeError(w, http.StatusBadRequest, "provide either ?cluster_id= or both ?address_a=&address_b=")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, struct {
			Evidence []domain.MergeEvidence `json:"evidence"`
		}{rows})
	}
}

// labels_of (docs/01 §5), cluster variant.
func handleLabelsOfCluster(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chainID := chi.URLParam(r, "chain")
		clusterID := chi.URLParam(r, "clusterID")
		labels, err := deps.Label.LabelsOf(r.Context(), chainID, "cluster", clusterID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, struct {
			Labels []domain.Label `json:"labels"`
		}{labels})
	}
}

// labels_of (docs/01 §5), address variant.
func handleLabelsOfAddress(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chainID := chi.URLParam(r, "chain")
		addr := chi.URLParam(r, "address")
		labels, err := deps.Label.LabelsOf(r.Context(), chainID, "address", addr)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, struct {
			Labels []domain.Label `json:"labels"`
		}{labels})
	}
}

// hub_status (docs/01 §5): whether an address is flagged as a hub.
func handleHubStatus(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chainID := chi.URLParam(r, "chain")
		addr := chi.URLParam(r, "address")

		a, found, err := deps.Address.Get(r.Context(), chainID, addr)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "address not seen")
			return
		}
		writeJSON(w, http.StatusOK, struct {
			IsHub         bool     `json:"is_hub"`
			HubType       *string  `json:"hub_type,omitempty"`
			HubConfidence *float64 `json:"hub_confidence,omitempty"`
		}{a.IsHub, a.HubType, a.HubConfidence})
	}
}
