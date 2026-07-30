// Package queryservice implements QueryService (docs/04-architecture §2 [9]):
// the REST surface for docs/01-functional-spec.md §5's logical query
// capabilities. Every response carries confidence — this system never
// asserts identity or membership as fact (docs/01 §6, AC-7).
//
// entity_flow is not implemented — it needs a different aggregation
// (folding address-level flows into entity-level edges), and docs/01 §5
// itself scopes its consumption (investigation/risk tooling) as a
// consumer-service concern built on top of this API, not part of it.
package queryservice

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
)

// Deps are the stores QueryService reads from. It never writes — every
// endpoint here is a read path over data other components already
// produced (docs/04 §2 [9]).
type Deps struct {
	Cluster  *cluster.Store
	Evidence *evidence.Store
	Label    *label.Store
	Address  *address.Store
}

func NewRouter(deps Deps) chi.Router {
	r := chi.NewRouter()
	r.Route("/v1/chains/{chain}", func(r chi.Router) {
		r.Get("/addresses/{address}/cluster", handleClusterOf(deps))
		r.Get("/addresses/{address}/labels", handleLabelsOfAddress(deps))
		r.Get("/addresses/{address}/hub-status", handleHubStatus(deps))
		r.Get("/clusters/{clusterID}/members", handleMembersOf(deps))
		r.Get("/clusters/{clusterID}/labels", handleLabelsOfCluster(deps))
		r.Get("/same-cluster", handleSameCluster(deps))
		r.Get("/evidence", handleEvidenceOf(deps))
	})
	return r
}

func minConfidenceParam(r *http.Request) (float64, error) {
	raw := r.URL.Query().Get("min_confidence")
	if raw == "" {
		return 0, nil
	}
	return strconv.ParseFloat(raw, 64)
}

func paginationParams(r *http.Request) (limit, offset int, err error) {
	limit, offset = 100, 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, err
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, err = strconv.Atoi(raw)
		if err != nil {
			return 0, 0, err
		}
	}
	return limit, offset, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
