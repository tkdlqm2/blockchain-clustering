//go:build integration

package queryservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/cluster"
	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/evidence"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
	"github.com/tkdlqm2/blockchain-cluster/internal/label"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered.
// Run with: go test -tags=integration ./internal/queryservice/...
//
// End to end over real HTTP: docs/01 §5's REST surface, backed by the live
// stores. Every response must carry confidence (AC-7).
func TestQueryService_FullSurface(t *testing.T) {
	pool := integrationtest.Pool(t)
	ctx := context.Background()

	addrStore := address.NewStore(pool)
	evidenceStore := evidence.NewStore(pool)
	clusterStore := cluster.NewStore(pool, evidenceStore)
	labelStore := label.NewStore(pool)

	server := httptest.NewServer(NewRouter(Deps{
		Cluster:  clusterStore,
		Evidence: evidenceStore,
		Label:    labelStore,
		Address:  addrStore,
	}))
	defer server.Close()

	run := fmt.Sprintf("it%d", time.Now().UnixNano())
	a, b := run+"-A", run+"-B"
	blockHash := "b-" + run
	txid := "tx-" + run

	if err := addrStore.Upsert(ctx, "bitcoin", a, 100); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if err := addrStore.Upsert(ctx, "bitcoin", b, 100); err != nil {
		t.Fatalf("upsert b: %v", err)
	}
	if err := addrStore.SetHub(ctx, "bitcoin", a, "exchange", 0.95); err != nil {
		t.Fatalf("set_hub: %v", err)
	}

	if _, err := evidenceStore.Append(ctx, domain.MergeEvidence{
		ChainID: "bitcoin", AddressA: a, AddressB: b, HeuristicKey: "common-input",
		SourceTxID: &txid, SourceBlockHash: &blockHash, Confidence: 0.9,
	}); err != nil {
		t.Fatalf("append evidence: %v", err)
	}
	if err := clusterStore.RebuildFromEvidence(ctx, "bitcoin"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	clusterID := run + "-cluster-label-target"
	if _, err := labelStore.AddLabel(ctx, domain.Label{
		TargetType: "cluster", ChainID: "bitcoin", TargetClusterID: &clusterID,
		Label: "Test Exchange", Category: "exchange", Source: "official", SourceConfidence: 0.9,
	}); err != nil {
		t.Fatalf("add_label: %v", err)
	}

	get := func(path string) (int, map[string]any) {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("GET %s: decode: %v", path, err)
		}
		return resp.StatusCode, body
	}

	// cluster_of
	status, body := get(fmt.Sprintf("/v1/chains/bitcoin/addresses/%s/cluster", b))
	if status != http.StatusOK {
		t.Fatalf("cluster_of: expected 200, got %d body=%v", status, body)
	}
	realClusterID, _ := body["cluster_id"].(string)
	if realClusterID == "" {
		t.Fatalf("cluster_of: expected non-empty cluster_id, got %v", body)
	}
	if _, ok := body["confidence"]; !ok {
		t.Fatalf("cluster_of: response missing confidence (AC-7): %v", body)
	}

	// members_of
	status, body = get(fmt.Sprintf("/v1/chains/bitcoin/clusters/%s/members", realClusterID))
	if status != http.StatusOK {
		t.Fatalf("members_of: expected 200, got %d body=%v", status, body)
	}
	members, _ := body["members"].([]any)
	if len(members) != 2 {
		t.Fatalf("members_of: expected 2 members, got %v", body)
	}

	// same_cluster
	status, body = get(fmt.Sprintf("/v1/chains/bitcoin/same-cluster?a=%s&b=%s", a, b))
	if status != http.StatusOK {
		t.Fatalf("same_cluster: expected 200, got %d body=%v", status, body)
	}
	if same, _ := body["same_cluster"].(bool); !same {
		t.Fatalf("same_cluster: expected true, got %v", body)
	}
	if _, ok := body["confidence"]; !ok {
		t.Fatalf("same_cluster: response missing confidence: %v", body)
	}

	// evidence_of via address pair
	status, body = get(fmt.Sprintf("/v1/chains/bitcoin/evidence?address_a=%s&address_b=%s", a, b))
	if status != http.StatusOK {
		t.Fatalf("evidence_of (pair): expected 200, got %d body=%v", status, body)
	}
	evList, _ := body["evidence"].([]any)
	if len(evList) != 1 {
		t.Fatalf("evidence_of (pair): expected 1 row, got %v", body)
	}

	// evidence_of via cluster_id
	status, body = get(fmt.Sprintf("/v1/chains/bitcoin/evidence?cluster_id=%s", realClusterID))
	if status != http.StatusOK {
		t.Fatalf("evidence_of (cluster): expected 200, got %d body=%v", status, body)
	}
	evList, _ = body["evidence"].([]any)
	if len(evList) != 1 {
		t.Fatalf("evidence_of (cluster): expected 1 row, got %v", body)
	}

	// evidence_of with neither param: 400
	status, _ = get("/v1/chains/bitcoin/evidence")
	if status != http.StatusBadRequest {
		t.Fatalf("evidence_of (no params): expected 400, got %d", status)
	}

	// labels_of (cluster)
	status, body = get(fmt.Sprintf("/v1/chains/bitcoin/clusters/%s/labels", clusterID))
	if status != http.StatusOK {
		t.Fatalf("labels_of (cluster): expected 200, got %d body=%v", status, body)
	}
	labels, _ := body["labels"].([]any)
	if len(labels) != 1 {
		t.Fatalf("labels_of (cluster): expected 1 label, got %v", body)
	}

	// hub_status
	status, body = get(fmt.Sprintf("/v1/chains/bitcoin/addresses/%s/hub-status", a))
	if status != http.StatusOK {
		t.Fatalf("hub_status: expected 200, got %d body=%v", status, body)
	}
	if isHub, _ := body["is_hub"].(bool); !isHub {
		t.Fatalf("hub_status: expected is_hub=true, got %v", body)
	}

	// cluster_of for a never-seen address: 404
	status, _ = get("/v1/chains/bitcoin/addresses/" + run + "-never-seen/cluster")
	if status != http.StatusNotFound {
		t.Fatalf("cluster_of (unknown address): expected 404, got %d", status)
	}

	// bad min_confidence: 400
	status, _ = get(fmt.Sprintf("/v1/chains/bitcoin/addresses/%s/cluster?min_confidence=not-a-number", a))
	if status != http.StatusBadRequest {
		t.Fatalf("cluster_of (bad min_confidence): expected 400, got %d", status)
	}
}
