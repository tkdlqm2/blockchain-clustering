//go:build integration

package ingestor

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/tkdlqm2/blockchain-cluster/internal/address"
	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
	"github.com/tkdlqm2/blockchain-cluster/internal/integrationtest"
)

// Requires: `docker compose up -d` and the "bitcoin" chain registered
// (see README §1) so the balance_delta_bitcoin/address_bitcoin partitions exist.
// Run with: go test -tags=integration ./internal/ingestor/...

func TestIngest_IdempotentAgainstLiveDB(t *testing.T) {
	pool := integrationtest.Pool(t)
	store := NewStore(pool, address.NewStore(pool))
	ctx := context.Background()

	txid := fmt.Sprintf("it-%d", time.Now().UnixNano())
	deltas := []domain.BalanceDelta{
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 0, Address: "addrA", Amount: big.NewInt(-1000), Kind: "native", BlockHeight: 100, BlockHash: "blockhash-1"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 1, Address: "addrB", Amount: big.NewInt(-500), Kind: "native", BlockHeight: 100, BlockHash: "blockhash-1"},
		{ChainID: "bitcoin", TxID: txid, DeltaIndex: 2, Address: "addrC", Amount: big.NewInt(1500), Kind: "native", BlockHeight: 100, BlockHash: "blockhash-1"},
	}

	first, err := store.Ingest(ctx, deltas)
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if first.Inserted != 3 || first.Skipped != 0 {
		t.Fatalf("expected 3 inserted / 0 skipped on first ingest, got %+v", first)
	}

	// FR-3: re-ingesting the identical batch must not change state.
	second, err := store.Ingest(ctx, deltas)
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if second.Inserted != 0 || second.Skipped != 3 {
		t.Fatalf("expected 0 inserted / 3 skipped on re-ingest, got %+v", second)
	}

	got, err := store.GetDeltasByTx(ctx, "bitcoin", txid)
	if err != nil {
		t.Fatalf("get_deltas_by_tx: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 deltas for tx, got %d (duplicate rows would mean idempotency broke)", len(got))
	}
	if got[0].Amount.Cmp(big.NewInt(-1000)) != 0 {
		t.Fatalf("amount round-trip through NUMERIC(78,0) failed: got %s, want -1000", got[0].Amount.String())
	}

	byBlock, err := store.GetDeltasByBlock(ctx, "bitcoin", "blockhash-1")
	if err != nil {
		t.Fatalf("get_deltas_by_block: %v", err)
	}
	found := false
	for _, d := range byBlock {
		if d.TxID == txid {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected tx %s to be found via GetDeltasByBlock", txid)
	}
}

func TestIngestCursor_RoundTrip(t *testing.T) {
	pool := integrationtest.Pool(t)
	store := NewStore(pool, address.NewStore(pool))
	ctx := context.Background()

	source := fmt.Sprintf("it-source-%d", time.Now().UnixNano())

	if _, found, err := store.GetCursor(ctx, "bitcoin", source); err != nil {
		t.Fatalf("get_cursor (before set): %v", err)
	} else if found {
		t.Fatalf("expected no cursor yet for fresh source %s", source)
	}

	if err := store.SetCursor(ctx, "bitcoin", source, "offset-42"); err != nil {
		t.Fatalf("set_cursor: %v", err)
	}
	pos, found, err := store.GetCursor(ctx, "bitcoin", source)
	if err != nil {
		t.Fatalf("get_cursor: %v", err)
	}
	if !found || pos != "offset-42" {
		t.Fatalf("expected offset-42, got found=%v pos=%q", found, pos)
	}

	if err := store.SetCursor(ctx, "bitcoin", source, "offset-99"); err != nil {
		t.Fatalf("set_cursor (update): %v", err)
	}
	pos, _, err = store.GetCursor(ctx, "bitcoin", source)
	if err != nil {
		t.Fatalf("get_cursor (after update): %v", err)
	}
	if pos != "offset-99" {
		t.Fatalf("expected cursor to update to offset-99, got %q", pos)
	}
}
