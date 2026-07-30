package consumer

import (
	"github.com/segmentio/kafka-go"

	"github.com/tkdlqm2/blockchain-cluster/internal/domain"
)

// Batcher groups incoming BalanceDeltas into per-block batches — the
// indexer publishes "블록 단위로 원자적" (docs/06-contract-decisions.md §6
// on the indexer side), so grouping by block_hash is the natural unit for
// one pipeline.Run() call, rather than running the pipeline per message.
//
// It tracks the raw kafka.Message alongside each delta so the caller can
// commit Kafka offsets for exactly (and only) the messages that ended up in
// a batch that was successfully run through the pipeline — never before.
// Committing a message's offset before its data is durably processed would
// mean a crash between commit and processing loses that delta forever,
// since Kafka won't redeliver an already-committed offset.
type Batcher struct {
	chainID   string
	blockHash string
	deltas    []domain.BalanceDelta
	messages  []kafka.Message
}

func NewBatcher() *Batcher {
	return &Batcher{}
}

// Add appends one delta and its source message to the in-progress batch.
// If d belongs to a different (chain, block) than what's currently
// accumulating, the previous batch is returned for the caller to flush
// before this call returns — d itself always starts (or continues) the new
// pending batch, it is never part of the flushed one.
func (b *Batcher) Add(d domain.BalanceDelta, m kafka.Message) (flushDeltas []domain.BalanceDelta, flushMessages []kafka.Message, flushChainID string, ready bool) {
	if b.blockHash != "" && (d.BlockHash != b.blockHash || d.ChainID != b.chainID) {
		flushDeltas, flushMessages, flushChainID, ready = b.deltas, b.messages, b.chainID, true
		b.deltas, b.messages = nil, nil
	}
	b.chainID = d.ChainID
	b.blockHash = d.BlockHash
	b.deltas = append(b.deltas, d)
	b.messages = append(b.messages, m)
	return flushDeltas, flushMessages, flushChainID, ready
}

// Flush forcibly drains whatever's pending — used on a reorg boundary,
// shutdown, or the periodic safety-net timer (so a batch doesn't sit
// unprocessed indefinitely just because no next block has arrived yet).
func (b *Batcher) Flush() (deltas []domain.BalanceDelta, messages []kafka.Message, chainID string) {
	deltas, messages, chainID = b.deltas, b.messages, b.chainID
	b.deltas, b.messages = nil, nil
	b.blockHash = ""
	return deltas, messages, chainID
}
