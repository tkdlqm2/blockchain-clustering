// Package domain은 클러스터링 시스템과의 연동 계약(docs/02 §A)이 고정한 출력 스키마를 그대로 표현한다.
package domain

// BalanceDelta는 Kafka topic balance-deltas로 발행되는 원자적 자산 이동 이벤트다 (docs/02 §A.1).
type BalanceDelta struct {
	Type        string         `json:"type"` // "balance_delta" 고정
	ChainID     string         `json:"chain_id"`
	TxID        string         `json:"txid"`
	DeltaIndex  int32          `json:"delta_index"`
	Address     string         `json:"address"`
	Amount      string         `json:"amount"` // 부호 있는 최소단위 정수, 문자열 강제 (docs/02 §A.4)
	Kind        string         `json:"kind"`   // "native" | "token"
	BlockHeight int64          `json:"block_height"`
	BlockHash   string         `json:"block_hash"`
	Meta        map[string]any `json:"meta,omitempty"`
}

// ReorgEvent는 롤백된 블록 해시 목록을 담아 재발행보다 먼저 발행된다 (docs/02 §A.2, docs/03 §3).
type ReorgEvent struct {
	Type                  string   `json:"type"` // "reorg" 고정
	ChainID               string   `json:"chain_id"`
	RolledBackBlockHashes []string `json:"rolled_back_block_hashes"`
}

// meta 컨벤션 키 (docs/02 §A.3).
const (
	MetaContractCreation = "contract_creation"
	MetaTokenContract    = "token_contract"
)

const (
	KindNative = "native"
	KindToken  = "token"
)
