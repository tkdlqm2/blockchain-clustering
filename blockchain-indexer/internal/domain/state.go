package domain

import "time"

// Block은 인덱서가 처리한 블록의 reorg 감지용 내부 상태다 (docs/02 §B.1).
type Block struct {
	ChainID    string
	Height     int64
	Hash       string
	ParentHash string
	Timestamp  time.Time
}

// Cursor는 체인별로 마지막으로 완전히 발행 완료한 높이다 (docs/02 §B.2).
type Cursor struct {
	ChainID string
	Height  int64
}

// ChainConfig는 체인 레지스트리 항목이다 (docs/02 §B.4). ChainID는 클러스터링 레지스트리 등록값과 정확히 일치해야 한다.
type ChainConfig struct {
	ChainID              string
	ModelType            string // "utxo" | "account"
	NodeEndpoint         string
	NodeAuthRef          string // 값이 아니라 시크릿 참조 (docs/06 §7)
	FinalityDepth        int64
	AddressNormalization string // 예: "evm-lowercase", "bitcoin"
	StartHeight          int64
	Enabled              bool
}
