// Package adapter는 체인별 BalanceDelta 추출 어댑터의 공통 인터페이스를 정의한다 (docs/04 어댑터 패턴 — 원본 문서 미수령, docs/03 의사코드 기준으로 정의).
package adapter

import (
	"context"

	"github.com/powerwaves5307/blockchain-indexer/internal/domain"
)

// RawTx는 어댑터가 필요로 하는 최소한의 트랜잭션 표현이다. 실제 필드는 체인별 구현에서 확장한다.
type RawTx any

// RawBlock은 어댑터가 필요로 하는 최소한의 블록 표현이다.
type RawBlock struct {
	Height     int64
	Hash       string
	ParentHash string
}

// Adapter는 체인의 model_type(utxo/account)별 BalanceDelta 추출 로직을 캡슐화한다.
// 새 체인 추가는 이 인터페이스 구현 + chain_config 등록만으로 끝나야 한다 (NFR-6, docs/02 §B.5).
type Adapter interface {
	// Extract는 하나의 트랜잭션에서 발생하는 모든 BalanceDelta를 결정적 순서(delta_index 0..n)로 반환한다.
	// ctx는 종료 신호(SIGTERM 등)에 맞춰 내부 RPC 호출(예: receipt 조회)을 즉시 취소하기 위함이다.
	Extract(ctx context.Context, chainID string, tx RawTx, block RawBlock) ([]domain.BalanceDelta, error)
}
