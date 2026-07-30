// Package bitcoin은 UTXO 모델 체인(Bitcoin 등)의 BalanceDelta 추출 어댑터다.
// 추출 알고리즘은 docs/03 §1 (BitcoinAdapter.extract) 의사코드를 그대로 구현한다:
//   - coinbase가 아닌 input마다 prevout 조회 → 지출(-) delta
//   - output마다 주소 추출(scriptPubKey) → 수취(+) delta, 실패 시 "unparsed:<hash>" (docs/03 §4)
//   - prevout 조회는 MVP 결정에 따라 -txindex 노드의 getrawtransaction 사용 (docs/03 §prevout (A))
//     — 자체 prevout 캐시(B)는 채택하지 않았으므로 여기 구현하지 않는다.
package bitcoin

import (
	"context"
	"errors"

	"github.com/powerwaves5307/blockchain-indexer/internal/adapter"
	"github.com/powerwaves5307/blockchain-indexer/internal/domain"
)

type Adapter struct {
	// TODO(M3): -txindex 활성화된 Bitcoin Core RPC 클라이언트 주입 (getrawtransaction으로 prevout 조회)
}

func New() *Adapter {
	return &Adapter{}
}

func (a *Adapter) Extract(ctx context.Context, chainID string, tx adapter.RawTx, block adapter.RawBlock) ([]domain.BalanceDelta, error) {
	// TODO(M3): docs/03 §1 의사코드 구현. coinbase vin skip → prevout 조회 → vout 순회.
	return nil, errors.New("bitcoin adapter: not implemented (see docs/03 §1, milestone M3)")
}
