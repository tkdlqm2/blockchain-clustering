// Package ethereum은 계정 모델 체인(Ethereum 등)의 노드 접근(M1) + BalanceDelta 추출(M2) 어댑터다.
// 추출 알고리즘은 docs/03 §2 (EthereumAdapter.extract) 의사코드를 구현한다:
//   - native 전송(tx.value>0) → from(-)/to(+) native delta
//   - receipt.status==0(revert) → delta 미생성 (FR-5)
//   - to==null && receipt.contractAddress!=null → meta.contract_creation:true (FR-6)
//   - ERC-20 Transfer 로그(topics[0]==keccak256("Transfer(address,address,uint256)"), topics 길이 3)
//     → from(-)/to(+) token delta, meta.token_contract 채움 (FR-7)
//   - 주소는 발행 전 EVM 소문자 정규화 (docs/03 §4)
package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/powerwaves5307/blockchain-indexer/internal/adapter"
	"github.com/powerwaves5307/blockchain-indexer/internal/domain"
)

var transferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

type Adapter struct {
	client  *ethclient.Client
	chainID *big.Int
}

// New는 노드에 접속해 네트워크의 실제 chain id를 조회해둔다. signer 생성에 tx.ChainId()를
// 쓰면 EIP-155 이전 legacy tx(chainId==0)에서 go-ethereum이 panic을 일으키므로,
// 항상 네트워크의 실제 chain id로 signer를 구성한다 (Extract 참고).
func New(ctx context.Context, rpcURL string) (*Adapter, error) {
	client, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("ethereum rpc dial 실패: %w", err)
	}
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("chain id 조회 실패: %w", err)
	}
	return &Adapter{client: client, chainID: chainID}, nil
}

// LatestHeight는 docs/03 §0 node.getLatestHeight에 해당한다.
func (a *Adapter) LatestHeight(ctx context.Context) (int64, error) {
	height, err := a.client.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("최신 높이 조회 실패: %w", err)
	}
	return int64(height), nil
}

// FetchBlock은 docs/03 §0 node.getBlock에 해당한다. 트랜잭션 순회·추출은 호출자(메인 루프)가
// 이 반환값의 Transactions()를 Extract에 넘기는 방식으로 담당한다.
func (a *Adapter) FetchBlock(ctx context.Context, height int64) (*types.Block, error) {
	block, err := a.client.BlockByNumber(ctx, big.NewInt(height))
	if err != nil {
		return nil, fmt.Errorf("블록 조회 실패(height=%d): %w", height, err)
	}
	return block, nil
}

// RawBlock은 go-ethereum의 *types.Block을 어댑터 공통 표현(adapter.RawBlock)으로 변환한다.
func (a *Adapter) RawBlock(block *types.Block) adapter.RawBlock {
	return adapter.RawBlock{
		Height:     int64(block.NumberU64()),
		Hash:       block.Hash().Hex(),
		ParentHash: block.ParentHash().Hex(),
	}
}

// BlockHash는 지정 높이 블록의 해시만 가볍게 조회한다(전체 블록 본문 없이 헤더만) —
// reorg 공통 조상 탐색에서 여러 높이를 훑을 때 사용한다 (internal/reorg 참고).
func (a *Adapter) BlockHash(ctx context.Context, height int64) (string, error) {
	header, err := a.client.HeaderByNumber(ctx, big.NewInt(height))
	if err != nil {
		return "", fmt.Errorf("헤더 조회 실패(height=%d): %w", height, err)
	}
	return header.Hash().Hex(), nil
}

// Extract는 docs/03 §2 의사코드를 구현한다. rawTx는 *types.Transaction이어야 한다.
// receipt 조회(I/O)만 여기서 하고, 실제 추출 로직은 순수 함수 extractDeltas에 위임한다
// (네트워크 없이 단위 테스트하기 위한 분리 — ethereum_test.go 참고).
func (a *Adapter) Extract(ctx context.Context, chainID string, rawTx adapter.RawTx, block adapter.RawBlock) ([]domain.BalanceDelta, error) {
	tx, ok := rawTx.(*types.Transaction)
	if !ok {
		return nil, fmt.Errorf("ethereum adapter: 예상치 못한 tx 타입 %T", rawTx)
	}

	receipt, err := a.client.TransactionReceipt(ctx, tx.Hash())
	if err != nil {
		return nil, fmt.Errorf("receipt 조회 실패(tx=%s): %w", tx.Hash().Hex(), err)
	}

	return extractDeltas(chainID, tx, receipt, a.chainID, block)
}

// extractDeltas는 docs/03 §2 의사코드 본체다. 네트워크 I/O가 전혀 없어 합성 픽스처로
// 단위 테스트 가능하다(docs/05 테스트 매트릭스 T-2,7,8,9,10,11,12 대응).
func extractDeltas(chainID string, tx *types.Transaction, receipt *types.Receipt, networkChainID *big.Int, block adapter.RawBlock) ([]domain.BalanceDelta, error) {
	if receipt.Status == types.ReceiptStatusFailed {
		return nil, nil // revert → delta 없음 (FR-5, T-8)
	}

	from, err := types.Sender(types.LatestSignerForChainID(networkChainID), tx)
	if err != nil {
		return nil, fmt.Errorf("from 주소 복구 실패(tx=%s): %w", tx.Hash().Hex(), err)
	}

	var deltas []domain.BalanceDelta
	di := int32(0)
	txHash := tx.Hash().Hex()

	// (1) native 전송의 지출 측
	if tx.Value().Sign() > 0 {
		deltas = append(deltas, newDelta(chainID, txHash, di, from.Hex(), negString(tx.Value()), domain.KindNative, block, nil))
		di++
	}

	// (2) native 전송의 수취 측 / 컨트랙트 배포 (FR-6, T-11)
	//
	// docs/03 §2 의사코드는 "(1) native 전송"과 "(2) 컨트랙트 배포"를 독립된 블록으로 나누지만,
	// tx.to==nil(배포 tx)인 경우 (1)의 "target = tx.to" 규칙을 그대로 따르면 target이 null이 되어
	// 잘못된 delta(빈 주소)가 나온다. 여기서는 수취 측을 tx.to 유무로 분기해, 배포 tx는 무조건
	// receipt.contractAddress를 대상으로 하나의 delta만 내고 contract_creation 메타를 붙인다
	// (동일한 자산 흐름을 한 번만, 정확한 주소로 표현).
	switch {
	case tx.To() != nil:
		if tx.Value().Sign() > 0 {
			deltas = append(deltas, newDelta(chainID, txHash, di, tx.To().Hex(), posString(tx.Value()), domain.KindNative, block, nil))
			di++
		}
	case receipt.ContractAddress != (common.Address{}):
		deltas = append(deltas, newDelta(chainID, txHash, di, receipt.ContractAddress.Hex(), posString(tx.Value()), domain.KindNative, block,
			map[string]any{domain.MetaContractCreation: true}))
		di++
	}

	// (3) ERC-20 Transfer 로그 (FR-7, T-9, T-10)
	for _, l := range receipt.Logs {
		if len(l.Topics) != 3 || l.Topics[0] != transferTopic {
			continue // topics 4개(ERC-721 등)·Transfer 아닌 이벤트 제외
		}
		fromAddr := common.HexToAddress(l.Topics[1].Hex())
		toAddr := common.HexToAddress(l.Topics[2].Hex())
		value := new(big.Int).SetBytes(l.Data)
		token := l.Address.Hex()

		deltas = append(deltas, newDelta(chainID, txHash, di, fromAddr.Hex(), "-"+value.String(), domain.KindToken, block,
			map[string]any{domain.MetaTokenContract: normalize(token)}))
		di++
		deltas = append(deltas, newDelta(chainID, txHash, di, toAddr.Hex(), value.String(), domain.KindToken, block,
			map[string]any{domain.MetaTokenContract: normalize(token)}))
		di++
	}

	return deltas, nil
}

func newDelta(chainID, txID string, di int32, address, amount, kind string, block adapter.RawBlock, meta map[string]any) domain.BalanceDelta {
	return domain.BalanceDelta{
		ChainID:     chainID,
		TxID:        txID,
		DeltaIndex:  di,
		Address:     normalize(address),
		Amount:      amount,
		Kind:        kind,
		BlockHeight: block.Height,
		BlockHash:   block.Hash,
		Meta:        meta,
	}
}

// normalize는 EVM 주소를 소문자로 통일한다 (docs/03 §4, docs/06 §3).
func normalize(addr string) string {
	return strings.ToLower(addr)
}

func negString(v *big.Int) string {
	return new(big.Int).Neg(v).String()
}

func posString(v *big.Int) string {
	return v.String()
}
