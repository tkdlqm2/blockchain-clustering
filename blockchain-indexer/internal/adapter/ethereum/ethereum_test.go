package ethereum

import (
	"crypto/ecdsa"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/powerwaves5307/blockchain-indexer/internal/adapter"
	"github.com/powerwaves5307/blockchain-indexer/internal/domain"
)

var testChainID = big.NewInt(1)

var testBlock = adapter.RawBlock{
	Height:     100,
	Hash:       "0xblockhash",
	ParentHash: "0xparenthash",
}

func mustKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("키 생성 실패: %v", err)
	}
	return key
}

// signedTx는 지정된 to/value로 서명된 LegacyTx를 만든다. to==nil이면 컨트랙트 배포 tx가 된다.
func signedTx(t *testing.T, key *ecdsa.PrivateKey, to *common.Address, value *big.Int) *types.Transaction {
	t.Helper()
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		GasPrice: big.NewInt(1),
		Gas:      21000,
		To:       to,
		Value:    value,
	})
	signer := types.LatestSignerForChainID(testChainID)
	signed, err := types.SignTx(tx, signer, key)
	if err != nil {
		t.Fatalf("서명 실패: %v", err)
	}
	return signed
}

func successReceipt() *types.Receipt {
	return &types.Receipt{Status: types.ReceiptStatusSuccessful}
}

// transferLog는 ERC-20 Transfer(from, to, value) 로그를 만든다.
func transferLog(token, from, to common.Address, value *big.Int) *types.Log {
	valBytes := make([]byte, 32)
	value.FillBytes(valBytes)
	return &types.Log{
		Address: token,
		Topics: []common.Hash{
			transferTopic,
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
		},
		Data: valBytes,
	}
}

// T-7: value>0 native 전송 → from(-)/to(+) 델타.
func TestExtractDeltas_NativeTransfer(t *testing.T) {
	key := mustKey(t)
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x00000000000000000000000000000000000AAA")
	value := big.NewInt(1_000_000_000_000_000_000)

	tx := signedTx(t, key, &to, value)
	deltas, err := extractDeltas("ethereum", tx, successReceipt(), testChainID, testBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(deltas) != 2 {
		t.Fatalf("delta 개수 = %d, want 2", len(deltas))
	}
	if deltas[0].DeltaIndex != 0 || deltas[1].DeltaIndex != 1 {
		t.Fatalf("delta_index 연속이 아님: %d, %d", deltas[0].DeltaIndex, deltas[1].DeltaIndex)
	}
	if deltas[0].Address != strings.ToLower(from.Hex()) || deltas[0].Amount != "-1000000000000000000" {
		t.Fatalf("지출 delta 불일치: %+v", deltas[0])
	}
	if deltas[1].Address != strings.ToLower(to.Hex()) || deltas[1].Amount != "1000000000000000000" {
		t.Fatalf("수취 delta 불일치: %+v", deltas[1])
	}
	if deltas[0].Kind != domain.KindNative || deltas[1].Kind != domain.KindNative {
		t.Fatalf("kind가 native가 아님: %+v %+v", deltas[0], deltas[1])
	}
}

// T-8: receipt.status==0(revert) → delta 0건.
func TestExtractDeltas_RevertedTxProducesNoDeltas(t *testing.T) {
	key := mustKey(t)
	to := common.HexToAddress("0x00000000000000000000000000000000000AAA")
	tx := signedTx(t, key, &to, big.NewInt(1))

	receipt := &types.Receipt{Status: types.ReceiptStatusFailed}
	deltas, err := extractDeltas("ethereum", tx, receipt, testChainID, testBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(deltas) != 0 {
		t.Fatalf("revert tx인데 delta %d건 발행됨", len(deltas))
	}
}

// T-11: to==nil && contractAddress!=zero → contract_creation 메타가 붙은 delta 1건.
func TestExtractDeltas_ContractCreation(t *testing.T) {
	key := mustKey(t)
	tx := signedTx(t, key, nil, big.NewInt(0))

	contractAddr := common.HexToAddress("0x00000000000000000000000000000000000CCC")
	receipt := &types.Receipt{Status: types.ReceiptStatusSuccessful, ContractAddress: contractAddr}

	deltas, err := extractDeltas("ethereum", tx, receipt, testChainID, testBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(deltas) != 1 {
		t.Fatalf("delta 개수 = %d, want 1", len(deltas))
	}
	d := deltas[0]
	if d.Address != strings.ToLower(contractAddr.Hex()) {
		t.Fatalf("컨트랙트 주소 불일치: %s", d.Address)
	}
	created, ok := d.Meta[domain.MetaContractCreation].(bool)
	if !ok || !created {
		t.Fatalf("meta.contract_creation 누락 또는 false: %+v", d.Meta)
	}
}

// T-9, T-2: ERC-20 Transfer 로그 → from(-)/to(+) token delta, meta.token_contract,
// 2^53을 초과하는 큰 값도 문자열로 무손실 전달.
func TestExtractDeltas_ERC20TransferWithLargeValue(t *testing.T) {
	key := mustKey(t)
	to := common.HexToAddress("0x00000000000000000000000000000000000AAA")
	tx := signedTx(t, key, &to, big.NewInt(0)) // value=0인 컨트랙트 호출(예: transfer())

	token := common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111a")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222b")

	// 2^53(9007199254740992)을 훌쩍 넘는 값 — JSON number였다면 정밀도가 깨진다.
	bigValue, ok := new(big.Int).SetString("123456789012345678901234567890", 10)
	if !ok {
		t.Fatal("빅 넘버 파싱 실패")
	}

	receipt := &types.Receipt{
		Status: types.ReceiptStatusSuccessful,
		Logs:   []*types.Log{transferLog(token, fromAddr, toAddr, bigValue)},
	}

	deltas, err := extractDeltas("ethereum", tx, receipt, testChainID, testBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(deltas) != 2 {
		t.Fatalf("delta 개수 = %d, want 2 (native 없음, token만)", len(deltas))
	}
	if deltas[0].Amount != "-123456789012345678901234567890" {
		t.Fatalf("큰 음수 amount 손실: %s", deltas[0].Amount)
	}
	if deltas[1].Amount != "123456789012345678901234567890" {
		t.Fatalf("큰 양수 amount 손실: %s", deltas[1].Amount)
	}
	for _, d := range deltas {
		if d.Kind != domain.KindToken {
			t.Fatalf("kind가 token이 아님: %+v", d)
		}
		tc, ok := d.Meta[domain.MetaTokenContract].(string)
		if !ok || tc != strings.ToLower(token.Hex()) {
			t.Fatalf("meta.token_contract 불일치: %+v", d.Meta)
		}
	}
}

// T-10: topics 4개(ERC-721 등)는 Transfer로 취급하지 않고 제외한다.
func TestExtractDeltas_ExcludesNonERC20TransferLogs(t *testing.T) {
	key := mustKey(t)
	to := common.HexToAddress("0x00000000000000000000000000000000000AAA")
	tx := signedTx(t, key, &to, big.NewInt(0))

	token := common.HexToAddress("0x00000000000000000000000000000000000BEE")
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111a")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222b")
	tokenIDTopic := common.BigToHash(big.NewInt(42))

	nftLog := &types.Log{
		Address: token,
		Topics: []common.Hash{
			transferTopic,
			common.BytesToHash(fromAddr.Bytes()),
			common.BytesToHash(toAddr.Bytes()),
			tokenIDTopic, // ERC-721은 topics가 4개(tokenId도 indexed)
		},
	}
	receipt := &types.Receipt{Status: types.ReceiptStatusSuccessful, Logs: []*types.Log{nftLog}}

	deltas, err := extractDeltas("ethereum", tx, receipt, testChainID, testBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(deltas) != 0 {
		t.Fatalf("ERC-721(4 topics) 로그가 delta로 발행됨: %+v", deltas)
	}
}

// T-12: 체크섬 대소문자 섞인 주소도 소문자로 정규화되어 발행된다.
func TestExtractDeltas_AddressNormalizedToLowercase(t *testing.T) {
	key := mustKey(t)
	to := common.HexToAddress("0x00000000000000000000000000000000000AAA") // Hex()는 체크섬 대소문자 반환
	tx := signedTx(t, key, &to, big.NewInt(1))

	deltas, err := extractDeltas("ethereum", tx, successReceipt(), testChainID, testBlock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, d := range deltas {
		if d.Address != strings.ToLower(d.Address) {
			t.Fatalf("주소가 소문자로 정규화되지 않음: %s", d.Address)
		}
	}
}

// T-1: 계약 스키마 — amount가 JSON에서 문자열(숫자 아님)로 직렬화되는지 확인.
func TestBalanceDelta_AmountIsJSONString(t *testing.T) {
	d := domain.BalanceDelta{
		Type: "balance_delta", ChainID: "ethereum", TxID: "0xabc", DeltaIndex: 0,
		Address: "0x00", Amount: "123456789012345678901234567890", Kind: domain.KindNative,
		BlockHeight: 1, BlockHash: "0xh",
	}
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal 실패: %v", err)
	}
	if !strings.Contains(string(raw), `"amount":"123456789012345678901234567890"`) {
		t.Fatalf("amount가 문자열로 직렬화되지 않음: %s", raw)
	}
}
