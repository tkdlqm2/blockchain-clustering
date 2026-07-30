# 08. 인덱서 연동 계약 (Indexer Integration Contract)

> **목적**: 인덱서는 이 시스템과 별개의 프로젝트다(`docs/01` §1.2 비범위: "블록 인덱싱 자체는 별도 인덱서의 책임"). 이 문서는 그 별도 프로젝트를 설계·개발할 때 필요한 데이터 계약을, 지금까지 구현된 클러스터링 시스템(`internal/domain`, `migrations/0001_init.sql`)과 정확히 대조해 한 곳에 모은 것이다. `docs/02` §1·`docs/04` §4·`docs/06` §2.3에 흩어져 있던 논리 요구사항을 실제 코드 기준으로 재정리했다.
>
> **상태**: 아래 §2~§6은 우리 쪽 스키마·코드가 이미 그렇게 고정되어 있어 사실상 확정이다. §7("아직 결정 안 된 것")은 인덱서 팀과 반드시 협의해야 하며, 지금은 우리도 잠정 가정으로만 구현해뒀다.

---

## 1. 전체 관계

```
[블록체인 노드] → [인덱서 (별도 프로젝트)] → Kafka → [이 클러스터링 시스템]
```

인덱서의 책임: 블록체인 노드에서 블록/트랜잭션을 읽어 BalanceDelta로 변환하고, reorg를 감지해 통지하는 것까지. 그 이후(전처리·클러스터링·조회)는 전부 이 시스템의 몫이다.

---

## 2. 전달 방식 (확정)

- **전송**: Kafka
- **Topic**: `${KAFKA_TOPIC_BALANCE_DELTA}` (로컬 기본값 `balance-deltas`, `.env` 참고)
- **직렬화 포맷**: 현재 JSON을 가정하고 구현했다. 실제 Avro/Protobuf 채택 여부는 **미정**(§7-1).
- **이벤트 구분**: BalanceDelta 이벤트와 reorg 이벤트가 **같은 topic**을 공유하며, 메시지의 `type` 필드로 구분한다(인터뷰 결정 사항).

---

## 3. 메시지 스키마

### 3.1 BalanceDelta 이벤트

```json
{
  "type": "balance_delta",
  "chain_id": "bitcoin",
  "txid": "3a7f...",
  "delta_index": 0,
  "address": "bc1q...",
  "amount": "-100000000",
  "kind": "native",
  "block_height": 820123,
  "block_hash": "00000000000000000001...",
  "meta": {}
}
```

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| `type` | string | ✅ | `"balance_delta"` 고정 (reorg 이벤트와 구분) |
| `chain_id` | string | ✅ | `clustering.chain`에 미리 등록된 값과 정확히 일치해야 함(§6) |
| `txid` | string | ✅ | 트랜잭션 식별자. 공통 입력 그룹핑의 키 |
| `delta_index` | int32 | ✅ | **한 txid 내에서 유일**해야 함 — `(chain_id, txid, delta_index)`가 물리 PK(멱등성의 근거, FR-3) |
| `address` | string | ✅ | 아래 §7-4(정규화) 참고 |
| `amount` | **string** | ✅ | 부호 있는 정수. **JSON number 금지, 반드시 문자열**(§4 참고) |
| `kind` | string | ✅ | `"native"` \| `"token"` — 그 이상의 토큰 식별자는 없음(§7-3 열린 문제) |
| `block_height` | int64 | ✅ | 증분·정렬 기준 |
| `block_hash` | string | ✅ | **reorg 롤백의 유일한 근거.** 이게 없으면 그 delta로 만들어진 병합은 나중에 절대 되돌릴 수 없다 |
| `meta` | object (JSONB) | 선택 | 불투명 필드. 현재 이 시스템이 실제로 해석하는 유일한 키는 `contract_creation`(§5) |

### 3.2 Reorg 이벤트

```json
{
  "type": "reorg",
  "chain_id": "bitcoin",
  "rolled_back_block_hashes": ["00000000000000000001...", "00000000000000000002..."]
}
```

수신 측 처리: `reorg.Handler.OnReorg(ctx, chain_id, rolled_back_block_hashes)` — 그 block_hash를 근거로 삼은 병합만 무효화되고 재생된다(`docs/03` §9).

---

## 4. `amount`는 반드시 문자열로 인코딩할 것 (가장 중요한 제약)

이 시스템은 `NUMERIC(78,0)`과 Go `*big.Int`로 amount를 다뤄서 uint256까지 **무손실**로 저장한다(`docs/06` §4, `internal/pgnumeric`). 그런데 **JSON의 number 타입은 IEEE754 double이라 2^53을 넘는 정수부터 정밀도가 깨진다.** 만약 인덱서가 `"amount": 123456789012345678901234567890` 처럼 JSON 숫자 리터럴로 큰 값을 보내면, 우리 쪽 파서에 도달하기 **전에** 이미 값이 손상된다.

→ **`amount`는 항상 큰따옴표로 감싼 문자열**(`"amount": "123456789012345678901234567890"`)로 보내야 한다. 이건 협의 대상이 아니라 반드시 지켜야 하는 제약이다.

---

## 5. `meta.contract_creation` 컨벤션 (계정 체인 전용, 잠정)

계정 체인(이더리움류)의 **컨트랙트 배포** 이벤트를 일반 EOA 자금 이체와 구분하기 위한 컨벤션이다. BalanceDelta 자체에는 "이 tx가 컨트랙트 배포다"를 나타낼 필드가 원래 없어서(§1의 논리 스키마엔 native/token kind뿐), 받는 쪽(컨트랙트) delta의 `meta`에 다음을 실어 보내주면 `DeployerEngine`이 이를 소비해 funding이 아니라 deployer 관계로 병합한다:

```json
"meta": { "contract_creation": true }
```

이 플래그가 없으면 새로 생긴 주소는 전부 일반 funding(자금 출처)으로 처리된다. **이건 우리 쪽에서 임시로 정한 컨벤션**이라, 인덱서 팀과 실제로 이 방식을 쓸지, 아니면 다른 방식(예: 별도 `kind: "contract_creation"`)으로 바꿀지 협의가 필요하다.

---

## 6. 체인 등록 시 함께 확정해야 할 값

새 체인을 이 시스템에 등록하는 것은 우리 쪽 DB 작업(`SELECT clustering.add_chain(...)`)이지만, 아래 값은 인덱서 쪽 정보와 반드시 일치해야 한다:

| 파라미터 | 예시 | 의미 |
|---|---|---|
| `chain_id` | `'bitcoin'`, `'ethereum'` | 인덱서가 보내는 메시지의 `chain_id`와 **정확히 일치**해야 함(대소문자 포함) |
| `model_type` | `'utxo'` \| `'account'` | 어떤 휴리스틱 세트가 자동으로 켜질지 결정(`docs/06` §2.1) |
| `native_symbol`, `native_decimals` | `'BTC'`, `8` | 표시용. 정밀도 계산엔 `amount`가 이미 최소 단위(satoshi/wei) 정수라고 가정하므로 영향 없음 |
| `finality_depth` | `6`, `12` | 이 체인에서 몇 블록 뒤를 "확정"으로 볼지 — reorg 통지 정책과 연관 |
| `address_normalization` | `'bitcoin'`, `'evm-lowercase'` | §7-4 참고 |

---

## 7. 아직 결정 안 된 것 (인덱서 팀과 반드시 협의 필요)

1. **실제 직렬화 포맷**: JSON으로 계속 갈지, Avro/Protobuf + 스키마 레지스트리로 바꿀지. 지금 우리 쪽 구현(역직렬화 코드)은 존재하지 않는다 — Kafka 컨슈머 자체를 아직 안 만들었기 때문에(TODO) 포맷이 뭐든 아직 되돌리기 쉬운 시점이다.
2. **토큰 식별자 부재**: `kind: "token"`만으로는 **어떤 토큰인지** 구분이 안 된다. 한 주소가 한 tx에서 USDC와 USDT를 동시에 주고받으면 두 delta를 구별할 방법이 없다. `meta`에 토큰 컨트랙트 주소를 싣는 컨벤션이 필요해 보이는데, 아직 정해진 게 없다.
3. **주소 정규화 책임 소재**: `docs/06` §2.3은 "정규화는 우리 쪽 어댑터(`normalizeAddress`)의 책임"이라고 설계해뒀지만, **그 어댑터를 아직 구현하지 않았다.** 지금 `Ingestor.Ingest()`는 받은 `address` 문자열을 그대로 저장한다. 따라서 당장은 **인덱서가 이미 정규화된 형태**(EVM은 소문자, Bitcoin은 표준 인코딩)로 보내줘야 정합성이 깨지지 않는다. 이 책임을 인덱서 쪽에 둘지, 우리가 어댑터를 마저 구현할지 결정 필요.
4. **순서 보장/재정렬 책임**: `docs/04` §4는 "블록 높이 오름차순 권장, 순서가 흐트러지면 Ingestor가 재정렬 버퍼를 둔다"고 되어 있지만, **그 재정렬 버퍼도 아직 구현 안 했다.** 지금은 인덱서가 순서를 지켜서 보내준다는 전제로만 동작한다.
5. **reorg 자체 감지 폴백**: 인덱서가 reorg 통지를 못 주는 경우를 대비한 "저장된 block_hash와 최신 체인 대조" 폴백(`docs/04` §4)도 미구현이다. 지금은 인덱서의 reorg 통지에 전적으로 의존한다.
6. **배치/파티셔닝 전략**: Kafka 파티션 키를 뭘로 할지(`chain_id`? `txid`?), 배치 크기, 백필(과거 블록 일괄 적재) 시의 처리 방식.
7. **인증/네트워크 경계**: 인덱서 → Kafka, 우리 시스템 → Kafka 사이의 인증(SASL 등) — 지금 로컬 docker-compose는 Kafka 인증이 아예 없다(`ALLOW_PLAINTEXT_LISTENER`).

---

## 8. 원본 참고 문서

- `docs/01-functional-spec.md` §1.2 (비범위 — 인덱서는 별도 책임)
- `docs/02-data-model.md` §1 (BalanceDelta 논리 필드)
- `docs/04-architecture-and-interfaces.md` §4 (인덱서 연동 계약, 순서·reorg 통지)
- `docs/06-multichain-extensibility.md` §2.3 (어댑터 책임 — 정규화)
- `internal/domain/types.go` (`BalanceDelta` 실제 Go 구현)
- `migrations/0001_init.sql` (`balance_delta`, `chain` 테이블 물리 스키마)
