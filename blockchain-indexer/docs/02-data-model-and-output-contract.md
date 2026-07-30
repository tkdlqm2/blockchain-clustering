# 02. 데이터 모델 & 출력 계약

> 두 부분으로 나뉜다. **§A 출력 계약**은 계약 문서가 고정한 것(바꿀 수 없음)을 구현 관점으로 재기술한다. **§B 내부 상태 모델**은 인덱서가 스스로 관리하는 논리 데이터(스택 비종속)다.

---

## A. 출력 계약 (고정 — 계약 문서 준수)

Kafka topic `balance-deltas`로 발행하는 두 종류 이벤트. **같은 topic, `type`로 구분.**

### A.1 BalanceDelta 이벤트

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

| 필드 | 타입 | 필수 | 인덱서의 이행 의무 |
|---|---|---|---|
| `type` | string | ✅ | `"balance_delta"` 고정 |
| `chain_id` | string | ✅ | 클러스터링 레지스트리 등록값과 **정확 일치**(대소문자 포함) |
| `txid` | string | ✅ | 공통 입력 그룹핑 키. 체인 표준 표기 |
| `delta_index` | int32 | ✅ | **한 txid 내 유일**. 트랜잭션에서 방출하는 모든 delta에 0,1,2… 연속 부여 |
| `address` | string | ✅ | **정규화 후** 값(06 §3). 추출 불가 output 처리 규칙은 06 §3 |
| `amount` | **string** | ✅ | 부호 있는 최소단위 정수. **JSON number 금지**(§강제 제약) |
| `kind` | string | ✅ | `"native"` \| `"token"` |
| `block_height` | int64 | ✅ | 발행 순서·증분 기준 |
| `block_hash` | string | ✅ | **reorg 롤백의 유일 근거.** 누락 시 그 delta의 병합은 영구히 되돌릴 수 없음 |
| `meta` | object | 선택 | 아래 §A.3 컨벤션 |

### A.2 Reorg 이벤트

```json
{
  "type": "reorg",
  "chain_id": "bitcoin",
  "rolled_back_block_hashes": ["00000000...0001", "00000000...0002"]
}
```

- 소비자는 `OnReorg(chain_id, rolled_back_block_hashes)`로 그 해시를 근거로 한 병합만 무효화·재생한다.
- 인덱서는 이 이벤트를 발행한 **뒤** 새 체인 블록을 재발행한다(순서: reorg 통지 → 재발행).

### A.3 `meta` 컨벤션

| 키 | 조건 | 값 | 목적 |
|---|---|---|---|
| `contract_creation` | 계정 체인 컨트랙트 배포 시 | `true` | 소비자 DeployerEngine이 funding이 아니라 deployer 관계로 병합(계약 §5) |
| `token_contract` | `kind == "token"`일 때 | 토큰 컨트랙트 주소(정규화) | **어떤 토큰인지** 식별(계약 §7-2 해소, 06 §2. 소비자 협의 대상) |

> `meta`는 불투명 필드지만, 위 두 키는 소비자와 합의된(또는 합의 제안 중인) 컨벤션이다. 그 외 키는 디버깅용으로 자유롭게 실을 수 있으나 소비자가 해석하지 않는다.

### A.4 강제 제약 — `amount`는 문자열 (재강조)

`NUMERIC(78,0)`/`*big.Int`로 uint256을 무손실 저장하는 소비자에 도달하기 전, **JSON number(IEEE754 double)는 2^53 초과 정수에서 정밀도가 깨진다.** 반드시 `"amount": "123456789012345678901234567890"` 형태의 문자열로 발행한다. 이건 협의 대상이 아니라 절대 제약이다. 직렬화 포맷을 Protobuf/Avro로 바꾸더라도, 큰 정수는 문자열 또는 bytes로 표현한다(06 §1).

---

## B. 내부 상태 모델 (스택 비종속, 인덱서 소유)

인덱서가 reorg 감지·멱등·prevout 조회를 위해 스스로 관리하는 논리 데이터. 물리 스키마·저장소는 구현자 선택(관계형/KV 등).

### B.1 block — 처리한 블록
| 필드 | 의미 | 용도 |
|---|---|---|
| `chain_id` | 체인 | |
| `height` | 높이 | 순서·증분 |
| `hash` | 블록 해시 | reorg 감지 |
| `parent_hash` | 부모 해시 | **reorg 감지의 핵심** |
| `timestamp` | 블록 시각 | 보조 |

- **키**: `(chain_id, hash)`. 조회 인덱스: `(chain_id, height)`.
- **불변식**: 신규 블록 N의 `parent_hash` == 저장된 블록 (N-1)의 `hash` 여야 연속. 아니면 reorg(03 §reorg).

### B.2 cursor — 진행 위치
| 필드 | 의미 |
|---|---|
| `chain_id` | 체인 |
| `height` | 마지막으로 **완전히 발행 완료**한 높이 |

- 발행이 원자적으로 끝난 뒤에만 전진(FR-16). 부분 실패 시 미전진 → 재처리(멱등).

### B.3 prevout_cache — UTXO 이전 출력 (UTXO 체인 전용)
| 필드 | 의미 |
|---|---|
| `chain_id` | 체인 |
| `txid`, `vout` | 이전 출력 식별 |
| `address` | 그 출력의 소유 주소(정규화) |
| `value` | 금액(satoshi, 정수) |
| `block_hash` | 이 출력이 생성된 블록(롤백 대비) |

- **용도**: input(vin)이 참조하는 이전 output의 주소·금액을 O(1)로 조회(03 §prevout).
- **대안**: 노드를 `-txindex`로 운영하면 `getrawtransaction`으로 대체 가능(캐시 불요). 03 §prevout에서 선택.
- **reorg 시**: 롤백된 `block_hash`에 속한 캐시 항목을 함께 되돌린다.

### B.4 chain_config — 체인 설정 (레지스트리)
| 필드 | 의미 | 계약 연계 |
|---|---|---|
| `chain_id` | 체인 식별자 | 소비자 레지스트리와 **정확 일치** 필요(계약 §6) |
| `model_type` | `utxo` \| `account` \| ... | 어떤 추출 어댑터를 쓸지 결정 |
| `node_endpoint` | RPC 주소 | |
| `node_auth` | 인증 정보(참조) | 06 §7 |
| `finality_depth` | 확정 깊이 | reorg 통지 정책(03 §finality) |
| `address_normalization` | 정규화 방식 식별자 | 06 §3 |
| `start_height` | 백필 시작 높이 | |
| `enabled` | 활성화 | |

- 새 체인 추가 = 이 레지스트리에 한 행 + 어댑터 등록(04 §확장).

### B.5 개체 관계 요약
```
chain_config ──▶ (어댑터 선택: model_type)
      │
block(parent_hash) ──▶ ReorgDetector ──▶ reorg 이벤트 발행
      │
raw block ──▶ Extractor(어댑터) ──▶ BalanceDelta ──▶ (정규화·amount 문자열화) ──▶ Kafka 발행
      │                                   ▲
prevout_cache ─────────────────────────────┘ (UTXO input 소유자 해석)
      │
cursor ◀── 발행 완료 후 전진
```
