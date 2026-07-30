# 06. 다체인 확장성 설계 (Multi-chain Extensibility)

> **목표**: 새 블록체인을 추가하는 작업이 **"설정(레지스트리) + 어댑터 + 파티션 생성"** 으로 끝나야 한다. 코어 테이블·병합 엔진·재생 로직·조회 API는 **절대 변경하지 않는다.** 이 문서는 그 구조를 규정하고, 물리 실현은 `07-postgres-schema.sql`이 담당한다.

---

## 1. 확장의 3가지 변이 축

체인마다 달라지는 것은 사실상 세 가지뿐이다. 이 세 축만 데이터/설정으로 흡수하면 나머지는 공통이다.

1. **데이터 모델 타입**: `utxo`(비트코인류) vs `account`(이더리움류) vs 향후 타입. → 어떤 휴리스틱이 적용되는지를 결정.
2. **주소 형식·정규화 규칙**: EVM 소문자화, Bitcoin bech32/base58, 기타 체인별 규칙.
3. **적용 휴리스틱과 파라미터**: 같은 UTXO라도 체인별 임계치(dust 기준, 수수료 수준)가 다를 수 있음.

이 세 축을 **레지스트리 테이블**로 밀어넣고, 코어는 "레지스트리를 읽어 동작하는 제네릭 엔진"으로 만든다.

---

## 2. 레지스트리 기반 설계

### 2.1 chain 레지스트리
각 체인을 한 행으로 선언한다(07 `chain` 테이블). 담는 정보:
- `chain_id` (예 'bitcoin', 'ethereum', 'solana')
- `model_type` ('utxo' | 'account' | ...) — 휴리스틱 선택의 기준
- `native_symbol`, `native_decimals`
- `finality_depth`
- `address_normalization` (어댑터가 참조할 정규화 방식 식별자)
- `config` (JSONB — 체인별 자유 설정)
- `enabled`

**새 체인 = 이 테이블에 INSERT + 파티션 생성.** 코어 스키마 변경 없음.

### 2.2 heuristic 레지스트리 + chain_heuristic 매핑
- `heuristic` 테이블: 휴리스틱 카탈로그(`common-input`, `sweep-seed`, `change`, `funding`, `deployer`, `behavioral`, `manual`). 각 휴리스틱이 어떤 `model_type`에 적용되는지(`applies_to`), 기본 confidence.
- `chain_heuristic` 테이블: `(chain_id, heuristic_key)` 매핑. 체인별로 **활성화 여부·confidence 오버라이드·파라미터(JSONB)** 를 둔다.

이 구조 덕분에:
- 새 UTXO 체인을 추가하면, `model_type='utxo'`에 적용되는 휴리스틱들을 `chain_heuristic`에 몇 줄 넣는 것으로 끝난다.
- 특정 체인만 dust 임계치를 다르게 주고 싶으면 그 체인의 `chain_heuristic.params`만 바꾼다. 코드 불변.

### 2.3 어댑터(코드 측) — 최소 책임
클러스터링 시스템의 체인 어댑터는 인덱서 어댑터와 달리 무겁지 않다. BalanceDelta 정규화는 이미 인덱서가 했기 때문이다. 클러스터링 어댑터의 책임은:
- **주소 정규화**: `normalizeAddress(chain, raw) → canonical`
- **모델 타입 제공**: 레지스트리에서 읽거나 어댑터가 선언
- **체인별 시드/허브 힌트 제공**(선택): known-deposit 시드, 허브 라벨 등

어댑터는 `04-architecture` §5의 확장 포인트를 따른다. 새 체인 어댑터는 이 인터페이스만 구현하면 된다.

---

## 3. 코어 불변 원칙 (체인 무관)

다음은 어떤 체인을 추가해도 **바뀌지 않는다.**

- `merge_evidence`(진실의 원천), 재생 로직, Union-Find — 모두 `chain_id`를 discriminator로만 사용하고 체인 종류를 모른다.
- `cluster`, `cluster_membership`, `label`, `audit_log` — 제네릭.
- 병합 엔진의 "근거 append + 재생" 메커니즘 — 체인 불문 동일.
- reorg 롤백(근거 무효화 + 재생) — 체인 불문 동일.

즉 **체인 종류를 아는 곳은 "휴리스틱 엔진"과 "어댑터"뿐**이고, 그마저도 `model_type` 단위로 일반화되어 있어 개별 체인을 하드코딩하지 않는다.

---

## 4. 파티셔닝 전략 (PostgreSQL)

대용량 테이블은 `chain_id` 기준 **LIST 파티셔닝**한다(07 참조). 효과:
- **격리**: 한 체인의 대용량 쓰기/재생이 다른 체인 조회에 주는 영향 최소화.
- **확장 절차의 단순함**: 새 체인 추가 = `CREATE TABLE ... PARTITION OF ... FOR VALUES IN ('newchain')`. 스키마 변경이 아니라 파티션 추가.
- **운영**: 체인 단위로 VACUUM·백업·아카이빙·삭제(체인 지원 중단 시 파티션 DETACH) 가능.

파티셔닝 대상: `balance_delta`, `address`, `merge_evidence`, `cluster`, `cluster_membership`. 소형 레지스트리/라벨 테이블은 파티션하지 않는다.

> **PostgreSQL 이점 활용**: `NUMERIC(78,0)`으로 uint256을 손실 없이 저장할 수 있고(MySQL의 65자리 한계 문제 없음), 부분 인덱스(`WHERE status='active'`)와 부분 유니크 인덱스(native 자산 `contract IS NULL`)를 그대로 쓸 수 있어 논리 모델(02)을 왜곡 없이 실현한다.

---

## 5. 크로스체인 엔티티 (super-entity)

같은 주체가 여러 체인에 주소를 갖는 경우(예: 한 거래소가 Bitcoin·Ethereum 양쪽에 지갑 보유)를 다룬다. 기본 클러스터는 체인 내부에서만 형성되므로, 그 위에 **상위 엔티티 링크 계층**을 둔다.

- `super_entity`: 체인을 가로지르는 논리 주체(예 "Binance").
- `super_entity_member`: `(super_entity_id, chain_id, cluster_id)` — 각 체인의 클러스터를 상위 엔티티에 링크.

이 계층은 클러스터링 코어와 분리되어 있어, 크로스체인 기능을 나중에 켜도 코어에 영향이 없다. 링크의 근거(왜 이 두 체인 클러스터가 같은 주체인가)는 라벨 일치·공개 공시·운영자 확정 등으로 부여하고 confidence를 동반한다.

---

## 6. 새 체인 추가 절차 (체크리스트)

새 체인 `X`를 추가할 때 수행하는 전부:

1. **레지스트리 등록**: `chain`에 `X` INSERT (`model_type`, `native_*`, `finality_depth`, `address_normalization`, `config`).
2. **휴리스틱 매핑**: `chain_heuristic`에 `X`의 `model_type`에 해당하는 휴리스틱들을 활성화(필요 시 confidence·params 오버라이드).
3. **파티션 생성**: 파티션 대상 테이블 각각에 `X` 파티션 생성(07의 `add_chain_partitions('X')` 헬퍼 참고).
4. **어댑터 구현/등록**(코드): 주소 정규화 + (선택) 시드/허브 힌트. 기존 `model_type`이면 휴리스틱 엔진 재사용, 신규 모델 타입이면 §7 참고.
5. **인덱서 연동 확인**: 상류 인덱서가 `X`의 BalanceDelta를 `source_block_hash`와 함께 공급하는지 확인.
6. **검증**: 05의 테스트 매트릭스를 `X` 데이터로 재실행(특히 T-3 supernode 억제, T-9 reorg).

**코어 코드/스키마 변경은 0.** 위 6단계 중 코드 작업은 4번(어댑터)뿐이고, 그마저 기존 model_type이면 재사용으로 끝난다.

---

## 7. 진짜 새로운 모델 타입이 등장할 때

UTXO도 account도 아닌 체인(예: 리소스/오브젝트 모델 계열, 또는 UTXO 변종)이 오면:

1. `model_type`에 새 값 추가(enum이 아니라 텍스트+체크 제약으로 두어 확장 용이 — 07 참조).
2. 그 모델에 맞는 **휴리스틱 엔진 세트를 새로 구현**하고 `heuristic`·`chain_heuristic`에 등록.
3. 코어(merge_evidence·재생·Union-Find·조회)는 **여전히 불변**. 새 모델의 휴리스틱도 결국 "두 주소가 같은 주체"라는 MergeCandidate를 방출할 뿐이므로, 병합 계층은 아무 변화가 없다.

이것이 이 설계의 핵심 이점이다: **모델이 아무리 달라도, 클러스터링의 본질("같은 주체 주소쌍을 근거와 함께 병합")은 동일**하므로, 변이는 항상 휴리스틱 계층에 국한된다.

---

## 8. 확장성 불변식 체크 (구현 자기점검)

- [ ] 코어 테이블(`merge_evidence`/`cluster`/`membership`/`label`)이 특정 체인을 하드코딩하지 않고 `chain_id`만 참조하는가?
- [ ] 휴리스틱 선택이 개별 체인이 아니라 `model_type` + `chain_heuristic` 매핑으로 결정되는가?
- [ ] 새 체인 추가가 스키마 변경 없이 파티션 생성 + 레지스트리 INSERT로 되는가?
- [ ] 파라미터(dust 임계치 등)가 `chain_heuristic.params`로 체인별 조정 가능한가?
- [ ] 크로스체인 엔티티가 코어와 분리된 링크 계층으로 처리되는가?
- [ ] 새 model_type 추가 시 코어 병합/재생 로직이 그대로인가?
