# 02. 논리 데이터 모델 (Logical Data Model)

> **스택 비종속**. 아래는 물리 DDL이 아니라 **논리 스키마**다. 관계형/문서/그래프 DB 어디로도 사상 가능하도록, 개체·속성·키·관계·불변식만 규정한다. 물리 타입·인덱스·파티셔닝은 구현자가 선택한 스택에서 결정한다.
>
> **핵심 불변식(가장 중요)**: `merge_evidence`(병합 근거)가 **진실의 원천**이고, `cluster_membership`은 그로부터 재생되는 **파생 캐시**다. 두 저장소가 불일치하면 언제나 merge_evidence가 옳다.

---

## 1. 입력 개체: BalanceDelta (외부, 참조용)

인덱서가 생산한다. 이 시스템은 **소비만** 하며 정의하지 않지만, 의존하는 최소 필드는 다음과 같다.

| 필드 | 의미 | 클러스터링에서의 용도 |
|---|---|---|
| `chain` | 체인 식별자 | 체인별 휴리스틱 분기 |
| `txid` | 트랜잭션 식별자 | 공통 입력 그룹핑 키 |
| `delta_index` | 트랜잭션 내 일련번호 | 멱등 식별 |
| `address` | 주소 | 클러스터 원소 |
| `amount` | 부호 있는 정수(±) | 지출(−)/수취(+) 구분 |
| `kind` | native / token | 필터링 |
| `block_height` | 블록 높이 | 증분·순서 |
| `block_hash` | 블록 해시 | **reorg 롤백 근거(필수)** |
| `meta` | 부가정보(예: vin/vout 역할, log index) | 잔돈·협업 탐지 보조 |

> **파생 규칙**: "공통 입력 집합"은 `동일 txid` + `amount < 0`(지출) delta들의 `address` 집합이다. "수취 집합"은 `amount > 0` delta들의 `address` 집합이다. 03의 여러 휴리스틱이 이 두 파생을 사용한다.

---

## 2. address — 주소 레지스트리

주소별 메타·플래그를 보관한다. 클러스터링·전처리의 공용 참조점.

| 필드 | 의미 | 비고 |
|---|---|---|
| `chain` | 체인 | PK 일부 |
| `address` | 주소 문자열 | PK 일부 (체인별 정규화 규칙 적용: 예 EVM 소문자) |
| `first_seen_height` | 최초 관측 높이 | 신선도·정렬 |
| `last_seen_height` | 최종 관측 높이 | |
| `is_hub` | 허브 여부 | 전처리 산출(FR-4) |
| `hub_type` | exchange / mixer / bridge / contract-hub / null | |
| `hub_confidence` | 허브 판정 신뢰도 | |
| `dust_flag` | dust 유입 표적 여부 | 전처리 산출(FR-6) |

- **PK**: `(chain, address)`.
- **불변식**: `is_hub = true`인 주소를 경유하는 병합은 차단된다(03 §2).

---

## 3. merge_evidence — 병합 근거 (★ 진실의 원천)

append-only. 시스템의 심장. 모든 병합은 여기에 한 줄로 남는다.

| 필드 | 의미 | 비고 |
|---|---|---|
| `op_id` | 전역 단조 증가 식별자 | 재생 순서 결정 |
| `chain` | 체인 | |
| `address_a` | 병합 대상 A | |
| `address_b` | 병합 대상 B | A,B는 이 근거로 "같은 엔티티"로 주장됨 |
| `heuristic` | 휴리스틱 종류 | `common-input` / `sweep-seed` / `change` / `funding` / `deployer` / `behavioral` / `manual` |
| `source_txid` | 근거 트랜잭션 | manual/seed는 null 가능 |
| `source_block_hash` | 근거 블록 해시 | **reorg 롤백 필수**. 근거가 온체인일 때 필수 |
| `source_block_height` | 근거 블록 높이 | 롤백 범위 판정 |
| `confidence` | 이 병합의 신뢰도 (0~1) | 03 §7 |
| `status` | active / invalidated | 무효화(정정·롤백) 시 전이 |
| `invalidated_reason` | reorg / manual-correction / null | |
| `created_at` | 생성 시각 | 감사 |
| `created_by` | system / operator-id | 감사 |

- **PK**: `op_id`.
- **불변식 1 (append-only)**: 레코드는 물리적으로 수정/삭제하지 않는다. 무효화는 `status`를 `invalidated`로 전이하는 것으로만 한다.
- **불변식 2 (근거 특정)**: 온체인 휴리스틱은 `source_txid`와 `source_block_hash`를 반드시 채운다. 이것이 없으면 reorg 롤백이 불가능하다.
- **불변식 3 (재생 가능)**: `status = active`인 레코드만 `op_id` 순으로 재생하면 언제나 동일한 클러스터 멤버십이 산출되어야 한다(멱등·결정적).

---

## 4. cluster — 엔티티

병합 근거 재생으로 형성되는 클러스터의 대표 레코드. 라벨·정체성이 붙는 대상.

| 필드 | 의미 | 비고 |
|---|---|---|
| `cluster_id` | 클러스터 식별자 | 재생 시 안정적으로 유지되도록 canonical 규칙 필요(§6) |
| `chain` | 체인 | 교차체인 엔티티는 별도 링크(§7)로 처리, 기본은 체인 내 |
| `size` | 소속 주소 수 | 파생·캐시 |
| `entity_type` | exchange / individual / protocol / mixer / unknown | 라벨에서 유도 |
| `representative_confidence` | 클러스터 응집 신뢰도 요약 | 파생 |
| `updated_at` | 최종 갱신 | |

- **PK**: `cluster_id`.
- **파생 규칙**: cluster는 merge_evidence 재생 결과의 요약이다. 독립적으로 수정하지 않는다.

## 5. cluster_membership — 주소↔클러스터 매핑 (파생 캐시)

| 필드 | 의미 | 비고 |
|---|---|---|
| `chain` | 체인 | |
| `address` | 주소 | |
| `cluster_id` | 소속 클러스터 | |
| `membership_confidence` | 이 주소가 이 클러스터에 속할 신뢰도 | 근거 경로의 결합값(03 §7) |

- **PK**: `(chain, address)` — 한 주소는 한 클러스터에만 속한다.
- **불변식**: 이 표는 merge_evidence(active)로부터 재생 가능해야 한다. 불일치 시 재생값이 정답.

---

## 6. cluster_id 안정성 규칙 (구현 주의)

재생 때마다 cluster_id가 바뀌면 라벨·외부 참조가 깨진다. 다음 중 하나로 안정화한다.

- **(권장) canonical anchor 방식**: 클러스터의 대표를 "가장 이른 op_id로 병합에 처음 등장한 주소" 또는 "가장 오래된 주소(first_seen_height 최소)"로 결정하고, cluster_id를 그 대표 주소의 해시로 유도한다. 병합으로 두 클러스터가 합쳐지면 더 이른 대표가 승계한다.
- **불변식**: cluster_id는 소속 주소 집합의 결정적 함수여야 한다(동일 집합 → 동일 id). 라벨은 cluster_id가 아니라 "대표 주소" 또는 "안정 키"에 붙여 승계가 자연스럽게 되도록 설계할 수도 있다(구현자 선택).

---

## 7. label — 라벨

클러스터/주소에 정체성을 부여. 출처·신선도 관리(FR-18~21).

| 필드 | 의미 | 비고 |
|---|---|---|
| `label_id` | 식별자 | |
| `target_type` | cluster / address | |
| `target_id` | cluster_id 또는 (chain,address) | |
| `label` | 표시명 (예: "Binance hot wallet") | |
| `category` | exchange / mixer / bridge / scam / protocol / ... | entity_type 유도에 사용 |
| `source` | 출처 | known-deposit / official / crowdsourced / investigation / operator |
| `source_confidence` | 출처 등급 기반 신뢰도 | 직접검증 > 공식 > 크라우드소싱 |
| `collected_at` | 수집 시각 | 신선도 |
| `last_verified_at` | 최종 검증 시각 | 신선도 감쇠 기준 |
| `status` | active / stale / conflicted / retired | FR-20, FR-21 |

- **불변식**: 라벨은 확정이 아니라 신뢰도를 동반한 주장이다. 조회 응답은 confidence를 함께 반환한다.
- **충돌 처리**: 같은 target에 상충 category 라벨이 active로 공존하면 `conflicted`로 전이하고 자동 확정 금지.

---

## 8. 전처리 산출 개체

### 8.1 excluded_tx — 병합 제외 트랜잭션
| 필드 | 의미 |
|---|---|
| `chain`, `txid` | 대상 |
| `reason` | coinjoin / hub-touch / dust-only |
| `detector_confidence` | 탐지 신뢰도 |
| `signal` | 보존할 신호(예: "coinjoin:wasabi") — 추적 신뢰도 하향 근거 |

- **용도**: 공통 입력 등 휴리스틱은 이 표에 있는 txid를 근거로 삼지 않는다(FR-5,6,7).

### 8.2 sweep_target — 집금 목적지 앵커 (거래소 클러스터 시드)
| 필드 | 의미 |
|---|---|
| `chain`, `address` | 집금 목적지(핫/콜드/집금 지갑) |
| `entity_hint` | 어느 거래소로 추정/확정되는지 |
| `source` | known-deposit / observed | 
| `confidence` | 신뢰도 |

- **용도**: 같은 `sweep_target`으로 집금되는 입금 주소들을 그 엔티티로 병합(FR-9). 단, 입금을 보낸 원천 주소는 제외.

---

## 9. audit_log — 감사 추적 (FR-26)

| 필드 | 의미 |
|---|---|
| `event_id` | 식별자 |
| `actor` | system / operator-id |
| `action` | merge / invalidate / label-add / label-retire / hub-set / seed-add |
| `target` | 대상 참조 |
| `rationale` | 사유 |
| `at` | 시각 |

---

## 10. 개체 관계 요약

```
BalanceDelta(외부) ──파생──▶ 공통입력집합 / 수취집합
        │
        ▼ (휴리스틱)
merge_evidence  ── op_id 순 재생 ──▶  cluster / cluster_membership (파생 캐시)
        ▲                                   │
        │ (무효화: reorg/manual)              ▼
   audit_log ◀──────────────────────── label (신선도 관리)
                                            ▲
address(is_hub/dust) ── 전처리 ── excluded_tx / sweep_target
```

**한 문장 요약**: 모든 화살표의 종착점은 "재생 가능성"이다. merge_evidence(active)만 있으면 cluster·membership을 언제든 결정적으로 재생할 수 있어야 하며, 이것이 reorg 롤백(FR-23)과 오탐 정정(FR-24)과 멱등성(NFR-3)을 동시에 보장한다.
