# 03. 클러스터링 알고리즘 명세 (핵심 문서)

> 이 시스템의 정확성이 결정되는 문서다. 모든 로직은 **언어 무관 의사코드**로 기술한다. 구현자는 스택 언어로 옮기되 **알고리즘 구조와 순서를 바꾸지 않는다.**
>
> 표기: `emit(...)`는 병합 후보(MergeEvidence candidate) 생성을 뜻하며, 실제 병합은 §6 병합 엔진이 근거를 기록하며 수행한다. 휴리스틱은 병합을 직접 하지 않는다(FR-12).

---

## 0. 전체 파이프라인 (순서가 정확성을 지배한다)

```
function runPipeline(deltaBatch):
    # [A] 전처리 — 병합 이전 오염 차단 (반드시 먼저)
    markHubs(deltaBatch)                 # §1
    markCollaborativeTx(deltaBatch)      # §2
    markDust(deltaBatch)                 # §3

    # [B] 휴리스틱 — 병합 후보 생성 (제외 대상은 건너뜀)
    candidates = []
    candidates += commonInputHeuristic(deltaBatch)     # §4  (UTXO)
    candidates += sweepHeuristic(deltaBatch)           # §5  (거래소 입금주소)
    candidates += changeHeuristic(deltaBatch)          # §5b (보수적)
    candidates += accountModelHeuristics(deltaBatch)   # §5c (계정 체인)

    # [C] 병합 — 근거 기록 + 파생 재생
    for c in candidates:
        recordAndMerge(c)                # §6  (merge_evidence append + Union-Find)

    # [D] 라벨·신선도
    expandFromSeeds()                    # §5 시드 확장 + §8 라벨
```

**불변 규칙**: [A]는 언제나 [B]보다 먼저. [C]는 [B]의 산출에만 의존. 순서 위반은 supernode 붕괴를 유발한다.

---

## 1. 허브 탐지 (markHubs)

무관한 다수 주체의 자금이 통과하는 노드를 표시해, 이후 모든 병합에서 차단한다.

```
function markHubs(batch):
    for addr in addresses(batch):
        # (a) known 라벨 우선
        if hasKnownHubLabel(addr):
            setHub(addr, type=labelType, conf=0.99); continue

        # (b) 행동 지표 (윈도 집계값 사용; 실측 튜닝)
        counterparties = distinctCounterpartyCount(addr, window)
        txRate         = txCountPerDay(addr, window)
        sweepConverge  = sweepConvergenceScore(addr)   # 다수 주소가 이 addr로 집금되는 정도

        score = hubScore(counterparties, txRate, sweepConverge)   # 가중 합, 0~1
        if score >= HUB_THRESHOLD:
            setHub(addr, type=inferHubType(...), conf=score)
```

- **HUB_THRESHOLD**는 고정 상수로 시작하되(예: 상위 백분위 기반), 반드시 실측으로 튜닝. 고정값 맹신 금지.
- **차단 방식(중요)**: "제외"를 전역 삭제가 아니라 **"이 노드를 경유하는 병합만 차단"** 으로 국소화한다(§6에서 검사).
- **오탐 대비**: 활발한 개인이 허브로 오분류될 수 있으므로 hub 판정에도 confidence를 둔다.

---

## 2. 협업 트랜잭션 탐지 (markCollaborativeTx)

CoinJoin류는 "한 tx의 input = 같은 주인" 가정을 깨므로, 공통 입력 휴리스틱에서 제외한다.

```
function markCollaborativeTx(batch):
    for tx in groupByTx(batch):
        ins  = inputAddresses(tx)     # amount<0 delta
        outs = outputEntries(tx)      # amount>0 delta (address, amount)

        equalValueGroups = groupByAmount(outs)
        maxEqual = max(size of each group)

        isCoinjoin =
            (maxEqual >= EQUAL_OUTPUT_MIN) and           # 동일 금액 output 다수
            (len(ins) >= COLLAB_INPUT_MIN) and           # 다수 input
            (len(outs) >= COLLAB_OUTPUT_MIN) and         # 다수 output
            structureMatchesKnownImpl(tx)?               # Wasabi/Whirlpool/JoinMarket 지문(보조)

        if isCoinjoin:
            excludeTx(tx, reason='coinjoin', signal=implName?, conf=...)
```

- **강한 신호**: 동일 금액 output의 반복(익명성 집합 형성). 이것이 1순위 판정 근거.
- **구분 주의**: 거래소 batch 지급도 다수 output을 가진다. 동일 금액 반복 여부·output 라벨·지갑 지문으로 구분.
- **신호 보존**: 제외하되 "이 자금은 CoinJoin을 거쳤다"를 `excluded_tx.signal`로 남겨 추적 신뢰도 하향 근거로 쓴다.

---

## 3. dust 표시 (markDust)

의도적으로 심어진 극소액이 공통 입력 병합을 오염시키는 것을 막는다.

```
function markDust(batch):
    for d in batch where d.amount > 0 and d.amount <= DUST_THRESHOLD:
        flagDustInflow(d.address, d.txid)

    # dust만이 유일 근거인 병합을 이후 차단하기 위한 표시
    for tx in groupByTx(batch):
        ins = inputEntries(tx)
        if allInputsAreDustTainted(ins):   # 병합 근거가 dust 공유뿐이면
            excludeTx(tx, reason='dust-only', conf=...)
```

- **DUST_THRESHOLD**는 체인·수수료 수준에 맞춰 설정.
- dust 출처(뿌린 주소)를 역추적하면 dusting 캠페인 자체를 엔티티로 라벨링할 수 있다(부가).

---

## 4. 공통 입력 휴리스틱 — UTXO 주력 (commonInputHeuristic)

```
function commonInputHeuristic(batch):
    out = []
    for tx in groupByTx(batch):
        if isExcluded(tx): continue                    # §2,§3 (coinjoin/dust-only)
        ins = inputAddresses(tx)                        # amount<0 delta의 address 집합
        ins = [a for a in ins if not isHub(a)]          # §1 허브 경유 차단
        if len(ins) < 2: continue

        anchor = ins[0]
        for a in ins[1:]:
            out.append(MergeCandidate(
                a=anchor, b=a, heuristic='common-input',
                txid=tx.id, block_hash=tx.block_hash, block_height=tx.height,
                confidence=CONF_COMMON_INPUT))          # 높음 (예 0.95)
    return out
```

- **근거의 방향성**: 병합 근거는 **지출(input)** 이지 수취(input으로 들어온 것)가 아니다. `amount<0` delta만 사용.
- **star 병합**: 집합을 anchor 기준 별 모양으로 union하면 충분(전쌍 불필요). Union-Find가 전이적으로 하나로 만든다.

---

## 5. sweep(집금) 휴리스틱 — 거래소 입금주소 (sweepHeuristic)

거래소 입금 주소를 그 거래소 엔티티로 묶는다. **입금을 보낸 원천(사용자) 주소는 묶지 않는다.**

```
function sweepHeuristic(batch):
    out = []
    for tx in groupByTx(batch):
        # 입금주소 → 집금목적지 패턴: 한 주소의 자금이 전액에 가깝게 target으로 이동
        for (src, dst, amt) in transfers(tx):
            if isKnownSweepTarget(dst):                 # sweep_target 앵커(§data-model 8.2)
                if looksLikeSweep(src, dst, amt):       # 전액 근접 + 체계적/반복
                    out.append(MergeCandidate(
                        a=dst, b=src, heuristic='sweep-seed',
                        txid=tx.id, block_hash=tx.block_hash, block_height=tx.height,
                        confidence=CONF_SWEEP))          # 높음 (시드 검증 시 0.9+)
        # 주의: src로 "입금을 보낸" 바깥 주소는 후보에 넣지 않는다.
    return out

function looksLikeSweep(src, dst, amt):
    # 소유 근거 = 체계적 자동 집금. 단발 전송과 구분.
    return residualBalanceNearZero(src after tx)        # 받은 걸 거의 전액 쓸어감
        and recurringToSameTarget(dst)                  # 같은 target으로 반복
```

**시드 확보·확장 (expandFromSeeds).**
```
function expandFromSeeds():
    # 1) known-deposit 실험 결과를 sweep_target 앵커로 등록 (운영자 주입, FR-25)
    # 2) 같은 sweep_target으로 집금되는 모든 입금주소를 해당 엔티티로 병합
    # 3) 시드에서 멀어질수록(hop) confidence 감쇠
```

- **경계선(반드시 준수)**: 묶는 것은 `dst`(집금목적지)와 `src`(입금주소)이며, `src`에게 입금을 **보낸** 바깥 주소는 사용자 소유이므로 제외.
- **허브 시드 주의**: sweep_target이 대형 핫월렛(허브)이면 무제한 확장 위험. target을 "정체성 라벨"로 쓰되, 확장은 "그 target으로 집금하는 입금주소"로 한정.
- **batch collect**: 여러 입금주소를 한 tx로 집금하면 §4 공통 입력으로도 자연히 묶인다(동일 결론).

### 5b. 잔돈 휴리스틱 (changeHeuristic) — 보수적

```
function changeHeuristic(batch):
    out = []
    for tx in groupByTx(batch):
        if isExcluded(tx) or touchesHub(tx): continue
        ins  = inputAddresses(tx)
        outs = outputEntries(tx)
        cand = guessChangeOutput(outs, ins)   # 아래 단서들의 결합
        if cand != null and confidenceOf(cand) >= CHANGE_MIN:
            out.append(MergeCandidate(
                a=ins[0], b=cand.address, heuristic='change',
                txid=tx.id, block_hash=tx.block_hash, block_height=tx.height,
                confidence=min(CONF_CHANGE, confidenceOf(cand))))  # 낮음 (예 ≤0.4)
    return out

function guessChangeOutput(outs, ins):
    # 약한 단서들(각각 낮은 가중치): 새 주소 / 반올림 안 된 금액 / input과 동일 스크립트타입
    # / 곧바로 재소비되는 output. 단일 단서로 확정 금지 — 결합 점수로만.
```

- 잔돈 오판은 **수취인(상대방)을 내 클러스터로 끌어들이는** 치명적 오탐. 그래서 낮은 confidence + 단독 확정 금지.

### 5c. 계정 체인 휴리스틱 (accountModelHeuristics)

공통 입력이 없는 계정 체인(예: 이더리움) 전용. UTXO 휴리스틱과 혼용하지 않는다.

```
function accountModelHeuristics(batch):
    out = []
    # (1) funding: 새 EOA가 첫 가스/시드 자금을 받은 출처
    for addr in newAddresses(batch):
        funder = firstFundingSource(addr)
        if funder != null and not isHub(funder):
            out.append(MergeCandidate(a=funder, b=addr, heuristic='funding',
                                      confidence=CONF_FUNDING))   # 중간
    # (2) deployer: 컨트랙트 배포자
    for c in newContracts(batch):
        out.append(MergeCandidate(a=deployer(c), b=c.address, heuristic='deployer',
                                  confidence=CONF_DEPLOYER))      # 중간~높음
    # (3) behavioral: 반복 상호작용·시간패턴 (보조, 낮은 confidence)
    ...
    return out
```

- **계정 체인 허브 주의**: 인기 dApp/컨트랙트는 허브다. "같은 컨트랙트와 상호작용했다"만으로 묶지 말 것(§1을 계정 버전으로 적용).
- **엔티티 정의 주의**: 프록시·멀티시그·계정추상화(AA)로 "주소=사람"이 약함. 공동통제 가능성을 라벨에 반영.

---

## 6. 병합 엔진 — 근거 기록 + Union-Find 재생 (recordAndMerge)

**merge_evidence = 진실의 원천, Union-Find = 파생 캐시.** 이 관계가 되돌림·롤백·멱등의 근간.

```
function recordAndMerge(candidate):
    if candidate.a == candidate.b: return
    if isHub(candidate.a) or isHub(candidate.b): return    # 허브 경유 차단(재확인)

    # 1) 근거를 append-only로 기록 (진실의 원천)
    op_id = appendMergeEvidence(candidate, status='active')

    # 2) 파생 캐시(Union-Find) 갱신
    union(candidate.a, candidate.b)
    recomputeMembershipConfidence(candidate.a, candidate.b)  # §7

# Union-Find (경로 압축 + rank) — 대규모 상수시간
find(x):   루트까지 이동하며 경로 압축
union(a,b): 두 루트를 rank 기준 병합

# 멤버십 재생 (전체 재계산 / 롤백 후 사용)
function rebuildMembershipFromEvidence():
    resetUnionFind()
    for e in mergeEvidence where status='active' order by op_id:
        union(e.address_a, e.address_b)
    materializeClusters()   # cluster_id 안정 규칙(02 §6) 적용
```

- **경로 압축 주의**: 빠르지만 구조를 파괴해 "부분 되돌리기"가 불가능. 따라서 **되돌림은 Union-Find를 건드리지 않고 merge_evidence에서 재생**하는 방식으로만 한다(아래 §9).
- cluster_id는 결정적이어야 한다(02 §6). 재생 때마다 바뀌면 라벨이 깨진다.

---

## 7. 신뢰도 부여·결합 (confidence)

```
휴리스틱별 기본값(예시, 실측 튜닝):
  common-input : 0.95
  sweep-seed   : 0.90 (검증 시드), hop마다 감쇠
  deployer     : 0.85
  funding      : 0.60
  behavioral   : 0.30
  change       : ≤0.40
  manual       : 1.00 (운영자 확정) / 0 (운영자 부인)

결합 규칙:
  - 같은 주소쌍을 지지하는 '독립' 근거가 여럿이면 신뢰도 상향.
    combined = 1 - Π(1 - conf_i)   (독립 가정 하 noisy-OR)
  - 단, 근거들이 사실상 같은 원천이면 독립이 아님 → 곱하지 말 것(과대평가 금지).
  - 멤버십 confidence = 그 주소를 클러스터에 연결하는 근거 경로의 최소/결합값(보수적 선택 권장).
```

- **threshold 뷰**: 조회의 `min_confidence`는 "그 임계치 이상 근거로만 재구성한 클러스터"를 산출. 컴플라이언스(높은 threshold)와 인텔리전스(낮은 threshold)를 같은 데이터에서 분리 제공.

---

## 8. 라벨 신선도 (labelMaintenance)

```
function labelMaintenance():
    for lb in labels where status='active':
        if now - lb.last_verified_at > STALE_TTL:
            decay(lb.source_confidence)          # 또는 status='stale'
            enqueueReverify(lb)
    detectConflicts()                            # 같은 target 상충 category → 'conflicted'
```

- 출처 등급: known-deposit > official > crowdsourced. 무검증 공개셋은 낮은 신뢰로만.

---

## 9. 증분 갱신·reorg 롤백 (핵심 운영 로직)

```
# 증분: 신규 배치는 파이프라인(§0)을 그대로 태우면 됨. union은 두 기존 클러스터를 합칠 수 있음.

function onReorg(rolledBackBlockHashes):
    # 1) 롤백된 블록을 근거로 가진 병합을 무효화 (append-only: status 전이만)
    for e in mergeEvidence where source_block_hash in rolledBackBlockHashes and status='active':
        setStatus(e, 'invalidated', reason='reorg')
    # 2) 남은 active 근거로 멤버십 재생 (§6 rebuild)
    rebuildMembershipFromEvidence()             # 결정적·멱등

function onManualCorrection(op_id):
    setStatus(op_id, 'invalidated', reason='manual-correction')
    rebuildMembershipFromEvidence()
```

- **왜 재생이 안전한가**: merge_evidence가 append-only이고 재생이 결정적(불변식 02 §3-3)이므로, 무효화 후 재생은 언제나 일관된 상태를 준다. 이것이 FR-23·FR-24·NFR-2·NFR-3을 한꺼번에 만족시킨다.
- **성능**: 매 무효화마다 전체 재생이 비싸면, 영향 받은 연결요소만 부분 재생하는 최적화를 둘 수 있으나, **정확성 기준 구현은 전체 재생**이며 최적화는 그와 동일 결과를 보장할 때만 허용한다.

---

## 10. 파라미터 요약 (튜닝 대상)

| 상수 | 의미 | 초기 전략 |
|---|---|---|
| `HUB_THRESHOLD` | 허브 판정 점수 | 상위 백분위, 실측 |
| `EQUAL_OUTPUT_MIN` / `COLLAB_*_MIN` | CoinJoin 판정 | 보수적으로 시작 |
| `DUST_THRESHOLD` | dust 상한 | 체인 수수료 수준 |
| `CONF_*` | 휴리스틱별 신뢰도 | 위 §7 예시에서 시작 |
| `CHANGE_MIN` | 잔돈 병합 최소 신뢰 | 낮게(≤0.4) |
| `STALE_TTL` | 라벨 신선도 만료 | 정책에 따라 |

모든 파라미터는 설정 가능해야 하며(하드코딩 금지), 변경 시 재생으로 효과를 검증한다.

---

## 11. 알고리즘 불변식 체크 (구현 자기점검)

- [ ] 전처리(§1~3)가 휴리스틱(§4~5)보다 먼저 실행되는가?
- [ ] 공통 입력이 `amount<0`(지출)만 근거로 쓰는가?
- [ ] 허브를 경유하는 병합이 §1과 §6 두 곳에서 차단되는가?
- [ ] sweep가 입금주소만 묶고, 입금을 보낸 원천 주소는 제외하는가?
- [ ] 모든 병합이 `source_block_hash`를 가진 근거로 기록되는가?
- [ ] reorg/정정이 Union-Find 직접 수정이 아니라 근거 무효화 + 재생으로 처리되는가?
- [ ] 재생이 결정적이어서 동일 active 근거 집합 → 동일 멤버십인가?
