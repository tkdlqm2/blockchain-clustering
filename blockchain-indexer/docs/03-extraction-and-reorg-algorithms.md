# 03. 추출 & reorg 알고리즘 (핵심 문서)

> 인덱서 정확성의 근간. 언어 무관 의사코드로 기술한다. 모든 추출은 **계약 준수 메시지**(02 §A)를 산출해야 한다: `amount`는 문자열, `delta_index`는 txid 내 유일, 주소는 정규화, `block_hash` 포함.
>
> 공통 규약: `emitDelta(...)`는 하나의 BalanceDelta 이벤트를 만들어 발행 큐에 넣는다. `di`는 트랜잭션 내 delta_index 카운터(0부터).

---

## 0. 메인 루프

```
function indexLoop(chain):
    cfg   = chainConfig(chain)
    adapter = adapterFor(cfg.model_type)      # utxo / account / ...
    loop:
        latest = node.getLatestHeight(chain)
        cursor = readCursor(chain)
        if cursor >= latest: sleep(); continue

        next = cursor + 1
        raw  = node.getBlock(chain, next)      # verbosity 충분히(트랜잭션·vin/vout 또는 receipts 포함)

        # reorg 검사 (§reorg)
        if not isContinuous(chain, raw):
            handleReorg(chain, raw); continue   # 통지 후 재개

        # 추출 → 발행 (하나의 논리 트랜잭션으로)
        beginPublish()
        persistBlock(chain, raw.height, raw.hash, raw.parent_hash)
        for tx in raw.transactions:
            adapter.extract(chain, tx, raw)     # emitDelta들 (§1 or §2)
        commitPublish()                          # Kafka 발행 성공 + 상태 반영이 원자적
        writeCursor(chain, next)                  # 발행 완료 후에만 전진(FR-16)
```

- **발행 원자성**: `beginPublish/commitPublish`는 "블록의 모든 delta가 성공적으로 Kafka에 들어간 뒤 커서를 전진"함을 의미한다. 실패 시 커서 미전진 → 다음 루프에서 같은 높이 재처리(멱등이라 안전).
- **순서**: 블록을 높이순으로만 처리하므로 발행도 높이순(FR-2). 파티션 키 `chain_id`로 체인 내 순서 보존.

---

## 1. UTXO 추출 (BitcoinAdapter.extract)

Bitcoin 트랜잭션은 N개 input(지출) × M개 output(수취). 이를 delta로 펼친다.

```
function extract_utxo(chain, tx, block):
    di = 0

    # (1) inputs → 지출 (coinbase의 vin은 건너뜀)
    for vin in tx.vin:
        if isCoinbase(vin): continue
        prev = lookupPrevout(chain, vin.txid, vin.vout)     # §prevout
        addr = prev.address                                  # 이미 정규화 저장, 없으면 unparsed 처리(§주소)
        emitDelta(
            chain_id=chain, txid=tx.id, delta_index=di++,
            address=addr, amount=neg(prev.value),            # "-value" 문자열
            kind="native",
            block_height=block.height, block_hash=block.hash,
            meta={})                                         # 필요 시 {"role":"vin","i":vin.index}

    # (2) outputs → 수취
    for vout in tx.vout:
        addr = extractAddress(chain, vout.scriptPubKey)      # §주소 (실패 시 규칙 처리)
        emitDelta(
            chain_id=chain, txid=tx.id, delta_index=di++,
            address=addr, amount=pos(vout.value_sat),        # "+value" 문자열
            kind="native",
            block_height=block.height, block_hash=block.hash,
            meta={})

        # prevout 캐시에 이 output 등록(다른 tx의 vin이 참조할 것)
        putPrevout(chain, tx.id, vout.n, addr, vout.value_sat, block.hash)

    # 수수료는 delta로 만들지 않는다(Σvin - Σvout로 파생). change output도 특별 취급 없음.
```

- **금액 표현**: `neg("100000000") = "-100000000"`. 항상 문자열, 최소 단위(satoshi).
- **coinbase**: 첫 트랜잭션의 vin은 이전 출력이 없으므로 수취 delta만.

### §prevout — 이전 출력 조회
input(vin)은 `txid:vout`만 참조하므로, 소유 주소·금액을 알려면 되짚어야 한다. 두 방법:
- **(A) `-txindex` 노드 + `getrawtransaction <txid> true`**: 캐시 불요, 구현 단순. 1차 권장.
- **(B) 자체 prevout 캐시(B.3)**: output 인덱싱 시 `(txid,vout)→(addr,value)` 저장, input에서 조회. 고성능이나 reorg 시 캐시 롤백 필요.
- **선택 지침**: 1차는 (A). 성능 병목 확인 시 (B)로 전환. reorg 롤백(§reorg)은 (B) 사용 시 캐시도 함께 되돌려야 함.

---

## 2. 계정 체인 추출 (EthereumAdapter.extract)

native 전송 + ERC-20 로그 + 컨트랙트 배포를 하나의 delta 리스트로 합친다.

```
TRANSFER_TOPIC = keccak256("Transfer(address,address,uint256)")

function extract_account(chain, tx, block):
    receipt = node.getReceipt(chain, tx.hash)
    if receipt.status == 0: return              # revert → delta 없음(FR-5)
    di = 0

    # (1) native ETH 전송
    if tx.value > 0:
        emitDelta(chain, tx.hash, di++, normalize(tx.from), neg(tx.value), "native",
                  block.height, block.hash, meta={})
        target = tx.to
        emitDelta(chain, tx.hash, di++, normalize(target), pos(tx.value), "native",
                  block.height, block.hash, meta={})

    # (2) 컨트랙트 배포 (to == null → 새 컨트랙트 생성)
    if tx.to == null and receipt.contractAddress != null:
        emitDelta(chain, tx.hash, di++, normalize(receipt.contractAddress),
                  pos(tx.value_or_zero), "native",
                  block.height, block.hash,
                  meta={"contract_creation": true})           # 계약 §5 (FR-6)
        # deployer(=tx.from) ↔ 생성 컨트랙트 관계를 소비자가 병합할 근거가 됨

    # (3) ERC-20 Transfer 로그
    for log in receipt.logs:
        if log.topics[0] != TRANSFER_TOPIC: continue
        if len(log.topics) != 3: continue        # ERC-721(topics 4개)·기타 이벤트 제외
        from  = normalize(decodeAddress(log.topics[1]))
        to    = normalize(decodeAddress(log.topics[2]))
        value = decodeUint256(log.data)           # value는 indexed 아님 → data
        token = normalize(log.address)            # 토큰 컨트랙트 주소
        emitDelta(chain, tx.hash, di++, from, neg(value), "token",
                  block.height, block.hash, meta={"token_contract": token})  # FR-7
        emitDelta(chain, tx.hash, di++, to, pos(value), "token",
                  block.height, block.hash, meta={"token_contract": token})
```

- **로그 존재 = revert 안 됨**: revert된 서브콜 로그는 receipt에 없다. 따라서 `Transfer` 로그가 있으면 그 이동은 실제로 일어난 것.
- **`meta.token_contract`(FR-7)**: 없으면 소비자가 USDC/USDT를 구분 못 한다(계약 §7-2). ERC-20 delta에는 반드시 채운다.
- **컨트랙트 배포(FR-6)**: `meta.contract_creation:true`로 표시해 소비자가 deployer 관계로 병합하게 한다.
- **internal ETH transfer**(CALL로 발생하는 컨트랙트 간 전송)는 `tx.value`·로그에 안 잡힌다. **1차 범위 밖**. 필요 시 `trace_block`/`debug_traceTransaction`로 CALL 프레임의 value>0을 delta로 추가하는 확장(어댑터 내부만 변경). 06 §확장 참고.

---

## 3. reorg 감지·통지 (§reorg)

인덱서는 reorg의 **권위 있는 감지자**다(계약 §7-5 해소, 06 §5). parent_hash 체인으로 감지한다.

```
function isContinuous(chain, newBlock):
    parent = getStoredBlock(chain, newBlock.height - 1)
    if parent == null: return true               # 초기 동기화 구간
    return parent.hash == newBlock.parent_hash

function handleReorg(chain, newBlock):
    # 1) 공통 조상 찾기: 저장 해시와 노드 실제 해시가 갈라진 지점까지 되감기
    h = newBlock.height - 1
    rolledBack = []
    while getStoredBlock(chain, h).hash != node.getBlockHashAt(chain, h):
        rolledBack.append(getStoredBlock(chain, h).hash)
        h = h - 1
    # h = 공통 조상. h+1 이상이 orphan.

    # 2) reorg 이벤트 발행 (소비자가 이 해시들의 병합을 무효화·재생)
    publishReorg(chain, rolled_back_block_hashes = rolledBack)   # 계약 §3.2 (FR-12,13)

    # 3) 내부 상태 롤백: orphan 블록·(사용 시)prevout 캐시 삭제, 커서를 h로
    deleteBlocksFromHeight(chain, h + 1)
    if usingPrevoutCache: deletePrevoutFromBlocks(rolledBack)
    writeCursor(chain, h)
    # 다음 루프에서 h+1부터 새 체인 블록을 재추출·재발행
```

- **통지 우선**: reorg 이벤트를 **먼저** 발행하고 나서 새 블록을 재발행한다. 소비자가 무효화를 마친 뒤 새 데이터를 받게 하기 위함.
- **멱등 재발행**: 재발행되는 새 체인 블록의 delta들은 정상 `(chain_id, txid, delta_index)` 키를 가지므로 소비자가 정합적으로 반영한다.
- **자체 감지가 유일 경로**: 계약 §7-5의 "소비자 측 폴백"은 인덱서가 신뢰성 있게 reorg를 통지하면 불필요해진다. 인덱서는 이 통지를 **누락 없이** 수행할 책임이 있다(그래서 감지 로직이 인덱서 정확성의 1급 요소).

### §finality — 확정 정책
- `finality_depth` 이하로 오래된 블록은 reorg 가능성이 사실상 없다고 본다.
- Ethereum PoS처럼 노드가 `finalized` 태그를 주면 그것을 사용, 아니면 `latest - height >= finality_depth`.
- 인덱서는 미확정 구간에서만 reorg를 감지·통지하면 충분하다.

---

## 4. 주소 정규화 (§주소)

인덱서가 발행 **전에** 수행한다(06 §3 결정).

```
function normalize(chain, rawAddress):
    switch chainConfig(chain).address_normalization:
        case "evm-lowercase":  return toLowerCase(rawAddress)      # 체크섬 대소문자 제거
        case "bitcoin":        return rawAddress                    # 노드가 준 표준 인코딩 사용
        default:               return rawAddress

function extractAddress(chain, scriptPubKey):
    addr = scriptToAddress(scriptPubKey)    # 노드 응답의 scriptPubKey.address 우선 사용
    if addr == null:
        return "unparsed:" + hash(scriptPubKey)   # OP_RETURN 등 (계약과 합의된 표기)
    return normalize(chain, addr)
```

- **EVM**: 소문자 통일(대소문자 혼용 체크섬 방지).
- **Bitcoin**: `getblock ... 2` 응답의 `scriptPubKey.address`를 그대로 신뢰(직접 인코딩보다 안전). 없으면 `unparsed:` 표기 후 발행하되, 소비자는 이를 잔액/클러스터링에서 제외한다(합의 사항).

---

## 5. 멱등·발행 규칙

- 한 트랜잭션의 delta_index는 방출 순서대로 0,1,2… 연속 부여하며, **추출 로직이 결정적**이면 재처리 시 동일 키가 재생된다(NFR-2).
- 발행 실패는 커서를 전진시키지 않음으로써 재시도된다. 소비자가 `(chain_id,txid,delta_index)`로 중복 제거하므로 재발행은 안전.
- Kafka 파티션 키 = `chain_id`(체인 내 순서 보존). 배치 크기·백필 전략은 06 §4.

---

## 6. 알고리즘 불변식 체크 (구현 자기점검)

- [ ] 모든 delta의 `amount`가 **문자열**로 인코딩되는가?
- [ ] `delta_index`가 한 txid 안에서 0부터 연속·유일한가?
- [ ] UTXO input이 prevout 조회로 소유 주소·금액을 정확히 채우는가? coinbase vin은 지출 delta를 안 만드는가?
- [ ] 계정 체인에서 `receipt.status==0`이면 delta를 만들지 않는가?
- [ ] ERC-20 delta에 `meta.token_contract`가, 컨트랙트 배포에 `meta.contract_creation`이 채워지는가?
- [ ] 주소가 발행 전에 정규화되는가(EVM 소문자)?
- [ ] reorg 시 통지를 **먼저** 발행하고, 내부 상태(블록·prevout 캐시)를 롤백한 뒤 재발행하는가?
- [ ] 발행 완료 후에만 커서가 전진하는가?
