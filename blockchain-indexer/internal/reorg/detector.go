// Package reorg는 parent_hash 체인 검증으로 reorg를 감지하고, 공통 조상까지의 롤백 대상을
// 계산한다. 인덱서가 reorg의 권위 있는 감지자다 (docs/03 §3, docs/06 §5, FR-11/12/13).
//
// 실제 롤백 실행(reorg 이벤트 발행 → block 삭제 → cursor 되돌리기)은 호출자(cmd/indexer의
// 메인 루프)가 담당한다 — 이 패키지는 "어디까지 롤백해야 하는가"만 계산하는 순수 로직이라
// DB나 노드 클라이언트 구현 없이 단위 테스트 가능하다 (reorg_test.go 참고).
package reorg

import "context"

// BlockStore는 인덱서가 저장해둔 블록 해시를 높이로 조회한다. 없으면 found=false.
type BlockStore interface {
	GetBlockHash(ctx context.Context, chainID string, height int64) (hash string, found bool, err error)
}

// NodeHasher는 체인 노드가 실제로 갖고 있는(현재 canonical) 블록 해시를 높이로 조회한다.
type NodeHasher interface {
	BlockHash(ctx context.Context, height int64) (string, error)
}

type Detector struct{}

func New() *Detector {
	return &Detector{}
}

// IsContinuous는 newParentHash가 저장된 (height-1) 블록의 hash와 일치하는지 검증한다
// (docs/03 §reorg isContinuous). 저장된 블록이 없으면(초기 동기화 구간) true.
func (d *Detector) IsContinuous(ctx context.Context, store BlockStore, chainID string, newHeight int64, newParentHash string) (bool, error) {
	storedHash, found, err := store.GetBlockHash(ctx, chainID, newHeight-1)
	if err != nil {
		return false, err
	}
	if !found {
		return true, nil
	}
	return storedHash == newParentHash, nil
}

// FindRollback은 docs/03 §3 handleReorg의 "공통 조상 찾기" 단계를 구현한다.
// newHeight에서 불연속이 감지된 뒤 호출한다고 가정하고, newHeight-1부터 저장된 해시와
// 노드의 실제 해시를 비교하며 거슬러 올라가 갈라진 지점(공통 조상)을 찾는다.
// 반환하는 rolledBack은 롤백 대상 해시 목록(높이 내림차순, 즉 orphan이 된 순서 역순)이고,
// commonAncestor는 그 다음부터 재추출해야 할 마지막 정상 높이다.
func (d *Detector) FindRollback(ctx context.Context, store BlockStore, node NodeHasher, chainID string, newHeight int64) (rolledBack []string, commonAncestor int64, err error) {
	h := newHeight - 1
	for {
		storedHash, found, err := store.GetBlockHash(ctx, chainID, h)
		if err != nil {
			return nil, 0, err
		}
		if !found {
			break // 더 이상 저장된 블록이 없음(초기 동기화 구간까지 도달) — 여기가 조상
		}
		actualHash, err := node.BlockHash(ctx, h)
		if err != nil {
			return nil, 0, err
		}
		if storedHash == actualHash {
			break // 공통 조상 발견
		}
		rolledBack = append(rolledBack, storedHash)
		h--
	}
	return rolledBack, h, nil
}
