-- name: GetBlockAtHeight :one
-- reorg 연속성 검증의 근거 (docs/03 §reorg isContinuous).
SELECT * FROM block WHERE chain_id = $1 AND height = $2;

-- name: InsertBlock :exec
-- ON CONFLICT DO NOTHING: 발행 성공 후 커서 갱신 전에 크래시가 나서 같은 높이를 재처리하더라도
-- (chain_id, hash) PK 충돌로 죽지 않고 그대로 재시도할 수 있어야 한다 (FR-16, 멱등 재처리).
INSERT INTO block (chain_id, height, hash, parent_hash, "timestamp")
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (chain_id, hash) DO NOTHING;

-- name: DeleteBlocksFromHeight :exec
-- reorg 롤백 시 orphan 블록 제거 (docs/03 §reorg handleReorg).
DELETE FROM block WHERE chain_id = $1 AND height >= $2;
