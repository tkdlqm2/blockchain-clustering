-- name: GetCursor :one
SELECT * FROM cursor WHERE chain_id = $1;

-- name: UpsertCursor :exec
-- 발행 완료 후에만 호출한다 (docs/03 §0 writeCursor, FR-16).
INSERT INTO cursor (chain_id, height) VALUES ($1, $2)
ON CONFLICT (chain_id) DO UPDATE SET height = EXCLUDED.height;
