package state

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool은 애플리케이션 코드(NewPool 자체)와 sqlc가 생성하는 Queries(db.go, models.go, *.sql.go)를
// 구분하기 위해 별도 파일에 둔다 — sqlc generate는 이 패키지의 다른 파일들을 덮어쓴다.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, dsn)
}
