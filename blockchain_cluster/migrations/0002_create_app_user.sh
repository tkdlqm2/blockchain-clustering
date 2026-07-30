#!/bin/bash
# 최소 권한 애플리케이션 유저 생성.
# merge_evidence(그리고 balance_delta/address/label 등)는 DELETE 권한을 주지 않는다 —
# merge_evidence는 append-only 불변식(02-data-model.md §3) 때문이고, 나머지는 애초에
# 삭제할 이유가 없기 때문이다.
# 단, cluster/cluster_membership은 반대로 "언제든 전체 재생 가능한 파생 캐시"(02 §5)라서
# ClusterStore.RebuildFromEvidence()가 매번 DELETE 후 재삽입한다 — 이 두 테이블만 예외로
# DELETE를 허용한다. (부모 파티션 테이블에 대한 GRANT이므로 chain별 파티션에도 자동 적용된다.)
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${DB_APP_USER}') THEN
            CREATE ROLE ${DB_APP_USER} LOGIN PASSWORD '${DB_APP_PASSWORD}';
        END IF;
    END
    \$\$;

    ALTER ROLE ${DB_APP_USER} SET search_path = clustering, public;

    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${DB_APP_USER};
    GRANT USAGE ON SCHEMA clustering TO ${DB_APP_USER};

    GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA clustering TO ${DB_APP_USER};
    GRANT DELETE ON clustering.cluster, clustering.cluster_membership TO ${DB_APP_USER};
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA clustering TO ${DB_APP_USER};
    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA clustering TO ${DB_APP_USER};

    ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA clustering
        GRANT SELECT, INSERT, UPDATE ON TABLES TO ${DB_APP_USER};
    ALTER DEFAULT PRIVILEGES FOR ROLE ${POSTGRES_USER} IN SCHEMA clustering
        GRANT USAGE, SELECT ON SEQUENCES TO ${DB_APP_USER};
EOSQL
