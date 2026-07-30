#!/bin/bash
# docker-entrypoint-initdb.d 관례로 최초 초기화 시 0001_init.sql 다음에 자동 실행된다.
# superuser(postgres)와 분리된 애플리케이션 전용 유저를 최소 권한으로 생성 (안전 수칙: 최소 권한 원칙).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
      IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${POSTGRES_APP_USER}') THEN
        CREATE ROLE ${POSTGRES_APP_USER} LOGIN PASSWORD '${POSTGRES_APP_PASSWORD}';
      END IF;
    END
    \$\$;
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_APP_USER};
    GRANT USAGE, CREATE ON SCHEMA public TO ${POSTGRES_APP_USER};
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ${POSTGRES_APP_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO ${POSTGRES_APP_USER};
EOSQL
