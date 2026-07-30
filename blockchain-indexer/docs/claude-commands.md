# Claude Code 커스텀 커맨드 모음

Claude Code의 커스텀 슬래시 커맨드(`.claude/commands/*.md`) 모음입니다. 크게 두 종류로 구성됩니다.

- **리뷰 패밀리 (9개)**: DB, 통신, 동시성, 아키텍처, 정합성, 예외 처리, API 계약, 입력 검증, 보안을 각각 **딱 하나씩만** 담당 — 여러 개를 이어서 돌려도 같은 이슈가 중복 보고되지 않도록 설계.
- **`/init_project`**: 신규 프로젝트를 기술스택·인프라·도메인 인터뷰로 스캐폴딩하는 커맨드.
- **`/commit`**: conventional commit 형식으로 커밋 생성. 이 프로젝트의 언어/빌드 도구를 먼저 감지해서 그에 맞는 lint/build를 pre-commit 체크로 실행(하드코딩 아님).
- **`/report`**: 주간 업무 보고서 생성.

## 설치

이 폴더를 그대로 복사해서 쓰면 됩니다.

- **프로젝트 전용으로 쓰려면**: `.claude/commands/` 폴더를 프로젝트 루트에 그대로 복사
  ```bash
  cp -r .claude <내-프로젝트-경로>/
  ```
- **모든 프로젝트에서 공통으로 쓰려면**: 안의 `.md` 파일들을 `~/.claude/commands/`(사용자 홈 디렉토리)로 복사
  ```bash
  cp .claude/commands/*.md ~/.claude/commands/
  ```

설치 후 Claude Code에서 `/db_review`처럼 파일 이름으로 바로 호출할 수 있습니다.

## 커맨드 목록

| 커맨드 | 한 줄 요약 | 사용법 |
|---|---|---|
| `/db_review` | 단일 DB 쿼리·트랜잭션 병목 (N+1, 카티전 곱, 인덱스 미스, 락 경합, 커넥션 풀) | `/db_review [경로]` \| `/db_review --all` \| 인자 없음(기본값: diff만) |
| `/comm_review` | 외부 통신(HTTP/gRPC/브로커/서드파티 SDK/웹훅)의 정상 동작 + 장애 회복력(서킷브레이커/벌크헤드/재시도 폭풍) | `/comm_review [경로 또는 통합 이름]` |
| `/concurrency_review` | 프로세스 내부 동시성(스레드/고루틴/락/풀/비동기) | `/concurrency_review [경로 또는 컴포넌트]` |
| `/arch_review` | 레이어·바운디드 컨텍스트 경계, 의존성 방향 위반 | `/arch_review [경로 또는 모듈]` |
| `/consistency_review` | 여러 리소스/서비스에 걸친 쓰기 정합성(outbox/saga/멱등성/이벤트 순서) | `/consistency_review [경로 또는 유스케이스]` |
| `/error_review` | 예외 처리 전략(에러→상태코드 매핑, swallowed exception, 실패 시 자원 정리) | `/error_review [경로 또는 컴포넌트]` |
| `/api_review` | 우리가 외부에 제공하는 API 계약(버전 관리, 하위 호환, 페이지네이션, 엔티티 노출) | `/api_review [경로 또는 엔드포인트]` |
| `/validation_review` | 입력 검증이 애초에 존재하는가(경계+도메인 이중 방어, injection 방지) | `/validation_review [경로 또는 진입점]` |
| `/security_review` | 인증/인가(IDOR), 시크릿 하드코딩, 민감정보 로깅, rate limiting | `/security_review [경로 또는 엔드포인트]` |
| `/init_project` | 기술스택/인프라/도메인/외부통신 인터뷰 → 신규 프로젝트 스캐폴딩 | `/init_project [프로젝트명 또는 경로]` |
| `/commit` | 언어/빌드 도구 자동 감지 후 conventional commit 형식으로 커밋 생성 | `/commit [메시지]` \| `--no-verify` \| `--amend` |
| `/report` | 이번 주 git 커밋 메시지 기반 주간 업무 보고서 생성 | `/report` |

인자 없이 실행하면 프로젝트 전체를 스캔하고, 경로나 이름을 주면 그 범위로 한정합니다.

## 리뷰 커맨드 8개의 공통 구조

`db_review`를 제외한 8개(`comm`/`concurrency`/`arch`/`consistency`/`error`/`api`/`validation`/`security`)는 모두 같은 3단계 구조를 따릅니다.

1. **STEP 1 — 디스커버리**: 코드베이스에 실제로 그 관심사에 해당하는 것이 있는지부터 찾습니다. 없으면 체크리스트를 억지로 채우지 않고, "찾지 못함 — 경로를 알려달라"고 한 번만 물어봅니다.
2. **STEP 2 — 대상별 점검**: STEP 1에서 발견된 종류에 해당하는 항목만 점검합니다. 프로젝트에 없는 기술(예: gRPC를 안 쓰는데 gRPC 항목)은 만들어내지 않습니다.
3. **STEP 3 — 심각도별 출력**: `Critical → High → Medium → Low` 순으로 정렬하고, 마지막에 통합/컴포넌트 × 점검 항목 요약 표와 심각도별 개수를 출력합니다.

공통 규칙:
- **DO**: 실제 `파일:라인`을 인용. 위치를 못 찾으면 "확인 불가"라고만 표시.
- **DO**: 문제뿐 아니라 잘 지켜진 부분도 ✅로 표시.
- **DO-NOT**: 범위 밖 이슈는 지적하지 않고 담당 커맨드로만 안내.
- **DO-NOT**: 근거 없는 추측성 지적 금지.

## 이슈 라우팅 표 (겹치는 주제는 누가 담당하는가)

| 주제 | 담당 커맨드 | 비고 |
|---|---|---|
| gRPC 스키마 진화 | `comm_review`(파싱 안전성) / `api_review`(계약 관리 프로세스) | 같은 코드, 다른 관점 |
| 상태코드 | `api_review`(설계된 체계) / `error_review`(예외 매핑 일관성) | |
| 도메인 엔티티 노출 | `api_review`(하위호환 리스크) / `arch_review`(계층 위반 자체) | |
| SQL/커맨드 인젝션 | `validation_review` 단독 소유 | `security_review`에서 제외 |
| 시크릿·민감정보 로깅 | `security_review` 단독 소유 | `comm_review`에서 제외 |
| 페이지네이션 | `db_review`(성능) / `api_review`(계약 안전성, offset의 중복·누락 위험) | |
| 도메인 계층 누수 | 각 커맨드가 발견 시 지적 | 최종 정리는 `arch_review` |

## `/init_project` 사용법

리뷰 패밀리와 성격이 다릅니다 — 기존 코드를 점검하는 게 아니라 **인터뷰를 거쳐 신규 프로젝트를 만들어냅니다.**

### 언제 쓰는가

새 리포지토리를 막 만들었거나(또는 만들 예정이거나), 빈 디렉토리에서 백엔드 서비스를 처음 세팅할 때. 이미 코드가 어느 정도 쌓인 프로젝트에는 적합하지 않습니다(그런 경우엔 리뷰 패밀리를 쓰세요).

### 호출 방법

```
/init_project                # 현재 디렉토리를 대상으로
/init_project my-new-service # 지정한 이름/경로에 새로 생성
```

디렉토리가 비어있지 않으면 기존 파일 목록을 먼저 보여주고, 진행 여부를 확인받습니다 — 조용히 덮어쓰지 않습니다.

### 진행 흐름 (8단계, 대화형)

질문(STEP 1~6)이 먼저 끝나야 생성(STEP 7~8)이 시작됩니다. 한 번에 다 묻지 않고 섹션 단위로 자연스럽게 이어집니다.

| STEP | 내용 | 형식 |
|---|---|---|
| 1. 기술스택 | 언어/프레임워크, DB, 메시지 큐·캐시 등 | 선택형(AskUserQuestion) |
| 2. 인프라 | docker-compose 적용 범위(db/redis/브로커), 버전·포트·볼륨, **실제 랜덤 자격증명 생성**(`openssl rand`) | 선택형 + 자동 생성 |
| 3. 설정 체계 | config 파일 형식·위치, 환경별 분리, 시크릿 관리 방향 | 선택형 |
| 4. 서비스 설명·도메인 | 이 서비스가 하는 일, 핵심 도메인, 시스템 내 위치 | 자유 서술 |
| 5. 외부 통신 | 연동 서비스 목록 + **API/통신 명세서 공유 요청** | 자유 서술 + 파일/텍스트 첨부 |
| 6. 동적 추가 질문 | 인증 방식, 배포 대상, 테스트 전략, 트래픽 규모, 규제 요구사항 등 **이 프로젝트에 실제로 필요한 것만** | 선택형/자유 서술 혼합 |
| 7. 생성 | 디렉토리 골격, `docker-compose.yml`, `.env`/`.env.example`, config, `CLAUDE.md`, `README.md` | 파일 생성 |
| 8. 완료 보고 | 생성 파일 목록 + 사용자가 채워야 할 TODO 정리 | 요약 출력 |

### 답변할 때 팁

- **STEP 5(외부 통신)**: "그냥 REST API 하나 붙일 거예요" 대신, 실제 OpenAPI 문서/`.proto` 파일 경로나 요청·응답 예시(curl, JSON)를 붙여넣어 주세요. 명세가 없으면 없다고 말해도 됩니다 — 추측 대신 TODO로 남습니다.
- **STEP 6(동적 질문)**: 가벼운 사이드 프로젝트면 "그냥 최소한으로만" 이라고 답해도 됩니다. 결제·금융·의료처럼 규제가 있는 도메인이면 질문이 더 늘어나는 게 정상입니다.
- 중간에 답변을 바꾸고 싶으면 그냥 이어지는 대화에서 다시 말하면 됩니다 — 매번 처음부터 다시 실행할 필요 없습니다.

### 생성 결과 예시

```
my-new-service/
├── CLAUDE.md              # 서비스 설명·도메인·기술스택·외부 통신 목록
├── README.md              # 개요 + 로컬 실행 방법
├── docker-compose.yml     # STEP 2에서 고른 서비스만 (db, redis 등)
├── .env                   # 실제 생성된 개발용 시크릿 (git 커밋 안 됨)
├── .env.example           # placeholder만
├── configs/
│   └── config.yaml        # STEP 3에서 정한 형식대로
└── docs/api/               # STEP 5에서 받은 외부 API 명세서 원본
```

### 생성 후 체크리스트

- `docker compose config`로 `docker-compose.yml` 문법 검증 결과 확인
- `.env`가 `.gitignore`에 걸려 있는지 확인
- 완료 보고에 나온 TODO(실제 운영 시크릿, 아직 못 받은 API 명세서 등) 채우기
- 실제로 띄워보고 싶으면 `docker compose up`은 사용자가 직접 실행(커맨드가 자동 실행하지 않음)

### 안전 수칙

- 기존 파일은 확인 없이 덮어쓰지 않습니다.
- 시크릿은 항상 `openssl rand` 등으로 실제 랜덤 생성 — 추측 가능한 기본값(`password123` 등) 사용 안 함.
- `docker compose up`처럼 리소스를 점유/실행하는 명령은 사용자 승인 없이 실행하지 않습니다(`docker compose config` 검증까지만).

## 사용 예시

```
/db_review internal/repository
/comm_review --all
/security_review src/api/admin
/init_project my-new-service
```

## 참고

- `db_review`는 가장 먼저 만들어져서 위 STEP 1/2/3 형식이 아닌 단순 체크리스트 구조입니다. 기능은 동일하게 동작하지만, 나머지와 포맷을 통일하고 싶다면 리팩터링이 필요합니다.
- 모든 커맨드는 CLAUDE.md에 아키텍처 규칙이나 레이어 구조가 문서화되어 있으면 그 내용을 우선 인용하도록 설계되어 있습니다. 프로젝트에 CLAUDE.md가 있다면 리뷰 정확도가 올라갑니다.
