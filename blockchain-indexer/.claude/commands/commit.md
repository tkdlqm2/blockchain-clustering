---
allowed-tools: Bash(git add:*), Bash(git status:*), Bash(git commit:*), Bash(git diff:*), Bash(git log:*), Read, Grep
argument-hint: [message] | --no-verify | --amend
description: Conventional commit 형식을 사용하여 잘 포맷된 커밋 생성
model: sonnet
---

# Smart Git Commit

잘 포맷된 커밋 생성: $ARGUMENTS

## 현재 저장소 상태

- Git 상태: !`git status --porcelain`
- 현재 브랜치: !`git branch --show-current`
- 스테이징된 변경사항: !`git diff --cached --stat`
- 스테이징되지 않은 변경사항: !`git diff --stat`
- 최근 커밋: !`git log --oneline -5`

## 이 명령어가 하는 일

1. `--no-verify`로 지정하지 않는 한, 자동으로 pre-commit 체크를 실행합니다. **이 프로젝트의 언어/빌드 도구를 먼저 확인한 뒤(package.json, go.mod, pom.xml, pyproject.toml 등) 그에 맞는 커맨드를 사용합니다** — 다른 언어의 커맨드를 그대로 가져다 쓰지 않습니다:
   - Node/TS: `package.json`의 `lint`/`build`/`typecheck` 스크립트
   - Go: `gofmt -l .`, `go vet ./...`, `go build ./...`
   - Java/Kotlin: `./gradlew check` 또는 `mvn verify`
   - Python: `ruff`/`flake8` + 프로젝트에 정의된 테스트 커맨드
   - 위에 없는 언어/구성이면 `README.md`나 `Makefile`, CI 설정 파일에서 실제 사용하는 lint/build 커맨드를 찾아서 사용
2. `git status`로 스테이징된 파일 확인
3. 스테이징된 파일이 0개인 경우, 자동으로 모든 수정된 파일과 새 파일을 `git add`로 추가
4. 커밋할 변경사항을 이해하기 위해 `git diff` 수행
5. diff를 분석하여 여러 개의 서로 다른 논리적 변경사항이 있는지 확인
6. 여러 개의 서로 다른 변경사항이 감지되면, 커밋을 여러 개의 작은 커밋으로 나누는 것을 제안
7. 각 커밋(또는 나누지 않은 단일 커밋)에 대해 conventional commit 형식을 사용하여 커밋 메시지 생성

## 커밋 모범 사례

- **커밋 전 확인**: 코드가 린트되었고, 빌드가 올바르게 되며, 문서가 업데이트되었는지 확인
- **원자적 커밋**: 각 커밋은 단일 목적을 제공하는 관련 변경사항을 포함해야 함
- **큰 변경사항 분할**: 변경사항이 여러 관심사를 다루는 경우, 별도의 커밋으로 분할
- **Conventional commit 형식**: `<type>: <description>` 형식을 사용하며, type은 다음 중 하나:
  - `feat`: 새로운 기능
  - `fix`: 버그 수정
  - `docs`: 문서 변경
  - `style`: 코드 스타일 변경 (포맷팅 등)
  - `refactor`: 버그 수정도 기능 추가도 아닌 코드 변경
  - `perf`: 성능 개선
  - `test`: 테스트 추가 또는 수정
  - `chore`: 빌드 프로세스, 도구 등의 변경
- **현재형, 명령형**: 커밋 메시지를 명령으로 작성 (예: "add feature"가 "added feature"가 아님)
- **간결한 첫 줄**: 첫 줄을 72자 이하로 유지

## 커밋 분할 가이드라인

diff를 분석할 때 다음 기준에 따라 커밋을 분할하는 것을 고려하세요:

1. **다른 관심사**: 코드베이스의 관련 없는 부분에 대한 변경
2. **다른 유형의 변경**: 기능, 수정, 리팩토링 등을 혼합
3. **파일 패턴**: 다른 유형의 파일에 대한 변경 (예: 소스 코드 vs 문서)
4. **논리적 그룹화**: 별도로 이해하거나 리뷰하기 더 쉬운 변경사항
5. **크기**: 매우 큰 변경사항으로, 분해하면 더 명확해질 수 있는 경우

## 예시

좋은 커밋 메시지:
- feat: 사용자 인증 시스템 추가
- fix: 렌더링 프로세스의 메모리 누수 해결
- docs: 새로운 엔드포인트로 API 문서 업데이트
- refactor: 파서의 에러 처리 로직 단순화
- fix: 컴포넌트 파일의 린터 경고 해결
- chore: 개발자 도구 설정 프로세스 개선
- feat: 트랜잭션 검증을 위한 비즈니스 로직 구현
- fix: 헤더의 사소한 스타일 불일치 수정
- fix: 인증 플로우의 중요한 보안 취약점 패치
- style: 가독성 향상을 위한 컴포넌트 구조 재구성
- fix: 사용되지 않는 레거시 코드 제거
- feat: 사용자 등록 폼에 입력 검증 추가
- fix: 실패한 CI 파이프라인 테스트 해결
- feat: 사용자 참여를 위한 분석 추적 구현
- fix: 인증 비밀번호 요구사항 강화
- feat: 스크린 리더를 위한 폼 접근성 개선

커밋 분할 예시:
- 첫 번째 커밋: feat: 새로운 solc 버전 타입 정의 추가
- 두 번째 커밋: docs: 새로운 solc 버전에 대한 문서 업데이트
- 세 번째 커밋: chore: package.json 의존성 업데이트
- 네 번째 커밋: feat: 새로운 API 엔드포인트에 대한 타입 정의 추가
- 다섯 번째 커밋: feat: 워커 스레드의 동시성 처리 개선
- 여섯 번째 커밋: fix: 새 코드의 린팅 문제 해결
- 일곱 번째 커밋: test: 새로운 solc 버전 기능에 대한 단위 테스트 추가
- 여덟 번째 커밋: fix: 보안 취약점이 있는 의존성 업데이트

## 명령어 옵션

- `--no-verify`: pre-commit 체크 실행 건너뛰기 (lint/vet/build, 프로젝트 언어에 맞게 자동 감지)

## 중요 사항

- 기본적으로 이 프로젝트의 언어에 맞는 pre-commit 체크(lint, vet/typecheck, build)가 실행되어 코드 품질을 보장합니다
- 이러한 체크가 실패하면, 문제를 먼저 수정할지 아니면 그대로 커밋을 진행할지 물어봅니다
- 특정 파일이 이미 스테이징되어 있으면, 해당 파일만 커밋합니다
- 스테이징된 파일이 없으면, 자동으로 모든 수정된 파일과 새 파일을 스테이징합니다
- 커밋 메시지는 감지된 변경사항을 기반으로 구성됩니다
- 커밋하기 전에 명령어가 diff를 검토하여 여러 커밋이 더 적절한지 확인합니다
- 여러 커밋을 제안하는 경우, 변경사항을 별도로 스테이징하고 커밋하는 것을 도와줍니다
- 항상 커밋 diff를 검토하여 메시지가 변경사항과 일치하는지 확인합니다
- 커밋 메시지는 한글로 작성해주세요
- 이모지, 이모티콘은 사용하지 마세요.
- 커밋 메시지에 🤖 Generated with Claude Code, Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com> 이 속성은 표시하지 않기