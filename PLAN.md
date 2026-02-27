# commmon — Rust COM Port CLI + Node.js MCP Server

## Context
기존 Node.js `com-port-mcp-server`의 기능을 Rust CLI + Node.js MCP 래퍼로 재구현.
CLI를 독립적으로 셸에서도 사용 가능하게 하고, MCP 서버는 CLI 데몬에 TCP로 연결.

## 아키텍처

```
                  TCP (localhost:9900)
  ┌──────────┐        ┌──────────────┐
  │ REPL CLI ├───────►│              │
  └──────────┘        │  commmon     │      ┌──────────┐
                      │  daemon      ├─────►│ COM Port │
  ┌──────────┐        │              │      └──────────┘
  │ MCP서버  ├───────►│  (TCP서버 +  │
  │ (Node.js)│        │   시리얼관리 │──► monitor.html (HTTP :8765)
  └──────────┘        └──────────────┘
```

**3개 컴포넌트:**
1. **commmon daemon** (Rust) — TCP 서버 + 시리얼 포트 관리 + 웹 모니터
2. **commmon** (Rust REPL) — TCP 클라이언트, 대화형 인터페이스
3. **MCP 서버** (Node.js) — TCP 클라이언트, MCP 도구 → JSON 명령 변환

## 설계 결정 사항

| 항목 | 결정 | 이유 |
|---|---|---|
| read_port 버퍼 정책 | 클라이언트별 복제 | REPL과 MCP 동시 접속 시 양쪽 모두 전체 데이터를 볼 수 있어야 함 |
| 데몬 접속 실패 | 안내 메시지 후 종료 | 단순하고 명확. 자동 시작은 프로세스 관리 복잡도 증가 |
| 서버→클라이언트 푸시 | notification 타입 지원 | 로그 키워드 감지 등 비동기 이벤트를 실시간으로 전달 |
| HTTP/SSE 크레이트 | axum | SSE 내장 지원, 보일러플레이트 적음, tokio 기반 |

### 클라이언트별 버퍼 복제 구현

- 데몬이 시리얼 수신 데이터를 `broadcast` 채널로 전송
- 각 TCP 클라이언트가 접속 시 자체 수신 버퍼(최대 200 엔트리)를 보유
- `read_port`는 해당 클라이언트의 버퍼만 소비 — 다른 클라이언트에 영향 없음
- 클라이언트 접속 이전 데이터는 받지 못함 (접속 시점부터 수신)

## 1. TCP 프로토콜

라인 구분 JSON (newline-delimited JSON):

### 요청-응답

```
→ 요청: {"cmd":"open_port","args":{"port":"COM3","baudRate":115200}}\n
← 응답: {"ok":true,"data":"COM3 열기 성공 (115200bps, 8N1)"}\n

→ 요청: {"cmd":"list_ports"}\n
← 응답: {"ok":true,"data":[{"path":"COM3","manufacturer":"Silicon Labs",...}]}\n

← 에러: {"ok":false,"error":"COM3가 열려 있지 않습니다."}\n
```

### 서버→클라이언트 Notification

요청 없이 서버가 보내는 비동기 이벤트. `notify` 필드로 구분:

```
← {"notify":"log_stopped","data":{"port":"COM3","reason":"keyword","keyword":"OK","file":"/tmp/log.txt"}}\n
← {"notify":"port_error","data":{"port":"COM3","error":"장치 연결 해제"}}\n
```

클라이언트는 notification을 무시해도 동작에 영향 없음 (MCP 서버는 무시, REPL은 출력).

## 2. Rust CLI (`commmon/`)

### Cargo.toml 의존성

| 크레이트 | 용도 |
|---|---|
| `tokio` (full) | 비동기 런타임 |
| `tokio-serial` + `serialport` | 시리얼 통신 |
| `serde` + `serde_json` | JSON 직렬화 |
| `chrono` | 타임스탬프 |
| `hex` | hex 인코딩 |
| `axum` | 모니터 HTTP/SSE (SSE 내장 지원) |
| `tokio::sync::broadcast` | 시리얼 수신 데이터 클라이언트별 복제 |
| `clap` | CLI 인자 파싱 |
| `rustyline` | REPL 라인 에디팅 (히스토리, 자동완성) |
| `tokio-util` | CancellationToken |
| `tracing` + `tracing-subscriber` | 로깅 |

### 서브커맨드

```bash
commmon                    # REPL 모드 (데몬에 TCP 접속)
commmon daemon             # 데몬 모드 (TCP 서버, 기본 포트 9900)
commmon daemon --port 9900 # 데몬 포트 지정
```

### 데몬 모드 (`commmon daemon`)

- TCP 서버 (기본 localhost:9900)
- 클라이언트 접속 시 라인 단위 JSON 명령 처리
- 시리얼 포트 상태를 메모리에 관리 (기존 Node.js의 openPorts Map과 동일)
- 모니터 HTTP 서버도 데몬이 직접 서빙
- SIGINT 시 모든 포트/로그/모니터 정리 후 종료

### REPL 모드 (`commmon`)

- 데몬에 TCP 접속 (접속 실패 시 안내 메시지)
- 프롬프트: `commmon> `
- 명령어:

```
commmon> list                          # 포트 목록
commmon> open COM3 115200              # 포트 열기 (baud 생략 시 115200)
commmon> open COM3 115200 8N1          # 포트 열기 (데이터비트/패리티/스톱비트)
commmon> write COM3 hello              # ASCII 전송
commmon> write COM3 --hex 48454C4C4F   # HEX 전송
commmon> read COM3                     # 수신 버퍼 읽기
commmon> log start COM3                # 로그 시작 (임시 디렉토리에 자동 생성)
commmon> log start COM3 --file ./log.txt  # 파일 경로 지정
commmon> log start COM3 --duration 30  # 30초 후 자동 중지
commmon> log start COM3 --keyword OK   # 키워드 감지 시 중지
commmon> log update COM3 --keyword ERR # 로그 키워드 변경
commmon> log stop COM3                 # 로그 중지
commmon> close COM3                    # 포트 닫기
commmon> status                        # 전체 상태
commmon> monitor start                 # 웹 모니터 시작
commmon> monitor start --port 8765     # 모니터 포트 지정
commmon> monitor stop                  # 웹 모니터 종료
commmon> help                          # 도움말
commmon> exit                          # 종료
```

- 내부적으로 REPL 명령을 JSON으로 변환하여 데몬에 전송
- 응답을 사람이 읽기 쉬운 형태로 포맷팅

### 프로젝트 구조

```
commmon/
  Cargo.toml
  src/
    main.rs          — 엔트리포인트, clap 서브커맨드 분기
    daemon.rs        — TCP 서버 + 명령 디스패치
    serial.rs        — 시리얼 포트 관리 (open/close/write/read/log)
    repl.rs          — REPL 모드 (TCP 클라이언트 + rustyline)
    protocol.rs      — 요청/응답 JSON 타입 정의
    monitor.rs       — HTTP/SSE 모니터 서버
    monitor.html     — 웹 UI HTML (include_str!)
```

## 3. Node.js MCP 서버 (`com-port-mcp-server/`)

기존 `com-port-mcp-server/index.js`를 수정:
- 직접 serialport를 사용하는 대신 commmon 데몬에 TCP 연결
- 각 MCP 도구 호출 시 JSON 명령을 데몬에 전송하고 응답을 MCP 결과로 변환
- `serialport` 의존성 제거, TCP 클라이언트만 사용 (net 모듈)

또는 기존 파일은 그대로 두고 별도 파일로 만들 수도 있음.

## 4. 구현 순서

### Phase 1: 프로토콜 + 데몬 스켈레톤
- [ ] Cargo.toml 생성
- [ ] `protocol.rs` — 요청/응답 타입 정의
- [ ] `daemon.rs` — TCP 서버 기본 구조 (접속, 명령 파싱, 라우팅)
- [ ] `main.rs` — clap으로 daemon 서브커맨드

### Phase 2: 시리얼 핵심 기능
- [ ] `serial.rs` — list_ports, open_port, close_port, write_port, read_port
- [ ] 데몬에서 시리얼 명령 연동

### Phase 3: 로깅
- [ ] `serial.rs` — start_log, update_log, stop_log

### Phase 4: REPL
- [ ] `repl.rs` — TCP 클라이언트 + 명령 파싱 + 출력 포맷팅

### Phase 5: 모니터
- [ ] `monitor.rs` + `monitor.html` — HTTP/SSE 서버
- [ ] open_monitor, close_monitor 명령

### Phase 6: MCP 서버
- [ ] Node.js MCP 서버 수정 또는 새로 생성

### Phase 7: 마무리
- [ ] SIGINT 클린업
- [ ] cargo build --release
- [ ] 통합 테스트

## 5. 검증

1. `cargo build --release` 성공
2. `commmon daemon` 실행 후 셸에서 `commmon` REPL로 포트 열기/쓰기/읽기
3. Node.js MCP 서버로 Claude Code에서 동일 기능 동작 확인
4. 웹 모니터 브라우저에서 확인
5. 데몬에 REPL과 MCP 동시 접속하여 상태 공유 확인
