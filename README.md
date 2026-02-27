# commmon

Windows COM 포트 시리얼 통신 도구. Rust CLI 데몬 + REPL + Node.js MCP 서버.

## 아키텍처

```
                  TCP (localhost:9900)
  ┌──────────┐        ┌──────────────┐
  │ REPL CLI ├───────>│              │
  └──────────┘        │  commmon     │      ┌──────────┐
                      │  daemon      ├─────>│ COM Port │
  ┌──────────┐        │              │      └──────────┘
  │ MCP서버  ├───────>│  (TCP서버 +  │
  │ (Node.js)│        │   시리얼관리 │──> monitor.html (HTTP :8765)
  └──────────┘        └──────────────┘
```

| 컴포넌트 | 언어 | 역할 |
|---|---|---|
| **commmon daemon** | Rust | TCP 서버, 시리얼 포트 관리, 로깅, 웹 모니터 |
| **commmon** (REPL) | Rust | 대화형 CLI, 데몬에 TCP 접속 |
| **MCP 서버** | Node.js | Claude Code 등 MCP 클라이언트용 어댑터 |

데몬이 시리얼 포트를 관리하고, REPL과 MCP 서버가 동시에 접속하여 동일한 포트 상태를 공유합니다.

## 기능

- COM 포트 열기/닫기 (baud rate, data bits, parity, stop bits 설정)
- 데이터 전송 (ASCII, HEX, UTF-8)
- 수신 데이터 읽기 (클라이언트별 독립 버퍼)
- 파일 로깅 (시간 제한, 키워드 자동 중지)
- 웹 모니터 (브라우저에서 실시간 시리얼 데이터 확인)
- 서버→클라이언트 Notification (로그 완료 알림 등)

## 프로젝트 구조

```
commmon/
  README.md
  PLAN.md                          설계 문서
  commmon/                         Rust CLI (데몬 + REPL)
    Cargo.toml
    src/
      main.rs                      엔트리포인트 (clap 서브커맨드)
      daemon.rs                    TCP 서버 + 클라이언트 세션 관리
      serial.rs                    시리얼 포트 관리 + 로깅
      repl.rs                      REPL CLI (rustyline)
      monitor.rs                   웹 모니터 HTTP/SSE 서버 (axum)
      monitor.html                 웹 모니터 UI
      protocol.rs                  TCP 프로토콜 JSON 타입 정의
  mcp-server/                      MCP 서버 (Node.js)
    package.json
    index.js                       데몬 TCP 클라이언트
```

## 설치

### 방법 1: 사전 빌드 바이너리 (권장)

[Releases](https://github.com/matthew-zb/commmon/releases) 페이지에서 최신 zip을 다운로드하여 압축 해제합니다.

```
commmon-v0.1.0-windows-x86_64.zip
  ├── commmon.exe          CLI 바이너리
  └── mcp-server/
      ├── package.json
      └── index.js
```

`commmon.exe`를 PATH가 잡힌 디렉토리에 복사합니다.

### 방법 2: 소스에서 빌드

#### 요구 사항

- Windows 10/11
- [Rust 툴체인](https://rustup.rs/) (1.70+)
- Visual Studio Build Tools (serialport 네이티브 바인딩에 C++ 컴파일러 필요)
- [Node.js](https://nodejs.org/) 18+ (MCP 서버 사용 시)

#### 빌드

```bash
git clone git@github.com:matthew-zb/commmon.git
cd commmon/commmon
cargo build --release
```

바이너리: `commmon/target/release/commmon.exe`

## 실행

### 1. 데몬 시작

별도 터미널에서 데몬을 실행합니다. 데몬이 시리얼 포트를 관리합니다.

```bash
commmon daemon              # 기본 포트 9900
commmon daemon --port 9900  # 포트 지정
```

Ctrl+C로 종료 시 모든 포트, 로그, 모니터가 자동 정리됩니다.

### 2. REPL 접속

다른 터미널에서 REPL로 데몬에 접속합니다.

```bash
commmon                # 기본 포트 9900의 데몬에 접속
commmon --port 9900    # 포트 지정
```

데몬이 실행 중이지 않으면 안내 메시지를 출력하고 종료합니다.

## REPL 명령어

```
commmon> list                            포트 목록 조회
commmon> open COM3 115200                포트 열기 (baud 생략 시 115200)
commmon> open COM3 115200 8N1            데이터비트/패리티/스톱비트 지정
commmon> close COM3                      포트 닫기

commmon> write COM3 hello                ASCII 전송
commmon> write COM3 --hex 48454C4C4F     HEX 전송
commmon> read COM3                       수신 버퍼 읽기

commmon> log start COM3                  로그 시작 (임시 디렉토리)
commmon> log start COM3 --file ./log.txt 파일 경로 지정
commmon> log start COM3 --duration 30    30초 후 자동 중지
commmon> log start COM3 --keyword OK     키워드 감지 시 중지
commmon> log update COM3 --keyword ERR   로그 키워드 변경
commmon> log stop COM3                   로그 중지

commmon> status                          전체 상태 (포트 + 모니터)
commmon> monitor start                   웹 모니터 시작 (http://127.0.0.1:8765)
commmon> monitor start --port 8765       모니터 포트 지정
commmon> monitor stop                    웹 모니터 종료

commmon> help                            도움말
commmon> exit                            종료
```

입력 히스토리는 `~/.commmon/history.txt`에 저장되어 다음 세션에서도 사용 가능합니다.

## MCP 서버 설치 (Claude Code)

MCP 서버는 commmon 데몬에 TCP로 연결합니다. **serialport 네이티브 의존성이 없으므로** C++ 빌드 도구 없이 `npm install`만으로 설치됩니다.

### 1. 의존성 설치

```bash
cd mcp-server
npm install
```

### 2. Claude Code 설정

`~/.claude.json` (전역) 또는 프로젝트 `.mcp.json`에 추가:

```json
{
  "mcpServers": {
    "com-port": {
      "command": "node",
      "args": ["/path/to/commmon/mcp-server/index.js"],
      "env": {
        "COMMMON_HOST": "127.0.0.1",
        "COMMMON_PORT": "9900"
      }
    }
  }
}
```

> `args`의 경로를 실제 설치 위치로 변경하세요. `env`는 기본값과 같으면 생략 가능합니다.

### 3. 사용 흐름

```
1. commmon daemon 실행 (별도 터미널)
2. Claude Code 시작 (MCP 서버가 데몬에 자동 연결)
3. Claude Code에서 시리얼 도구 사용
```

REPL과 MCP 서버를 동시에 사용할 수 있으며, 수신 데이터는 각 클라이언트에 독립적으로 복제됩니다.

### MCP 도구 목록

| 도구 | 설명 |
|---|---|
| `list_ports` | 사용 가능한 COM 포트 목록 조회 |
| `open_port` | COM 포트 열기 |
| `close_port` | COM 포트 닫기 |
| `write_port` | 데이터 전송 |
| `read_port` | 수신 버퍼 읽기 |
| `start_log` | 파일 로깅 시작 |
| `update_log` | 로그 설정 변경 |
| `stop_log` | 로그 중지 |
| `port_status` | 전체 상태 조회 |
| `open_monitor` | 웹 모니터 시작 |
| `close_monitor` | 웹 모니터 종료 |

## TCP 프로토콜

데몬과 클라이언트 간 newline-delimited JSON 통신:

```
→ {"cmd":"open_port","args":{"port":"COM3","baudRate":115200}}
← {"ok":true,"data":"COM3 열기 성공 (115200bps, 8N1)"}

← {"ok":false,"error":"COM3가 열려 있지 않습니다."}

← {"notify":"log_stopped","data":{"port":"COM3","reason":"keyword","keyword":"OK","file":"..."}}
```

## License

MIT
