# commmon — 배포판 사용 안내

Windows COM 포트 시리얼 통신 도구. 데몬 바이너리 + MCP 서버 + 설치 스크립트가 함께 들어 있습니다.

## 패키지 구성

```
commmon-{platform}-{arch}/
├── commmon(.exe)              Rust 데몬/REPL 바이너리
├── mcp-server/
│   ├── index.js               MCP 서버 엔트리
│   ├── package.json
│   └── node_modules/          (번들된 의존성)
├── install.ps1                Windows 설치 스크립트
├── install.sh                 Linux/macOS 설치 스크립트
└── RELEASE_README.md          이 파일
```

## 사전 요구사항

- **Node.js 18+** — MCP 서버 실행에 필요
- **Claude Code** — MCP 등록에 `claude` CLI 사용

## 설치

### Windows

PowerShell에서:
```powershell
cd <압축 해제 경로>
.\install.ps1
```

기본 동작:
- `claude mcp add com-port --scope user -- node <경로>\mcp-server\index.js`
- 환경 변수 `COMMMON_HOST=127.0.0.1`, `COMMMON_PORT=9900` 설정

옵션:
```powershell
.\install.ps1 -Scope project        # 프로젝트 범위(.mcp.json)로 등록
.\install.ps1 -Name commmon         # 다른 이름으로 등록
.\install.ps1 -Port 9901            # 데몬 포트 변경
```

### Linux / macOS

```bash
chmod +x install.sh commmon
./install.sh
```

옵션:
```bash
./install.sh --scope project
./install.sh --name commmon
./install.sh --port 9901
```

## 실행 흐름

MCP 서버는 데몬(TCP)에 연결해서 동작합니다. 반드시 **데몬을 먼저 실행**해야 합니다.

1. **데몬 실행** (별도 터미널에 상시 유지):
   ```
   # Windows
   .\commmon.exe daemon

   # Linux / macOS
   ./commmon daemon
   ```

2. **Claude Code 재시작**

3. Claude Code에서 `/mcp` 입력 → `com-port` 서버가 `connected` 상태인지 확인

4. 사용 가능한 도구: `list_ports`, `open_port`, `close_port`, `write_port`, `read_port`, `subscribe_rx`, `unsubscribe_rx`, `read_rx_stream`, `start_log`, `update_log`, `stop_log`, `port_status`, `open_monitor`, `close_monitor` (총 14개)

## 제거

```bash
# MCP 등록만 해제
claude mcp remove com-port --scope user

# 이후 배포 폴더 삭제
```

## 트러블슈팅

| 증상 | 원인 | 해결 |
|---|---|---|
| `/mcp`에서 `com-port`가 `failed` | 데몬 미실행 | `commmon daemon` 먼저 실행 |
| `데몬 연결이 끊어졌습니다` 오류 | 데몬이 중간에 종료됨 | 데몬 재실행 |
| `claude: command not found` | Claude Code 미설치 | Claude Code 설치 필요 |
| `node: command not found` | Node.js 미설치 | Node.js 18+ 설치 |
| 포트 충돌 | 9900 포트 사용 중 | `commmon daemon --port 9901` + 설치 스크립트에 `-Port 9901` |

## 수동 등록 (설치 스크립트 없이)

```bash
claude mcp add com-port --scope user \
    -e COMMMON_HOST=127.0.0.1 -e COMMMON_PORT=9900 \
    -- node /abs/path/to/mcp-server/index.js
```

또는 `~/.claude.json` 직접 편집:
```json
{
  "mcpServers": {
    "com-port": {
      "command": "node",
      "args": ["/abs/path/to/mcp-server/index.js"],
      "env": {
        "COMMMON_HOST": "127.0.0.1",
        "COMMMON_PORT": "9900"
      }
    }
  }
}
```
