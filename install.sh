#!/usr/bin/env bash
# commmon MCP 서버를 Claude Code에 등록하는 Linux/macOS 설치 스크립트.
#
# 사용법:
#   ./install.sh                          # user scope 기본
#   ./install.sh --scope project          # project scope
#   ./install.sh --name commmon           # 이름 변경
#   ./install.sh --host 127.0.0.1 --port 9900

set -euo pipefail

SCOPE="user"
NAME="com-port"
DAEMON_HOST="127.0.0.1"
PORT="9900"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --scope) SCOPE="$2"; shift 2;;
        --name) NAME="$2"; shift 2;;
        --host) DAEMON_HOST="$2"; shift 2;;
        --port) PORT="$2"; shift 2;;
        -h|--help)
            grep -E '^#' "$0" | sed 's/^# \{0,1\}//'
            exit 0;;
        *) echo "알 수 없는 옵션: $1" >&2; exit 1;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MCP_DIR="$SCRIPT_DIR/mcp-server"
MCP_ENTRY="$MCP_DIR/index.js"
DAEMON_BIN="$SCRIPT_DIR/commmon"

red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
cyan()  { printf '\033[0;36m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[0;33m%s\033[0m\n' "$*"; }

cyan "=== commmon MCP 서버 설치 ==="

# 1) 파일 존재 확인
if [[ ! -f "$MCP_ENTRY" ]]; then
    red "[ERROR] MCP 서버 파일을 찾을 수 없습니다: $MCP_ENTRY"
    exit 1
fi
if [[ ! -x "$DAEMON_BIN" ]]; then
    yellow "[WARN] 데몬 바이너리를 찾을 수 없습니다: $DAEMON_BIN"
    yellow "       MCP 서버는 데몬(TCP :$PORT)에 의존합니다."
fi

# 2) Node.js 확인
if ! command -v node >/dev/null 2>&1; then
    red "[ERROR] Node.js가 설치되어 있지 않습니다."
    red "        https://nodejs.org/ 에서 v18 이상 설치 후 재실행하세요."
    exit 1
fi
echo "[OK] Node.js $(node --version)"

# 3) claude CLI 확인
if ! command -v claude >/dev/null 2>&1; then
    red "[ERROR] claude CLI를 찾을 수 없습니다."
    red "        Claude Code가 설치되어 있어야 합니다: https://docs.claude.com/claude-code"
    exit 1
fi
echo "[OK] claude CLI: $(command -v claude)"

# 4) node_modules 확인
if [[ ! -d "$MCP_DIR/node_modules" ]]; then
    echo ""
    echo "[INFO] node_modules가 없습니다. npm install 실행 중..."
    (cd "$MCP_DIR" && npm install --omit=dev)
fi
echo "[OK] node_modules 확인 완료"

# 5) 기존 등록 제거
echo ""
echo "[INFO] 기존 '$NAME' 등록이 있으면 제거합니다..."
claude mcp remove "$NAME" --scope "$SCOPE" >/dev/null 2>&1 || true

# 6) MCP 등록
echo "[INFO] MCP 서버 등록: name=$NAME, scope=$SCOPE"
claude mcp add "$NAME" \
    --scope "$SCOPE" \
    -e "COMMMON_HOST=$DAEMON_HOST" \
    -e "COMMMON_PORT=$PORT" \
    -- node "$MCP_ENTRY"

echo ""
green "=== 설치 완료 ==="
echo ""
cyan "다음 단계:"
echo "  1) 데몬 실행 (별도 터미널에 상시 유지):"
yellow "       $DAEMON_BIN daemon"
echo ""
echo "  2) Claude Code 재시작 후 '/mcp' 로 '$NAME' 연결 상태 확인"
echo ""
echo "제거하려면:"
yellow "  claude mcp remove $NAME --scope $SCOPE"
