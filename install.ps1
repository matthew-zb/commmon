#Requires -Version 5.1
<#
.SYNOPSIS
  commmon MCP 서버를 Claude Code에 등록하는 Windows 설치 스크립트.

.DESCRIPTION
  1) Node.js 설치 확인
  2) claude CLI 존재 확인
  3) claude mcp add 로 MCP 서버 등록
  4) 데몬 실행 방법 안내

.PARAMETER Scope
  MCP 등록 범위: user (기본), project, local

.PARAMETER Name
  등록할 MCP 서버 이름 (기본: com-port)

.PARAMETER Host
  데몬 호스트 (기본: 127.0.0.1)

.PARAMETER Port
  데몬 포트 (기본: 9900)

.EXAMPLE
  .\install.ps1
  .\install.ps1 -Scope project -Name commmon
#>

[CmdletBinding()]
param(
    [ValidateSet("user", "project", "local")]
    [string]$Scope = "user",
    [string]$Name = "com-port",
    [string]$DaemonHost = "127.0.0.1",
    [string]$Port = "9900"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$McpDir = Join-Path $ScriptDir "mcp-server"
$McpEntry = Join-Path $McpDir "index.js"
$DaemonExe = Join-Path $ScriptDir "commmon.exe"

Write-Host "=== commmon MCP 서버 설치 ===" -ForegroundColor Cyan

# 1) 파일 존재 확인
if (-not (Test-Path $McpEntry)) {
    Write-Host "[ERROR] MCP 서버 파일을 찾을 수 없습니다: $McpEntry" -ForegroundColor Red
    exit 1
}
if (-not (Test-Path $DaemonExe)) {
    Write-Host "[WARN] 데몬 바이너리를 찾을 수 없습니다: $DaemonExe" -ForegroundColor Yellow
    Write-Host "       MCP 서버는 데몬(TCP :$Port)에 의존합니다." -ForegroundColor Yellow
}

# 2) Node.js 확인
try {
    $nodeVersion = node --version 2>$null
    if (-not $nodeVersion) { throw "not found" }
    Write-Host "[OK] Node.js $nodeVersion"
} catch {
    Write-Host "[ERROR] Node.js가 설치되어 있지 않습니다." -ForegroundColor Red
    Write-Host "        https://nodejs.org/ 에서 v18 이상 설치 후 재실행하세요." -ForegroundColor Red
    exit 1
}

# 3) claude CLI 확인
$claudeCmd = Get-Command claude -ErrorAction SilentlyContinue
if (-not $claudeCmd) {
    Write-Host "[ERROR] claude CLI를 찾을 수 없습니다." -ForegroundColor Red
    Write-Host "        Claude Code가 설치되어 있어야 합니다: https://docs.claude.com/claude-code" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] claude CLI: $($claudeCmd.Source)"

# 4) node_modules 확인 (없으면 설치)
$NodeModules = Join-Path $McpDir "node_modules"
if (-not (Test-Path $NodeModules)) {
    Write-Host "`n[INFO] node_modules가 없습니다. npm install 실행 중..."
    Push-Location $McpDir
    try {
        npm install --omit=dev
        if ($LASTEXITCODE -ne 0) { throw "npm install 실패" }
    } finally {
        Pop-Location
    }
}
Write-Host "[OK] node_modules 확인 완료"

# 5) 기존 등록 제거 (있을 경우)
Write-Host "`n[INFO] 기존 '$Name' 등록이 있으면 제거합니다..."
& claude mcp remove $Name --scope $Scope 2>$null | Out-Null

# 6) MCP 등록
Write-Host "[INFO] MCP 서버 등록: name=$Name, scope=$Scope"
& claude mcp add $Name `
    --scope $Scope `
    -e "COMMMON_HOST=$DaemonHost" `
    -e "COMMMON_PORT=$Port" `
    -- node $McpEntry

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] MCP 등록 실패" -ForegroundColor Red
    exit 1
}

Write-Host "`n=== 설치 완료 ===" -ForegroundColor Green
Write-Host ""
Write-Host "다음 단계:" -ForegroundColor Cyan
Write-Host "  1) 데몬 실행 (별도 터미널에 상시 유지):"
Write-Host "       $DaemonExe daemon" -ForegroundColor Yellow
Write-Host ""
Write-Host "  2) Claude Code 재시작 후 '/mcp' 로 '$Name' 연결 상태 확인"
Write-Host ""
Write-Host "제거하려면:"
Write-Host "  claude mcp remove $Name --scope $Scope" -ForegroundColor Yellow
