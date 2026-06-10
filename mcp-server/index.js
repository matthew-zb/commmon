import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import net from "node:net";
import { spawn, exec } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";

const DAEMON_HOST = process.env.COMMMON_HOST || "127.0.0.1";
const DAEMON_PORT = parseInt(process.env.COMMMON_PORT || "9900", 10);

const execAsync = promisify(exec);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEFAULT_DAEMON_BIN = path.resolve(__dirname, "..", "commmon", "target", "release", "commmon.exe");
const DAEMON_BIN = process.env.COMMMON_DAEMON_BIN || DEFAULT_DAEMON_BIN;

let daemonChild = null;

async function isDaemonAlive(port = DAEMON_PORT, host = DAEMON_HOST) {
  return new Promise((resolve) => {
    const sock = new net.Socket();
    let done = false;
    const finish = (v) => { if (done) return; done = true; sock.destroy(); resolve(v); };
    sock.setTimeout(500);
    sock.once("connect", () => finish(true));
    sock.once("timeout", () => finish(false));
    sock.once("error", () => finish(false));
    sock.connect(port, host);
  });
}

async function findPidByPort(port) {
  try {
    const { stdout } = await execAsync(`netstat -ano -p TCP`);
    const re = new RegExp(`^\\s*TCP\\s+\\S+:${port}\\s+\\S+\\s+LISTENING\\s+(\\d+)`, "m");
    const m = stdout.match(re);
    return m ? parseInt(m[1], 10) : null;
  } catch {
    return null;
  }
}

const MAX_RX_STREAM_ENTRIES = 500;

/** TCP 클라이언트로 데몬에 JSON 명령 전송 후 응답 수신 */
class DaemonClient {
  constructor() {
    this.socket = null;
    this.pending = [];
    this.buffer = "";
    /** @type {Map<string, Array<{port:string, timestamp:string, ascii:string, hex:string}>>} */
    this.rxStreamBuffers = new Map();
    /** @type {Map<string, Array<{port:string, keyword:string, timestamp:string, context:string}>>} */
    this.filterHits = new Map();
  }

  async connect() {
    if (this.socket && !this.socket.destroyed) return;

    this.socket = new net.Socket();
    this.buffer = "";

    await new Promise((resolve, reject) => {
      this.socket.connect(DAEMON_PORT, DAEMON_HOST, resolve);
      this.socket.once("error", reject);
    });

    this.socket.on("data", (chunk) => {
      this.buffer += chunk.toString();
      let idx;
      while ((idx = this.buffer.indexOf("\n")) !== -1) {
        const line = this.buffer.slice(0, idx).trim();
        this.buffer = this.buffer.slice(idx + 1);
        if (!line) continue;

        try {
          const msg = JSON.parse(line);
          // notification 처리
          if (msg.notify) {
            if (msg.notify === "rx_data" && msg.data) {
              const port = msg.data.port;
              if (port && this.rxStreamBuffers.has(port)) {
                const buf = this.rxStreamBuffers.get(port);
                buf.push(msg.data);
                if (buf.length > MAX_RX_STREAM_ENTRIES) {
                  buf.splice(0, buf.length - MAX_RX_STREAM_ENTRIES);
                }
              }
            } else if (msg.notify === "filter_hit" && msg.data) {
              const port = msg.data.port;
              if (port && this.filterHits.has(port)) {
                const buf = this.filterHits.get(port);
                buf.push(msg.data);
                if (buf.length > MAX_RX_STREAM_ENTRIES) {
                  buf.splice(0, buf.length - MAX_RX_STREAM_ENTRIES);
                }
              }
            }
            continue;
          }
          // 일반 응답
          if (this.pending.length > 0) {
            this.pending.shift()(msg);
          }
        } catch {}
      }
    });

    this.socket.on("error", () => {});
    this.socket.on("close", () => {
      // 대기 중인 요청 모두 에러 처리
      for (const resolve of this.pending) {
        resolve({ ok: false, error: "데몬 연결이 끊어졌습니다." });
      }
      this.pending = [];
      this.socket = null;
    });
  }

  async send(cmd, args = {}) {
    await this.connect();
    const msg = JSON.stringify({ cmd, args }) + "\n";
    return new Promise((resolve) => {
      this.pending.push(resolve);
      this.socket.write(msg);
    });
  }
}

const daemon = new DaemonClient();

function toMcpResult(resp) {
  if (resp.ok) {
    const text = typeof resp.data === "string" ? resp.data : JSON.stringify(resp.data, null, 2);
    return { content: [{ type: "text", text }] };
  }
  return { content: [{ type: "text", text: resp.error || "알 수 없는 오류" }], isError: true };
}

// 사용자 설명에서 추출한 키워드를 파일명에 안전하게 끼워넣기 위한 정규화.
// 파일시스템 금지문자/공백을 _로 치환하고 길이를 제한한다. (한글은 유지)
function sanitizeLabel(label) {
  if (!label) return "";
  const cleaned = label
    .trim()
    .replace(/[\\/:*?"<>|]/g, "_") // 금지 문자
    .replace(/\s+/g, "_")           // 공백류 → _
    .replace(/_+/g, "_")            // 연속 _ 축약
    .replace(/^_+|_+$/g, "");       // 양끝 _ 제거
  return cleaned.slice(0, 40);
}

const server = new McpServer({
  name: "com-port-server",
  version: "2.0.0",
});

server.tool("list_ports", "사용 가능한 COM 포트 목록을 조회합니다", {}, async () => {
  return toMcpResult(await daemon.send("list_ports"));
});

server.tool(
  "open_port",
  "COM 포트를 열어 시리얼 통신을 시작합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3, COM4)"),
    baudRate: z.number().default(115200).describe("통신 속도 (기본값: 115200)"),
    dataBits: z.enum(["5", "6", "7", "8"]).default("8").describe("데이터 비트"),
    stopBits: z.enum(["1", "1.5", "2"]).default("1").describe("스톱 비트"),
    parity: z.enum(["none", "even", "odd", "mark", "space"]).default("none").describe("패리티"),
  },
  async ({ port, baudRate, dataBits, stopBits, parity }) => {
    return toMcpResult(await daemon.send("open_port", { port, baudRate, dataBits, stopBits, parity }));
  }
);

server.tool(
  "write_port",
  "열려 있는 COM 포트에 데이터를 전송합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    data: z.string().describe("전송할 데이터 (문자열)"),
    encoding: z.enum(["ascii", "hex", "utf8"]).default("ascii").describe("인코딩 (ascii, hex, utf8)"),
  },
  async ({ port, data, encoding }) => {
    return toMcpResult(await daemon.send("write_port", { port, data, encoding }));
  }
);

server.tool(
  "read_port",
  "COM 포트에서 수신된 데이터를 읽습니다 (버퍼에 쌓인 데이터)",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    clear: z.boolean().default(true).describe("읽은 후 버퍼를 비울지 여부"),
  },
  async ({ port, clear }) => {
    return toMcpResult(await daemon.send("read_port", { port, clear }));
  }
);

server.tool(
  "start_log",
  "열려 있는 COM 포트의 수신 데이터를 파일에 로그로 기록합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    duration: z.number().optional().describe("로그 기록 시간 (초). 생략 시 수동 중지까지 계속"),
    stopKeyword: z.string().optional().describe("이 문자열이 수신 데이터에 포함되면 로그 자동 중지"),
    filePath: z.string().optional().describe("로그 파일 경로. 생략 시 현재 작업 디렉토리의 log/ 아래에 자동 생성"),
    label: z.string().optional().describe("로그의 용도/맥락을 나타내는 키워드(예: 도어락페어링, 부팅테스트). 사용자가 로그 요청과 함께 설명한 내용에서 핵심 키워드를 1~2개 뽑아 전달하면 파일명에 포함됩니다"),
  },
  async ({ port, duration, stopKeyword, filePath, label }) => {
    const args = { port };
    if (duration !== undefined) args.duration = duration;
    if (stopKeyword !== undefined) args.stopKeyword = stopKeyword;

    const safeLabel = sanitizeLabel(label);

    let resolvedPath;
    if (filePath !== undefined) {
      resolvedPath = path.isAbsolute(filePath) ? filePath : path.resolve(process.cwd(), filePath);
      // 명시적 경로에도 키워드를 확장자 앞에 삽입
      if (safeLabel) {
        const ext = path.extname(resolvedPath);
        const base = resolvedPath.slice(0, resolvedPath.length - ext.length);
        resolvedPath = `${base}_${safeLabel}${ext}`;
      }
    } else {
      const ts = new Date().toISOString().replace(/[-:T]/g, "").replace(/\..+$/, "");
      const safePort = port.replace(/[\\/:*?"<>|]/g, "_");
      const labelPart = safeLabel ? `${safeLabel}_` : "";
      resolvedPath = path.join(process.cwd(), "log", `commmon_${safePort}_${labelPart}${ts}.log`);
    }
    try {
      await fs.mkdir(path.dirname(resolvedPath), { recursive: true });
    } catch (e) {
      return toMcpResult({ ok: false, error: `로그 디렉토리 생성 실패: ${e.message}` });
    }
    args.filePath = resolvedPath;

    return toMcpResult(await daemon.send("start_log", args));
  }
);

server.tool(
  "update_log",
  "로그 기록 중 중지 키워드나 남은 시간을 변경합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    stopKeyword: z.string().optional().describe("새 중지 키워드. 빈 문자열(\"\")로 설정하면 키워드 중지 해제"),
    duration: z.number().optional().describe("지금부터 N초 후 자동 중지. 기존 타이머를 대체"),
  },
  async ({ port, stopKeyword, duration }) => {
    const args = { port };
    if (stopKeyword !== undefined) args.stopKeyword = stopKeyword;
    if (duration !== undefined) args.duration = duration;
    return toMcpResult(await daemon.send("update_log", args));
  }
);

server.tool(
  "stop_log",
  "COM 포트의 로그 기록을 중지합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
  },
  async ({ port }) => {
    return toMcpResult(await daemon.send("stop_log", { port }));
  }
);

server.tool(
  "close_port",
  "열려 있는 COM 포트를 닫습니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
  },
  async ({ port }) => {
    return toMcpResult(await daemon.send("close_port", { port }));
  }
);

server.tool("port_status", "현재 열려 있는 COM 포트의 상태를 확인합니다", {}, async () => {
  return toMcpResult(await daemon.send("port_status"));
});

server.tool(
  "open_monitor",
  "시리얼 모니터 웹 UI를 브라우저에서 열 수 있도록 HTTP 서버를 시작합니다",
  {
    httpPort: z.number().default(8765).describe("HTTP 서버 포트 (기본값: 8765)"),
  },
  async ({ httpPort }) => {
    return toMcpResult(await daemon.send("open_monitor", { httpPort }));
  }
);

server.tool("close_monitor", "시리얼 모니터 웹 UI HTTP 서버를 종료합니다", {}, async () => {
  return toMcpResult(await daemon.send("close_monitor"));
});

server.tool(
  "start_daemon",
  "commmon 백엔드 데몬을 백그라운드로 시작합니다. 이미 실행 중이면 그대로 둡니다.",
  {
    port: z.number().default(DAEMON_PORT).describe(`데몬 TCP 포트 (기본값: ${DAEMON_PORT})`),
    bin: z.string().optional().describe("commmon.exe 경로 (생략 시 mcp-server 옆 ../commmon/target/release/commmon.exe)"),
  },
  async ({ port, bin }) => {
    if (await isDaemonAlive(port)) {
      const pid = await findPidByPort(port);
      return { content: [{ type: "text", text: `데몬이 이미 실행 중 (127.0.0.1:${port}${pid ? `, PID ${pid}` : ""})` }] };
    }
    const binPath = bin || DAEMON_BIN;
    try {
      const child = spawn(binPath, ["daemon", "--port", String(port)], {
        detached: true,
        stdio: "ignore",
        windowsHide: true,
      });
      child.on("error", () => {});
      child.unref();
      daemonChild = child;

      for (let i = 0; i < 25; i++) {
        await new Promise((r) => setTimeout(r, 200));
        if (await isDaemonAlive(port)) {
          return { content: [{ type: "text", text: `데몬 시작 (PID ${child.pid}, 127.0.0.1:${port}, bin: ${binPath})` }] };
        }
      }
      return { content: [{ type: "text", text: `데몬 spawn은 했으나 ${port} 포트가 5초 내에 열리지 않음 (PID ${child.pid}, bin: ${binPath})` }], isError: true };
    } catch (e) {
      return { content: [{ type: "text", text: `데몬 시작 실패: ${e.message} (bin: ${binPath})` }], isError: true };
    }
  }
);

server.tool(
  "stop_daemon",
  "실행 중인 commmon 백엔드 데몬을 종료합니다",
  {
    port: z.number().default(DAEMON_PORT).describe(`데몬 TCP 포트 (기본값: ${DAEMON_PORT})`),
  },
  async ({ port }) => {
    const alive = await isDaemonAlive(port);
    if (!alive && !daemonChild) {
      return { content: [{ type: "text", text: `데몬이 실행 중이 아닙니다 (127.0.0.1:${port})` }] };
    }

    // 우리 MCP 클라이언트 소켓 정리 (데몬 종료 시 close 이벤트와 함께 자동 정리되지만 명시적으로)
    if (daemon.socket && !daemon.socket.destroyed) {
      daemon.socket.destroy();
    }

    let killedBy = null;
    if (daemonChild && !daemonChild.killed) {
      try {
        await execAsync(`taskkill /F /T /PID ${daemonChild.pid}`);
        killedBy = `child PID ${daemonChild.pid}`;
      } catch (e) {
        try { daemonChild.kill(); killedBy = `child.kill PID ${daemonChild.pid}`; } catch {}
      }
      daemonChild = null;
    }

    if (!killedBy) {
      const pid = await findPidByPort(port);
      if (pid) {
        try {
          await execAsync(`taskkill /F /T /PID ${pid}`);
          killedBy = `PID ${pid} (netstat)`;
        } catch (e) {
          return { content: [{ type: "text", text: `taskkill 실패: ${e.message}` }], isError: true };
        }
      }
    }

    for (let i = 0; i < 15; i++) {
      await new Promise((r) => setTimeout(r, 200));
      if (!(await isDaemonAlive(port))) {
        return { content: [{ type: "text", text: `데몬 종료 (${killedBy || "이미 종료된 상태"})` }] };
      }
    }
    return { content: [{ type: "text", text: `종료 시도(${killedBy || "대상 미식별"})했으나 ${port} 포트가 여전히 응답 중` }], isError: true };
  }
);

server.tool(
  "daemon_status",
  "commmon 백엔드 데몬의 실행 상태를 확인합니다",
  {
    port: z.number().default(DAEMON_PORT).describe(`데몬 TCP 포트 (기본값: ${DAEMON_PORT})`),
  },
  async ({ port }) => {
    const alive = await isDaemonAlive(port);
    const pid = alive ? await findPidByPort(port) : null;
    const text = JSON.stringify({
      alive,
      host: DAEMON_HOST,
      port,
      pid,
      childPid: daemonChild?.pid ?? null,
      bin: DAEMON_BIN,
    }, null, 2);
    return { content: [{ type: "text", text }] };
  }
);

server.tool(
  "subscribe_rx",
  "COM 포트의 실시간 RX 데이터 구독을 시작합니다. 구독 후 read_rx_stream으로 데이터를 읽을 수 있습니다.",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
  },
  async ({ port }) => {
    const resp = await daemon.send("subscribe_rx", { port });
    if (resp.ok) {
      daemon.rxStreamBuffers.set(port, []);
    }
    return toMcpResult(resp);
  }
);

server.tool(
  "unsubscribe_rx",
  "COM 포트의 실시간 RX 데이터 구독을 해제합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
  },
  async ({ port }) => {
    const resp = await daemon.send("unsubscribe_rx", { port });
    if (resp.ok) {
      daemon.rxStreamBuffers.delete(port);
    }
    return toMcpResult(resp);
  }
);

server.tool(
  "read_rx_stream",
  "구독 중인 COM 포트의 실시간 수신 데이터를 읽습니다 (subscribe_rx 후 사용)",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    clear: z.boolean().default(true).describe("읽은 후 버퍼를 비울지 여부"),
  },
  async ({ port, clear }) => {
    if (!daemon.rxStreamBuffers.has(port)) {
      return { content: [{ type: "text", text: `${port}는 구독 중이 아닙니다. 먼저 subscribe_rx를 호출하세요.` }], isError: true };
    }
    const buf = daemon.rxStreamBuffers.get(port);
    const data = [...buf];
    if (clear) {
      buf.length = 0;
    }
    const text = JSON.stringify(data, null, 2);
    return { content: [{ type: "text", text }] };
  }
);

server.tool(
  "filter_rx",
  "열려 있는 COM 포트의 수신 데이터에서 키워드를 모니터링합니다. 등록한 키워드가 RX 데이터에 나타나면 데몬이 hit을 기록하며, read_filter_hits로 조회합니다. (포트 열림 필요)",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    keywords: z
      .array(z.string().min(1))
      .min(1)
      .describe("모니터링할 키워드 목록 (예: [\"ERROR\", \"PANIC\", \"부팅완료\"]). 재등록 시 키워드 갱신"),
  },
  async ({ port, keywords }) => {
    const resp = await daemon.send("filter_rx", { port, keywords });
    if (resp.ok) {
      daemon.filterHits.set(port, []);
    }
    return toMcpResult(resp);
  }
);

server.tool(
  "add_filter_rx",
  "기존 키워드 필터에 키워드를 추가합니다 (중복 제외). filter_rx와 달리 기존 키워드와 누적된 hit 기록을 유지합니다. 등록된 필터가 없으면 새로 등록합니다.",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    keywords: z
      .array(z.string().min(1))
      .min(1)
      .describe("추가할 키워드 목록 (예: [\"WARN\", \"타임아웃\"]). 기존 키워드에 합쳐짐"),
  },
  async ({ port, keywords }) => {
    const resp = await daemon.send("add_filter_rx", { port, keywords });
    if (resp.ok && !daemon.filterHits.has(port)) {
      // 신규 등록인 경우에만 버퍼 생성, 기존 hit은 보존
      daemon.filterHits.set(port, []);
    }
    return toMcpResult(resp);
  }
);

server.tool(
  "unfilter_rx",
  "COM 포트의 키워드 모니터링을 해제합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
  },
  async ({ port }) => {
    const resp = await daemon.send("unfilter_rx", { port });
    if (resp.ok) {
      daemon.filterHits.delete(port);
    }
    return toMcpResult(resp);
  }
);

server.tool(
  "read_filter_hits",
  "모니터링 중인 키워드가 감지된 hit 목록을 읽습니다 (filter_rx 후 사용). 각 hit은 키워드, 타임스탬프, 주변 컨텍스트를 포함합니다",
  {
    port: z.string().describe("COM 포트 경로 (예: COM3)"),
    clear: z.boolean().default(true).describe("읽은 후 버퍼를 비울지 여부"),
  },
  async ({ port, clear }) => {
    if (!daemon.filterHits.has(port)) {
      return { content: [{ type: "text", text: `${port}에 등록된 필터가 없습니다. 먼저 filter_rx를 호출하세요.` }], isError: true };
    }
    const buf = daemon.filterHits.get(port);
    const data = [...buf];
    if (clear) {
      buf.length = 0;
    }
    const text = JSON.stringify(data, null, 2);
    return { content: [{ type: "text", text }] };
  }
);

process.on("SIGINT", () => {
  if (daemon.socket) {
    daemon.socket.destroy();
  }
  process.exit(0);
});

const transport = new StdioServerTransport();
await server.connect(transport);
