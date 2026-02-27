import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import net from "node:net";

const DAEMON_HOST = process.env.COMMMON_HOST || "127.0.0.1";
const DAEMON_PORT = parseInt(process.env.COMMMON_PORT || "9900", 10);

/** TCP 클라이언트로 데몬에 JSON 명령 전송 후 응답 수신 */
class DaemonClient {
  constructor() {
    this.socket = null;
    this.pending = [];
    this.buffer = "";
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
          // notification — 무시 (MCP에서는 불필요)
          if (msg.notify) continue;
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
    filePath: z.string().optional().describe("로그 파일 경로. 생략 시 임시 디렉토리에 자동 생성"),
  },
  async ({ port, duration, stopKeyword, filePath }) => {
    const args = { port };
    if (duration !== undefined) args.duration = duration;
    if (stopKeyword !== undefined) args.stopKeyword = stopKeyword;
    if (filePath !== undefined) args.filePath = filePath;
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

process.on("SIGINT", () => {
  if (daemon.socket) {
    daemon.socket.destroy();
  }
  process.exit(0);
});

const transport = new StdioServerTransport();
await server.connect(transport);
