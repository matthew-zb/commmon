use std::sync::Arc;

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub async fn run(port: u16) -> anyhow::Result<()> {
    let stream = match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
        Ok(s) => s,
        Err(_) => {
            eprintln!("데몬에 접속할 수 없습니다. (127.0.0.1:{})", port);
            eprintln!("먼저 데몬을 실행하세요: commmon daemon");
            return Ok(());
        }
    };

    println!("데몬 접속 완료 (127.0.0.1:{})", port);

    let (reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let mut lines = BufReader::new(reader).lines();

    // notification 수신 태스크: 서버에서 오는 라인을 읽어서 notification/응답 분류
    let (resp_tx, mut resp_rx) = tokio::sync::mpsc::channel::<String>(32);

    let recv_task = tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim().to_string();
            if line.is_empty() {
                continue;
            }
            // notification인지 응답인지 구분
            if let Ok(val) = serde_json::from_str::<Value>(&line) {
                if val.get("notify").is_some() {
                    // notification — 즉시 출력
                    print_notification(&val);
                    continue;
                }
            }
            // 일반 응답 — 응답 채널로 전달
            if resp_tx.send(line).await.is_err() {
                break;
            }
        }
    });

    // REPL 루프 (rustyline은 blocking이므로 spawn_blocking)
    let writer_clone = Arc::clone(&writer);
    let repl_task = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let mut rl = DefaultEditor::new().expect("rustyline 초기화 실패");
        let history_path = dirs_history_path();
        let _ = rl.load_history(&history_path);

        loop {
            match rl.readline("commmon> ") {
                Ok(line) => {
                    let line = line.trim().to_string();
                    if line.is_empty() {
                        continue;
                    }
                    let _ = rl.add_history_entry(&line);

                    if line == "exit" || line == "quit" {
                        break;
                    }

                    if line == "help" {
                        print_help();
                        continue;
                    }

                    let json = match parse_command(&line) {
                        Some(j) => j,
                        None => {
                            eprintln!("알 수 없는 명령입니다. 'help'를 입력하세요.");
                            continue;
                        }
                    };

                    // 서버에 전송하고 응답 대기
                    let writer = Arc::clone(&writer_clone);
                    let resp = rt.block_on(async {
                        let mut w = writer.lock().await;
                        let mut msg = json;
                        msg.push('\n');
                        if w.write_all(msg.as_bytes()).await.is_err() {
                            return None;
                        }
                        // 응답 대기 (타임아웃 5초)
                        tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            resp_rx.recv(),
                        )
                        .await
                        .ok()
                        .flatten()
                    });

                    match resp {
                        Some(resp_line) => print_response(&resp_line),
                        None => {
                            eprintln!("서버 응답 없음 또는 연결 끊김");
                            break;
                        }
                    }
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
                Err(e) => {
                    eprintln!("입력 오류: {}", e);
                    break;
                }
            }
        }

        let _ = rl.save_history(&history_path);
    });

    // REPL 종료 대기
    let _ = repl_task.await;
    recv_task.abort();
    Ok(())
}

fn dirs_history_path() -> String {
    let home = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".to_string());
    let dir = std::path::Path::new(&home).join(".commmon");
    let _ = std::fs::create_dir_all(&dir);
    dir.join("history.txt").to_string_lossy().to_string()
}

/// REPL 명령을 TCP JSON 프로토콜로 변환
fn parse_command(input: &str) -> Option<String> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    let json = match parts[0] {
        "list" | "ls" => serde_json::json!({"cmd": "list_ports"}),

        "open" => {
            if parts.len() < 2 {
                eprintln!("사용법: open <포트> [baud] [8N1]");
                return None;
            }
            let port = parts[1];
            let baud: u64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(115200);
            let mut args = serde_json::json!({
                "port": port,
                "baudRate": baud,
            });
            // 8N1 형식 파싱
            if let Some(config) = parts.get(3) {
                let chars: Vec<char> = config.chars().collect();
                if chars.len() == 3 {
                    args["dataBits"] = Value::String(chars[0].to_string());
                    args["parity"] = Value::String(match chars[1] {
                        'N' | 'n' => "none",
                        'E' | 'e' => "even",
                        'O' | 'o' => "odd",
                        _ => "none",
                    }.to_string());
                    args["stopBits"] = Value::String(chars[2].to_string());
                }
            }
            serde_json::json!({"cmd": "open_port", "args": args})
        }

        "close" => {
            if parts.len() < 2 {
                eprintln!("사용법: close <포트>");
                return None;
            }
            serde_json::json!({"cmd": "close_port", "args": {"port": parts[1]}})
        }

        "write" | "send" => {
            if parts.len() < 3 {
                eprintln!("사용법: write <포트> <데이터> [--hex]");
                return None;
            }
            let port = parts[1];
            if parts.get(2) == Some(&"--hex") {
                let data = parts[3..].join("");
                serde_json::json!({"cmd": "write_port", "args": {"port": port, "data": data, "encoding": "hex"}})
            } else {
                let data = parts[2..].join(" ");
                serde_json::json!({"cmd": "write_port", "args": {"port": port, "data": data}})
            }
        }

        "read" => {
            if parts.len() < 2 {
                eprintln!("사용법: read <포트>");
                return None;
            }
            serde_json::json!({"cmd": "read_port", "args": {"port": parts[1]}})
        }

        "log" => {
            if parts.len() < 2 {
                eprintln!("사용법: log start|stop|update <포트> [옵션]");
                return None;
            }
            match parts[1] {
                "start" => {
                    if parts.len() < 3 {
                        eprintln!("사용법: log start <포트> [--duration N] [--keyword K] [--file F]");
                        return None;
                    }
                    let port = parts[2];
                    let mut args = serde_json::json!({"port": port});
                    parse_log_flags(&parts[3..], &mut args);
                    serde_json::json!({"cmd": "start_log", "args": args})
                }
                "stop" => {
                    if parts.len() < 3 {
                        eprintln!("사용법: log stop <포트>");
                        return None;
                    }
                    serde_json::json!({"cmd": "stop_log", "args": {"port": parts[2]}})
                }
                "update" => {
                    if parts.len() < 3 {
                        eprintln!("사용법: log update <포트> [--keyword K] [--duration N]");
                        return None;
                    }
                    let port = parts[2];
                    let mut args = serde_json::json!({"port": port});
                    parse_log_flags(&parts[3..], &mut args);
                    serde_json::json!({"cmd": "update_log", "args": args})
                }
                _ => {
                    eprintln!("사용법: log start|stop|update <포트>");
                    return None;
                }
            }
        }

        "status" => serde_json::json!({"cmd": "port_status"}),

        "monitor" => {
            if parts.len() < 2 {
                eprintln!("사용법: monitor start|stop [--port N]");
                return None;
            }
            match parts[1] {
                "start" => {
                    let mut args = serde_json::json!({});
                    if let Some(idx) = parts.iter().position(|&s| s == "--port") {
                        if let Some(p) = parts.get(idx + 1).and_then(|s| s.parse::<u16>().ok()) {
                            args["httpPort"] = Value::Number(p.into());
                        }
                    }
                    serde_json::json!({"cmd": "open_monitor", "args": args})
                }
                "stop" => serde_json::json!({"cmd": "close_monitor"}),
                _ => {
                    eprintln!("사용법: monitor start|stop");
                    return None;
                }
            }
        }

        _ => return None,
    };

    Some(serde_json::to_string(&json).unwrap())
}

fn parse_log_flags(flags: &[&str], args: &mut Value) {
    let mut i = 0;
    while i < flags.len() {
        match flags[i] {
            "--duration" => {
                if let Some(val) = flags.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                    args["duration"] = Value::Number(val.into());
                    i += 1;
                }
            }
            "--keyword" => {
                if let Some(val) = flags.get(i + 1) {
                    args["stopKeyword"] = Value::String(val.to_string());
                    i += 1;
                }
            }
            "--file" => {
                if let Some(val) = flags.get(i + 1) {
                    args["filePath"] = Value::String(val.to_string());
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
}

fn print_response(line: &str) {
    let val: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(_) => {
            println!("{}", line);
            return;
        }
    };

    if val.get("ok") == Some(&Value::Bool(true)) {
        if let Some(data) = val.get("data") {
            format_data(data);
        }
    } else if let Some(err) = val.get("error").and_then(|v| v.as_str()) {
        eprintln!("오류: {}", err);
    }
}

fn format_data(data: &Value) {
    match data {
        Value::String(s) => println!("{}", s),
        Value::Array(arr) => {
            if arr.is_empty() {
                println!("(비어 있음)");
                return;
            }
            for item in arr {
                if let Some(obj) = item.as_object() {
                    // 포트 목록
                    if let Some(path) = obj.get("path").and_then(|v| v.as_str()) {
                        let mut desc = path.to_string();
                        if let Some(m) = obj.get("manufacturer").and_then(|v| v.as_str()) {
                            desc.push_str(&format!(" ({})", m));
                        }
                        if let Some(p) = obj.get("product").and_then(|v| v.as_str()) {
                            desc.push_str(&format!(" - {}", p));
                        }
                        println!("  {}", desc);
                        continue;
                    }
                    // 수신 데이터
                    if let (Some(ts), Some(d)) = (
                        obj.get("timestamp").and_then(|v| v.as_str()),
                        obj.get("data").and_then(|v| v.as_str()),
                    ) {
                        println!("[{}] {}", ts, d);
                        continue;
                    }
                    // 포트 상태
                    if let Some(port) = obj.get("port").and_then(|v| v.as_str()) {
                        let baud = obj
                            .get("baudRate")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        let config = obj
                            .get("config")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let mut line = format!("  {} {}bps {}", port, baud, config);
                        if let Some(log) = obj.get("log") {
                            if !log.is_null() {
                                if let Some(file) = log.get("file").and_then(|v| v.as_str()) {
                                    line.push_str(&format!(" [로그: {}]", file));
                                }
                            }
                        }
                        println!("{}", line);
                        continue;
                    }
                }
                // fallback
                println!("  {}", serde_json::to_string_pretty(item).unwrap_or_default());
            }
        }
        Value::Object(obj) => {
            // port_status 응답: { "ports": [...], "monitor": {...} }
            if let Some(ports) = obj.get("ports") {
                if let Value::Array(arr) = ports {
                    if arr.is_empty() {
                        println!("열린 포트 없음");
                    } else {
                        format_data(ports);
                    }
                }
                if let Some(mon) = obj.get("monitor") {
                    if mon.get("active") == Some(&Value::Bool(true)) {
                        let url = mon.get("url").and_then(|v| v.as_str()).unwrap_or("");
                        println!("  모니터: {}", url);
                    }
                }
                return;
            }
            // stop_log 응답
            if let Some(file) = obj.get("file").and_then(|v| v.as_str()) {
                let port = obj.get("port").and_then(|v| v.as_str()).unwrap_or("");
                println!("{} 로그 중지: {}", port, file);
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(data).unwrap_or_default()
                );
            }
        }
        _ => println!("{}", data),
    }
}

fn print_notification(val: &Value) {
    let notify = val.get("notify").and_then(|v| v.as_str()).unwrap_or("");
    let data = val.get("data").unwrap_or(&Value::Null);

    match notify {
        "log_stopped" => {
            let port = data.get("port").and_then(|v| v.as_str()).unwrap_or("");
            let reason = data.get("reason").and_then(|v| v.as_str()).unwrap_or("");
            let file = data.get("file").and_then(|v| v.as_str()).unwrap_or("");
            match reason {
                "keyword" => {
                    let kw = data.get("keyword").and_then(|v| v.as_str()).unwrap_or("");
                    eprintln!("\n[알림] {} 로그 중지 (키워드 '{}' 감지): {}", port, kw, file);
                }
                "duration" => {
                    eprintln!("\n[알림] {} 로그 중지 (시간 만료): {}", port, file);
                }
                _ => {
                    eprintln!("\n[알림] {} 로그 중지: {}", port, file);
                }
            }
        }
        "port_error" => {
            let port = data.get("port").and_then(|v| v.as_str()).unwrap_or("");
            let err = data.get("error").and_then(|v| v.as_str()).unwrap_or("");
            eprintln!("\n[알림] {} 오류: {}", port, err);
        }
        _ => {
            eprintln!("\n[알림] {}", serde_json::to_string(val).unwrap_or_default());
        }
    }
}

fn print_help() {
    println!(
        r#"명령어:
  list                           포트 목록
  open <포트> [baud] [8N1]       포트 열기
  close <포트>                   포트 닫기
  write <포트> <데이터>          ASCII 전송
  write <포트> --hex <HEX>       HEX 전송
  read <포트>                    수신 버퍼 읽기
  log start <포트> [옵션]        로그 시작
    --file <경로>                  파일 경로
    --duration <초>                자동 중지 시간
    --keyword <키워드>             감지 시 중지
  log update <포트> [옵션]       로그 설정 변경
  log stop <포트>                로그 중지
  status                         전체 상태
  monitor start [--port N]       웹 모니터 시작
  monitor stop                   웹 모니터 종료
  help                           이 도움말
  exit                           종료"#
    );
}
