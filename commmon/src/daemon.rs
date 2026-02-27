use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, Mutex};
use tracing::{error, info, warn};

use crate::monitor::{self, MonitorHandle};
use crate::protocol::{Notification, Request, Response};
use crate::serial::{RxEntry, SerialManager};

const MAX_BUFFER_ENTRIES: usize = 200;

/// RX 스트리밍 태스크에 보내는 명령
enum RxSubCommand {
    Subscribe(String, broadcast::Receiver<RxEntry>),
    Unsubscribe(String),
}

/// 데몬 전체 상태
struct DaemonState {
    serial: Arc<SerialManager>,
    monitor: Mutex<Option<MonitorHandle>>,
}

/// 클라이언트별 수신 버퍼
struct ClientSession {
    receivers: HashMap<String, broadcast::Receiver<RxEntry>>,
    buffers: HashMap<String, VecDeque<RxEntry>>,
    /// 실시간 RX push 대상 포트
    rx_subscriptions: HashSet<String>,
}

impl ClientSession {
    fn new() -> Self {
        Self {
            receivers: HashMap::new(),
            buffers: HashMap::new(),
            rx_subscriptions: HashSet::new(),
        }
    }

    fn drain_receiver(&mut self, port_name: &str) {
        if let Some(rx) = self.receivers.get_mut(port_name) {
            let buffer = self.buffers.entry(port_name.to_string()).or_default();
            while let Ok(entry) = rx.try_recv() {
                buffer.push_back(entry);
                if buffer.len() > MAX_BUFFER_ENTRIES {
                    buffer.pop_front();
                }
            }
        }
    }

    async fn ensure_subscribed(&mut self, port_name: &str, serial: &SerialManager) {
        if !self.receivers.contains_key(port_name) {
            if let Some(rx) = serial.subscribe(port_name).await {
                self.receivers.insert(port_name.to_string(), rx);
                self.buffers.insert(port_name.to_string(), VecDeque::new());
            }
        }
    }

    fn read_port(&mut self, port_name: &str, clear: bool) -> Response {
        self.drain_receiver(port_name);

        let buffer = match self.buffers.get_mut(port_name) {
            Some(b) => b,
            None => return Response::error(&format!("{}의 수신 버퍼가 없습니다.", port_name)),
        };

        let entries: Vec<Value> = buffer
            .iter()
            .map(|e| {
                let text = String::from_utf8_lossy(&e.data);
                serde_json::json!({
                    "timestamp": e.timestamp,
                    "data": text,
                })
            })
            .collect();

        if clear {
            buffer.clear();
        }

        Response::success(Value::Array(entries))
    }

    fn on_port_closed(&mut self, port_name: &str) {
        self.receivers.remove(port_name);
        self.buffers.remove(port_name);
        self.rx_subscriptions.remove(port_name);
    }
}

pub async fn run(port: u16) -> anyhow::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    info!("데몬 시작: 127.0.0.1:{}", port);

    let state = Arc::new(DaemonState {
        serial: Arc::new(SerialManager::new()),
        monitor: Mutex::new(None),
    });

    // SIGINT 클린업
    let state_cleanup = Arc::clone(&state);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("SIGINT 수신, 정리 중...");

        // 모니터 종료
        if let Some(handle) = state_cleanup.monitor.lock().await.take() {
            handle.stop().await;
        }

        // 모든 포트/로그 종료
        state_cleanup.serial.close_all().await;

        info!("정리 완료, 종료합니다.");
        std::process::exit(0);
    });

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("클라이언트 접속: {}", addr);

        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, state).await {
                error!("클라이언트 {} 처리 오류: {}", addr, e);
            }
            info!("클라이언트 연결 종료: {}", addr);
        });
    }
}

async fn handle_client(
    stream: tokio::net::TcpStream,
    state: Arc<DaemonState>,
) -> anyhow::Result<()> {
    let (reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let mut lines = BufReader::new(reader).lines();
    let mut session = ClientSession::new();

    // notification 전달 태스크
    let mut notify_rx = state.serial.subscribe_notifications();
    let writer_notify = Arc::clone(&writer);
    let notify_task = tokio::spawn(async move {
        while let Ok(notif) = notify_rx.recv().await {
            let mut json = match serde_json::to_string(&notif) {
                Ok(j) => j,
                Err(_) => continue,
            };
            json.push('\n');
            let mut w = writer_notify.lock().await;
            if w.write_all(json.as_bytes()).await.is_err() {
                break;
            }
        }
    });

    // RX 스트리밍 태스크: subscribe_rx로 등록된 포트의 수신 데이터를 실시간 push
    let (rx_cmd_tx, mut rx_cmd_rx) = mpsc::channel::<RxSubCommand>(16);
    let writer_rx = Arc::clone(&writer);
    let rx_stream_task = tokio::spawn(async move {
        let mut rx_receivers: HashMap<String, broadcast::Receiver<RxEntry>> = HashMap::new();

        loop {
            // 새 명령 수신
            while let Ok(cmd) = rx_cmd_rx.try_recv() {
                match cmd {
                    RxSubCommand::Subscribe(port, receiver) => {
                        rx_receivers.insert(port, receiver);
                    }
                    RxSubCommand::Unsubscribe(port) => {
                        rx_receivers.remove(&port);
                    }
                }
            }

            if rx_receivers.is_empty() {
                // 구독 없으면 새 명령이 올 때까지 대기
                match rx_cmd_rx.recv().await {
                    Some(RxSubCommand::Subscribe(port, receiver)) => {
                        rx_receivers.insert(port, receiver);
                    }
                    Some(RxSubCommand::Unsubscribe(port)) => {
                        rx_receivers.remove(&port);
                        continue;
                    }
                    None => break,
                }
            }

            let mut got_data = false;
            let mut closed_ports = Vec::new();

            for (port_name, rx) in rx_receivers.iter_mut() {
                loop {
                    match rx.try_recv() {
                        Ok(entry) => {
                            got_data = true;
                            let ascii = String::from_utf8_lossy(&entry.data).to_string();
                            let hex_str = hex::encode(&entry.data);
                            let notif = Notification {
                                notify: "rx_data".into(),
                                data: serde_json::json!({
                                    "port": port_name,
                                    "timestamp": entry.timestamp,
                                    "ascii": ascii,
                                    "hex": hex_str,
                                }),
                            };
                            let mut json = match serde_json::to_string(&notif) {
                                Ok(j) => j,
                                Err(_) => continue,
                            };
                            json.push('\n');
                            let mut w = writer_rx.lock().await;
                            if w.write_all(json.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                        Err(broadcast::error::TryRecvError::Lagged(n)) => {
                            warn!("{} rx_data {}건 누락", port_name, n);
                            continue; // lagged 후 재시도
                        }
                        Err(broadcast::error::TryRecvError::Empty) => {
                            break;
                        }
                        Err(broadcast::error::TryRecvError::Closed) => {
                            closed_ports.push(port_name.clone());
                            break;
                        }
                    }
                }
            }

            for port in closed_ports {
                rx_receivers.remove(&port);
            }

            if !got_data {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    });

    while let Some(line) = lines.next_line().await? {
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Request>(&line) {
            Ok(req) => {
                dispatch(&req.cmd, &req.args, &state, &mut session, &rx_cmd_tx).await
            }
            Err(e) => Response::error(&format!("JSON 파싱 오류: {}", e)),
        };

        let mut json = serde_json::to_string(&response)?;
        json.push('\n');
        let mut w = writer.lock().await;
        w.write_all(json.as_bytes()).await?;
    }

    notify_task.abort();
    rx_stream_task.abort();
    Ok(())
}

async fn dispatch(
    cmd: &str,
    args: &Value,
    state: &DaemonState,
    session: &mut ClientSession,
    rx_cmd_tx: &mpsc::Sender<RxSubCommand>,
) -> Response {
    let serial = &state.serial;

    match cmd {
        "list_ports" => serial.list_ports().await,

        "open_port" => {
            let resp = serial.open_port(args).await;
            if resp.ok {
                if let Some(port_name) = args.get("port").and_then(|v| v.as_str()) {
                    session.ensure_subscribed(port_name, serial).await;
                }
            }
            resp
        }

        "close_port" => {
            let resp = serial.close_port(args).await;
            if resp.ok {
                if let Some(port_name) = args.get("port").and_then(|v| v.as_str()) {
                    if session.rx_subscriptions.contains(port_name) {
                        let _ = rx_cmd_tx
                            .send(RxSubCommand::Unsubscribe(port_name.to_string()))
                            .await;
                    }
                    session.on_port_closed(port_name);
                }
            }
            resp
        }

        "subscribe_rx" => {
            let port_name = match args.get("port").and_then(|v| v.as_str()) {
                Some(p) => p.to_string(),
                None => return Response::error("port 파라미터가 필요합니다."),
            };

            if !serial.is_port_open(&port_name).await {
                return Response::error(&format!("{}가 열려 있지 않습니다.", port_name));
            }

            if session.rx_subscriptions.contains(&port_name) {
                return Response::error(&format!("{}는 이미 구독 중입니다.", port_name));
            }

            let rx = match serial.subscribe(&port_name).await {
                Some(rx) => rx,
                None => return Response::error(&format!("{} 구독 실패", port_name)),
            };

            session.rx_subscriptions.insert(port_name.clone());
            if rx_cmd_tx
                .send(RxSubCommand::Subscribe(port_name.clone(), rx))
                .await
                .is_err()
            {
                return Response::error("스트리밍 태스크 전달 실패");
            }

            Response::success_msg(&format!("{} 실시간 RX 구독 시작", port_name))
        }

        "unsubscribe_rx" => {
            let port_name = match args.get("port").and_then(|v| v.as_str()) {
                Some(p) => p.to_string(),
                None => return Response::error("port 파라미터가 필요합니다."),
            };

            if !session.rx_subscriptions.remove(&port_name) {
                return Response::error(&format!("{}는 구독 중이 아닙니다.", port_name));
            }

            let _ = rx_cmd_tx
                .send(RxSubCommand::Unsubscribe(port_name.clone()))
                .await;

            Response::success_msg(&format!("{} 실시간 RX 구독 해제", port_name))
        }

        "write_port" => serial.write_port(args).await,

        "read_port" => {
            let port_name = match args.get("port").and_then(|v| v.as_str()) {
                Some(p) => p,
                None => return Response::error("port 파라미터가 필요합니다."),
            };

            if !serial.is_port_open(port_name).await {
                return Response::error(&format!("{}가 열려 있지 않습니다.", port_name));
            }

            session.ensure_subscribed(port_name, serial).await;
            let clear = args
                .get("clear")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            session.read_port(port_name, clear)
        }

        "port_status" => {
            let mut resp = serial.port_status().await;
            // 모니터 상태 추가
            let monitor = state.monitor.lock().await;
            if let Some(ref handle) = *monitor {
                let port_data = resp.data.take().unwrap_or(Value::Array(vec![]));
                resp.data = Some(serde_json::json!({
                    "ports": port_data,
                    "monitor": {
                        "active": true,
                        "url": format!("http://127.0.0.1:{}", handle.http_port),
                    }
                }));
            } else {
                let port_data = resp.data.take().unwrap_or(Value::Array(vec![]));
                resp.data = Some(serde_json::json!({
                    "ports": port_data,
                    "monitor": { "active": false }
                }));
            }
            resp
        }

        "start_log" => serial.start_log(args).await,
        "update_log" => serial.update_log(args).await,
        "stop_log" => serial.stop_log(args).await,

        "open_monitor" => {
            let http_port = args
                .get("httpPort")
                .and_then(|v| v.as_u64())
                .unwrap_or(8765) as u16;

            let mut monitor = state.monitor.lock().await;
            if let Some(ref handle) = *monitor {
                return Response::success_msg(&format!(
                    "모니터가 이미 실행 중입니다: http://127.0.0.1:{}",
                    handle.http_port
                ));
            }

            match monitor::start(Arc::clone(serial), http_port).await {
                Ok(handle) => {
                    let url = format!("http://127.0.0.1:{}", handle.http_port);
                    *monitor = Some(handle);
                    Response::success_msg(&format!("모니터 시작: {}", url))
                }
                Err(e) => Response::error(&format!("모니터 시작 실패: {}", e)),
            }
        }

        "close_monitor" => {
            let mut monitor = state.monitor.lock().await;
            match monitor.take() {
                Some(handle) => {
                    handle.stop().await;
                    Response::success_msg("모니터 종료")
                }
                None => Response::error("모니터가 실행 중이 아닙니다."),
            }
        }

        _ => Response::error(&format!("알 수 없는 명령: {}", cmd)),
    }
}
