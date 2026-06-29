use std::collections::HashMap;
use std::sync::{Arc, RwLock as StdRwLock};

use chrono::Local;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::protocol::{Notification, Response};

const BROADCAST_CAPACITY: usize = 256;

#[derive(Clone, Debug)]
pub struct RxEntry {
    pub timestamp: String,
    pub data: Vec<u8>,
}

struct OpenPort {
    tx: broadcast::Sender<RxEntry>,
    writer: Arc<Mutex<tokio::io::WriteHalf<tokio_serial::SerialStream>>>,
    read_task: tokio::task::JoinHandle<()>,
    baud_rate: u32,
    data_bits: u8,
    parity: char,
    stop_bits: u8,
}

struct LogState {
    file_path: String,
    stop_keyword: Arc<StdRwLock<Option<String>>>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    duration_task: Option<tokio::task::JoinHandle<()>>,
}

pub struct SerialManager {
    ports: Mutex<HashMap<String, OpenPort>>,
    logs: Mutex<HashMap<String, LogState>>,
    notify_tx: broadcast::Sender<Notification>,
}

impl SerialManager {
    pub fn new() -> Self {
        let (notify_tx, _) = broadcast::channel(64);
        Self {
            ports: Mutex::new(HashMap::new()),
            logs: Mutex::new(HashMap::new()),
            notify_tx,
        }
    }

    pub fn subscribe_notifications(&self) -> broadcast::Receiver<Notification> {
        self.notify_tx.subscribe()
    }

    pub async fn list_ports(&self) -> Response {
        match tokio::task::spawn_blocking(serialport::available_ports).await {
            Ok(Ok(ports)) => {
                let list: Vec<Value> = ports
                    .iter()
                    .map(|p| {
                        let mut obj = serde_json::Map::new();
                        obj.insert("path".into(), Value::String(p.port_name.clone()));
                        if let serialport::SerialPortType::UsbPort(info) = &p.port_type {
                            if let Some(m) = &info.manufacturer {
                                obj.insert("manufacturer".into(), Value::String(m.clone()));
                            }
                            if let Some(p) = &info.product {
                                obj.insert("product".into(), Value::String(p.clone()));
                            }
                            if let Some(s) = &info.serial_number {
                                obj.insert("serialNumber".into(), Value::String(s.clone()));
                            }
                            obj.insert(
                                "vendorId".into(),
                                Value::String(format!("{:04X}", info.vid)),
                            );
                            obj.insert(
                                "productId".into(),
                                Value::String(format!("{:04X}", info.pid)),
                            );
                        }
                        Value::Object(obj)
                    })
                    .collect();
                Response::success(Value::Array(list))
            }
            Ok(Err(e)) => Response::error(&format!("포트 목록 조회 실패: {}", e)),
            Err(e) => Response::error(&format!("포트 목록 조회 실패: {}", e)),
        }
    }

    pub async fn open_port(&self, args: &Value, manager: Arc<Self>) -> Response {
        let port_name = match args.get("port").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => return Response::error("port 파라미터가 필요합니다."),
        };
        let baud_rate = args
            .get("baudRate")
            .and_then(|v| v.as_u64())
            .unwrap_or(115200) as u32;
        let data_bits_str = args
            .get("dataBits")
            .and_then(|v| v.as_str())
            .unwrap_or("8");
        let stop_bits_str = args
            .get("stopBits")
            .and_then(|v| v.as_str())
            .unwrap_or("1");
        let parity_str = args
            .get("parity")
            .and_then(|v| v.as_str())
            .unwrap_or("none");

        let data_bits = match data_bits_str {
            "5" => tokio_serial::DataBits::Five,
            "6" => tokio_serial::DataBits::Six,
            "7" => tokio_serial::DataBits::Seven,
            _ => tokio_serial::DataBits::Eight,
        };
        let stop_bits = match stop_bits_str {
            "2" => tokio_serial::StopBits::Two,
            _ => tokio_serial::StopBits::One,
        };
        let parity = match parity_str {
            "even" => tokio_serial::Parity::Even,
            "odd" => tokio_serial::Parity::Odd,
            _ => tokio_serial::Parity::None,
        };

        let mut ports = self.ports.lock().await;
        if ports.contains_key(&port_name) {
            return Response::error(&format!("{}는 이미 열려 있습니다.", port_name));
        }

        let builder = tokio_serial::new(&port_name, baud_rate)
            .data_bits(data_bits)
            .stop_bits(stop_bits)
            .parity(parity);

        let stream = match tokio_serial::SerialStream::open(&builder) {
            Ok(s) => s,
            Err(e) => return Response::error(&format!("{} 열기 실패: {}", port_name, e)),
        };

        let (mut reader, writer) = tokio::io::split(stream);
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let tx_clone = tx.clone();
        let port_name_clone = port_name.clone();
        let notify_tx = self.notify_tx.clone();

        let read_task = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            // 루프가 끝나는 사유를 담는다: EOF(0바이트)=장치 분리, Err=읽기 오류
            let reason = loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break "장치 연결 해제".to_string(),
                    Ok(n) => {
                        let entry = RxEntry {
                            timestamp: Local::now().format("%Y:%m:%d %H:%M:%S").to_string(),
                            data: buf[..n].to_vec(),
                        };
                        let _ = tx_clone.send(entry);
                    }
                    Err(e) => {
                        error!("{} 읽기 오류: {}", port_name_clone, e);
                        break format!("읽기 오류: {}", e);
                    }
                }
            };

            // 포트가 분리/오류로 끊겼다. 활성 로그를 멈추고 포트 엔트리를 제거한 뒤
            // 클라이언트에 port_error 알림을 보낸다. 이 정리가 없으면 포트가 ports 맵에
            // 남아 is_port_open/port_status가 계속 열린 것으로 보고하고, 구독자도 채널
            // 종료(Closed) 신호를 받지 못한다.
            manager.stop_log_internal(&port_name_clone).await;
            manager.ports.lock().await.remove(&port_name_clone);
            let _ = notify_tx.send(Notification {
                notify: "port_error".into(),
                data: serde_json::json!({
                    "port": port_name_clone,
                    "error": reason,
                }),
            });
        });

        let db = match data_bits_str {
            "5" => 5,
            "6" => 6,
            "7" => 7,
            _ => 8,
        };
        let pc = match parity_str {
            "even" => 'E',
            "odd" => 'O',
            _ => 'N',
        };
        let sb: u8 = match stop_bits_str {
            "2" => 2,
            _ => 1,
        };

        ports.insert(
            port_name.clone(),
            OpenPort {
                tx,
                writer: Arc::new(Mutex::new(writer)),
                read_task,
                baud_rate,
                data_bits: db,
                parity: pc,
                stop_bits: sb,
            },
        );

        Response::success_msg(&format!(
            "{} 열기 성공 ({}bps, {}{}{})",
            port_name, baud_rate, db, pc, sb
        ))
    }

    pub async fn close_port(&self, args: &Value) -> Response {
        let port_name = match args.get("port").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => return Response::error("port 파라미터가 필요합니다."),
        };

        // 활성 로그가 있으면 먼저 중지
        self.stop_log_internal(&port_name).await;

        let mut ports = self.ports.lock().await;
        match ports.remove(&port_name) {
            Some(port) => {
                port.read_task.abort();
                Response::success_msg(&format!("{} 닫기 성공", port_name))
            }
            None => Response::error(&format!("{}가 열려 있지 않습니다.", port_name)),
        }
    }

    pub async fn write_port(&self, args: &Value) -> Response {
        let port_name = match args.get("port").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => return Response::error("port 파라미터가 필요합니다."),
        };
        let data_str = match args.get("data").and_then(|v| v.as_str()) {
            Some(d) => d.to_string(),
            None => return Response::error("data 파라미터가 필요합니다."),
        };
        let encoding = args
            .get("encoding")
            .and_then(|v| v.as_str())
            .unwrap_or("ascii");

        let bytes = match encoding {
            "hex" => match hex::decode(&data_str) {
                Ok(b) => b,
                Err(e) => return Response::error(&format!("HEX 디코딩 실패: {}", e)),
            },
            _ => data_str.into_bytes(),
        };

        let ports = self.ports.lock().await;
        match ports.get(&port_name) {
            Some(port) => {
                let mut writer = port.writer.lock().await;
                match writer.write_all(&bytes).await {
                    Ok(_) => Response::success_msg(&format!(
                        "{} 전송 완료 ({} 바이트)",
                        port_name,
                        bytes.len()
                    )),
                    Err(e) => Response::error(&format!("{} 전송 실패: {}", port_name, e)),
                }
            }
            None => Response::error(&format!("{}가 열려 있지 않습니다.", port_name)),
        }
    }

    pub async fn subscribe(&self, port_name: &str) -> Option<broadcast::Receiver<RxEntry>> {
        let ports = self.ports.lock().await;
        ports.get(port_name).map(|p| p.tx.subscribe())
    }

    pub async fn is_port_open(&self, port_name: &str) -> bool {
        self.ports.lock().await.contains_key(port_name)
    }

    pub async fn open_port_names(&self) -> Vec<String> {
        self.ports.lock().await.keys().cloned().collect()
    }

    // ── 로깅 ──

    pub async fn start_log(&self, args: &Value) -> Response {
        let port_name = match args.get("port").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => return Response::error("port 파라미터가 필요합니다."),
        };

        // 포트가 열려 있는지 확인
        let rx = match self.subscribe(&port_name).await {
            Some(rx) => rx,
            None => return Response::error(&format!("{}가 열려 있지 않습니다.", port_name)),
        };

        let mut logs = self.logs.lock().await;
        if logs.contains_key(&port_name) {
            return Response::error(&format!("{}의 로그가 이미 실행 중입니다.", port_name));
        }

        // 파일 경로 결정
        let file_path = match args.get("filePath").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => {
                let ts = Local::now().format("%Y%m%d_%H%M%S");
                let tmp = std::env::temp_dir();
                tmp.join(format!("commmon_{}_{}.log", port_name, ts))
                    .to_string_lossy()
                    .to_string()
            }
        };

        let stop_keyword: Arc<StdRwLock<Option<String>>> = Arc::new(StdRwLock::new(
            args.get("stopKeyword")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        ));

        let cancel = CancellationToken::new();

        // 로그 태스크 생성
        let task = tokio::spawn(Self::log_task(
            rx,
            file_path.clone(),
            port_name.clone(),
            stop_keyword.clone(),
            cancel.clone(),
            self.notify_tx.clone(),
        ));

        // duration 타이머
        let duration_task = args
            .get("duration")
            .and_then(|v| v.as_u64())
            .map(|secs| {
                let cancel = cancel.clone();
                let notify_tx = self.notify_tx.clone();
                let port_name = port_name.clone();
                let file_path = file_path.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                    cancel.cancel();
                    let _ = notify_tx.send(Notification {
                        notify: "log_stopped".into(),
                        data: serde_json::json!({
                            "port": port_name,
                            "reason": "duration",
                            "file": file_path,
                        }),
                    });
                })
            });

        logs.insert(
            port_name.clone(),
            LogState {
                file_path: file_path.clone(),
                stop_keyword,
                cancel,
                task,
                duration_task,
            },
        );

        Response::success_msg(&format!("{} 로그 시작: {}", port_name, file_path))
    }

    pub async fn update_log(&self, args: &Value) -> Response {
        let port_name = match args.get("port").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => return Response::error("port 파라미터가 필요합니다."),
        };

        let mut logs = self.logs.lock().await;
        let log = match logs.get_mut(&port_name) {
            Some(l) => l,
            None => return Response::error(&format!("{}의 활성 로그가 없습니다.", port_name)),
        };

        let mut updated = Vec::new();

        // 키워드 업데이트
        if let Some(kw_val) = args.get("stopKeyword") {
            let new_kw = if kw_val.as_str() == Some("") {
                None
            } else {
                kw_val.as_str().map(|s| s.to_string())
            };
            *log.stop_keyword.write().unwrap() = new_kw.clone();
            updated.push(format!(
                "키워드: {}",
                new_kw.as_deref().unwrap_or("(해제)")
            ));
        }

        // duration 업데이트
        if let Some(secs) = args.get("duration").and_then(|v| v.as_u64()) {
            // 기존 타이머 취소
            if let Some(t) = log.duration_task.take() {
                t.abort();
            }
            let cancel = log.cancel.clone();
            let notify_tx = self.notify_tx.clone();
            let pn = port_name.clone();
            let fp = log.file_path.clone();
            log.duration_task = Some(tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                cancel.cancel();
                let _ = notify_tx.send(Notification {
                    notify: "log_stopped".into(),
                    data: serde_json::json!({
                        "port": pn,
                        "reason": "duration",
                        "file": fp,
                    }),
                });
            }));
            updated.push(format!("타이머: {}초", secs));
        }

        if updated.is_empty() {
            return Response::error("업데이트할 파라미터가 없습니다. (stopKeyword, duration)");
        }

        Response::success_msg(&format!(
            "{} 로그 업데이트: {}",
            port_name,
            updated.join(", ")
        ))
    }

    pub async fn stop_log(&self, args: &Value) -> Response {
        let port_name = match args.get("port").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => return Response::error("port 파라미터가 필요합니다."),
        };

        match self.stop_log_internal(&port_name).await {
            Some(file_path) => Response::success(serde_json::json!({
                "port": port_name,
                "file": file_path,
            })),
            None => Response::error(&format!("{}의 활성 로그가 없습니다.", port_name)),
        }
    }

    async fn stop_log_internal(&self, port_name: &str) -> Option<String> {
        let mut logs = self.logs.lock().await;
        let log = logs.remove(port_name)?;
        log.cancel.cancel();
        if let Some(t) = log.duration_task {
            t.abort();
        }
        // 태스크 종료 대기 (짧게)
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), log.task).await;
        info!("{} 로그 중지: {}", port_name, log.file_path);
        Some(log.file_path)
    }

    async fn log_task(
        mut rx: broadcast::Receiver<RxEntry>,
        file_path: String,
        port_name: String,
        stop_keyword: Arc<StdRwLock<Option<String>>>,
        cancel: CancellationToken,
        notify_tx: broadcast::Sender<Notification>,
    ) {
        let file = match tokio::fs::File::create(&file_path).await {
            Ok(f) => f,
            Err(e) => {
                error!("{} 로그 파일 생성 실패: {}", port_name, e);
                return;
            }
        };
        let mut writer = tokio::io::BufWriter::new(file);

        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                result = rx.recv() => {
                    match result {
                        Ok(entry) => {
                            let text = String::from_utf8_lossy(&entry.data);
                            let line = format!("[{}] {}\n", entry.timestamp, text);
                            let _ = writer.write_all(line.as_bytes()).await;
                            let _ = writer.flush().await;

                            // 키워드 체크
                            let keyword = stop_keyword.read().unwrap().clone();
                            if let Some(kw) = keyword {
                                if text.contains(&kw) {
                                    let _ = notify_tx.send(Notification {
                                        notify: "log_stopped".into(),
                                        data: serde_json::json!({
                                            "port": port_name,
                                            "reason": "keyword",
                                            "keyword": kw,
                                            "file": file_path,
                                        }),
                                    });
                                    break;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("{} 로그 데이터 {}건 누락", port_name, n);
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }

        let _ = writer.flush().await;
    }

    // ── 상태 ──

    pub async fn port_status(&self) -> Response {
        let ports = self.ports.lock().await;
        let logs = self.logs.lock().await;

        let list: Vec<Value> = ports
            .iter()
            .map(|(name, port)| {
                let log_info = logs.get(name).map(|l| {
                    let kw = l.stop_keyword.read().unwrap().clone();
                    serde_json::json!({
                        "file": l.file_path,
                        "stopKeyword": kw,
                    })
                });
                serde_json::json!({
                    "port": name,
                    "baudRate": port.baud_rate,
                    "config": format!("{}{}{}", port.data_bits, port.parity, port.stop_bits),
                    "subscribers": port.tx.receiver_count(),
                    "log": log_info,
                })
            })
            .collect();
        Response::success(Value::Array(list))
    }

    /// 모든 포트/로그 닫기 (SIGINT 클린업용)
    pub async fn close_all(&self) {
        // 먼저 모든 로그 중지
        let mut logs = self.logs.lock().await;
        for (name, log) in logs.drain() {
            log.cancel.cancel();
            if let Some(t) = log.duration_task {
                t.abort();
            }
            info!("{} 로그 중지", name);
        }
        drop(logs);

        let mut ports = self.ports.lock().await;
        for (name, port) in ports.drain() {
            port.read_task.abort();
            info!("{} 닫기 완료", name);
        }
    }
}
