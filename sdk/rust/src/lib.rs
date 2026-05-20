//! commmon 데몬 실시간 RX 데이터 수신 SDK
//!
//! TCP 접속 → `subscribe_rx` → `rx_data` notification 수신 → broadcast로 전달

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

/// 수신 데이터 구조체
#[derive(Debug, Clone, Deserialize)]
pub struct RxData {
    pub port: String,
    pub timestamp: String,
    pub ascii: String,
    pub hex: String,
}

#[derive(Deserialize)]
struct Notification {
    notify: String,
    data: serde_json::Value,
}

#[derive(Serialize)]
struct Request {
    cmd: String,
    args: serde_json::Value,
}

/// commmon 데몬 실시간 RX 클라이언트
pub struct CommmonRxClient {
    writer: tokio::io::WriteHalf<TcpStream>,
    tx: broadcast::Sender<RxData>,
    _recv_task: tokio::task::JoinHandle<()>,
}

impl CommmonRxClient {
    /// 데몬에 TCP 접속
    pub async fn connect(host: &str, port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(format!("{}:{}", host, port)).await?;
        let (reader, writer) = tokio::io::split(stream);
        let (tx, _) = broadcast::channel::<RxData>(256);
        let tx_clone = tx.clone();

        let recv_task = tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let line = line.trim().to_string();
                if line.is_empty() {
                    continue;
                }

                if let Ok(notif) = serde_json::from_str::<Notification>(&line) {
                    if notif.notify == "rx_data" {
                        if let Ok(data) = serde_json::from_value::<RxData>(notif.data) {
                            let _ = tx_clone.send(data);
                        }
                    }
                }
            }
        });

        Ok(Self {
            writer,
            tx,
            _recv_task: recv_task,
        })
    }

    /// RX 데이터 수신 채널 획득
    pub fn on_data(&self) -> broadcast::Receiver<RxData> {
        self.tx.subscribe()
    }

    /// 포트 실시간 RX 구독 시작
    pub async fn subscribe(&mut self, com_port: &str) -> Result<(), Box<dyn std::error::Error>> {
        let req = Request {
            cmd: "subscribe_rx".into(),
            args: serde_json::json!({ "port": com_port }),
        };
        let mut msg = serde_json::to_string(&req)?;
        msg.push('\n');
        self.writer.write_all(msg.as_bytes()).await?;
        Ok(())
    }

    /// 포트 실시간 RX 구독 해제
    pub async fn unsubscribe(&mut self, com_port: &str) -> Result<(), Box<dyn std::error::Error>> {
        let req = Request {
            cmd: "unsubscribe_rx".into(),
            args: serde_json::json!({ "port": com_port }),
        };
        let mut msg = serde_json::to_string(&req)?;
        msg.push('\n');
        self.writer.write_all(msg.as_bytes()).await?;
        Ok(())
    }
}
