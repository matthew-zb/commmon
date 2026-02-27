use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::Html;
use axum::routing::get;
use axum::Router;
use futures::stream::Stream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::serial::{RxEntry, SerialManager};

const MONITOR_HTML: &str = include_str!("monitor.html");

struct MonitorState {
    serial: Arc<SerialManager>,
}

/// 모니터 서버 핸들
pub struct MonitorHandle {
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    pub http_port: u16,
}

impl MonitorHandle {
    pub async fn stop(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}

pub async fn start(serial: Arc<SerialManager>, http_port: u16) -> anyhow::Result<MonitorHandle> {
    let state = Arc::new(MonitorState {
        serial: Arc::clone(&serial),
    });

    let app = Router::new()
        .route("/", get(serve_html))
        .route("/events", get(sse_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", http_port)).await?;
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    info!("모니터 시작: http://127.0.0.1:{}", http_port);

    let task = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app)
            .with_graceful_shutdown(cancel_clone.cancelled_owned())
            .await
        {
            error!("모니터 서버 오류: {}", e);
        }
        info!("모니터 종료");
    });

    Ok(MonitorHandle {
        cancel,
        task,
        http_port,
    })
}

async fn serve_html() -> Html<&'static str> {
    Html(MONITOR_HTML)
}

async fn sse_handler(
    State(state): State<Arc<MonitorState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // 모든 열린 포트의 broadcast를 구독
    let serial = Arc::clone(&state.serial);

    let stream = async_stream::stream! {
        // 포트별 리시버를 관리
        let mut receivers: HashMap<String, broadcast::Receiver<RxEntry>> = HashMap::new();

        loop {
            // 새로 열린 포트 구독 갱신 (1초마다)
            // 간단한 구현: 매번 subscribe 시도
            let port_names = serial.open_port_names().await;
            for name in &port_names {
                if !receivers.contains_key(name) {
                    if let Some(rx) = serial.subscribe(name).await {
                        receivers.insert(name.clone(), rx);
                    }
                }
            }
            // 닫힌 포트 제거
            receivers.retain(|name, _| port_names.contains(name));

            if receivers.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }

            // 모든 리시버에서 데이터 체크
            let mut got_data = false;
            for (port_name, rx) in receivers.iter_mut() {
                while let Ok(entry) = rx.try_recv() {
                    got_data = true;
                    let ascii = String::from_utf8_lossy(&entry.data).to_string();
                    let hex_str = hex::encode(&entry.data);
                    let payload = serde_json::json!({
                        "port": port_name,
                        "timestamp": entry.timestamp,
                        "ascii": ascii,
                        "hex": hex_str,
                    });
                    yield Ok(Event::default().data(payload.to_string()));
                }
            }

            if !got_data {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}
