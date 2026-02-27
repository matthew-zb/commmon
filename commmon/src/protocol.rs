use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 클라이언트 → 데몬 요청
#[derive(Debug, Deserialize)]
pub struct Request {
    pub cmd: String,
    #[serde(default)]
    pub args: Value,
}

/// 데몬 → 클라이언트 성공 응답
#[derive(Debug, Serialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    pub fn success(data: Value) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn success_msg(msg: &str) -> Self {
        Self {
            ok: true,
            data: Some(Value::String(msg.to_string())),
            error: None,
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(msg.to_string()),
        }
    }
}

/// 데몬 → 클라이언트 비동기 알림 (푸시 이벤트)
#[derive(Debug, Clone, Serialize)]
pub struct Notification {
    pub notify: String,
    pub data: Value,
}

