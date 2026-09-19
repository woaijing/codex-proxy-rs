//! OpenCode 身份与会话亲和：客户端作用域隔离，重试保留同一请求关联。

use base64::Engine as _;
use gateway_core::engine::AttemptContext;
use gateway_core::operation::ProtocolPayload;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub(crate) struct Identity {
    pub(crate) project: String,
    pub(crate) session: String,
    pub(crate) request: String,
    pub(crate) parent: Option<String>,
    pub(crate) affinity: String,
}

impl Identity {
    pub(crate) fn new(payload: &ProtocolPayload, context: &AttemptContext) -> Self {
        let headers = payload
            .context()
            .get("opaque_request_headers")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|header| {
                let pair = header.as_array()?;
                let name = pair.first()?.as_str()?;
                if !matches!(
                    name,
                    "x-opencode-project"
                        | "x-opencode-session"
                        | "x-opencode-request"
                        | "x-parent-session-id"
                ) {
                    return None;
                }
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(pair.get(1)?.as_str()?)
                    .ok()?;
                Some((
                    name.to_owned(),
                    Value::String(String::from_utf8(bytes).ok()?),
                ))
            })
            .collect::<serde_json::Map<_, _>>();
        let value = |key: &str| {
            headers
                .get(key)
                .or_else(|| payload.context().get(key))
                .or_else(|| {
                    payload
                        .body()
                        .get("metadata")
                        .and_then(|metadata| metadata.get(key))
                })
                .and_then(Value::as_str)
                .filter(|text| {
                    !text.is_empty()
                        && text.len() <= 256
                        && text.bytes().all(|b| b.is_ascii_graphic())
                })
        };
        let scope = context.client_api_key_ref().as_str();
        let project = value("x-opencode-project").unwrap_or("default");
        let session = value("x-opencode-session")
            .or_else(|| value("session_id"))
            .or_else(|| {
                payload
                    .body()
                    .get("prompt_cache_key")
                    .and_then(Value::as_str)
            })
            .unwrap_or(context.request_id().as_str());
        let scoped_session = |id: &str| format!("ses_{}", digest(&[scope, project, id]));
        Self {
            project: digest(&[scope, project]),
            session: scoped_session(session),
            request: value("x-opencode-request")
                .or_else(|| {
                    payload
                        .body()
                        .get("input")
                        .and_then(Value::as_array)
                        .and_then(|items| {
                            items.iter().rev().find(|item| {
                                item.get("role").and_then(Value::as_str) == Some("user")
                            })
                        })
                        .and_then(|item| item.get("id"))
                        .and_then(Value::as_str)
                        .filter(|id| {
                            !id.is_empty()
                                && id.len() <= 256
                                && id.bytes().all(|byte| byte.is_ascii_graphic())
                        })
                })
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!("msg_{}", digest(&[scope, context.request_id().as_str()]))
                }),
            parent: value("x-parent-session-id")
                .or_else(|| value("parent_thread_id"))
                .map(scoped_session),
            affinity: digest(&[scope, project, session]),
        }
    }
}

pub(crate) fn digest(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
