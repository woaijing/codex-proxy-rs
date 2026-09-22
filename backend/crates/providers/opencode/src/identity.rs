//! OpenCode 身份与会话亲和：客户端作用域隔离，重试保留同一请求关联。

use std::fmt::Write as _;

use base64::Engine as _;
use gateway_core::engine::AttemptContext;
use gateway_core::operation::ProtocolPayload;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// 官方 identifier 使用的 base62 字符表。
const IDENTIFIER_CHARS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

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
        let scoped_session = |id: &str| identifier("ses", &[scope, project, id]);
        Self {
            // 官方 project id 取自调用方本地仓库（remote 归一化串、根提交 SHA 或字面量 `global`），
            // 网关无从得知；这里按 Client Key 与调用方声明的项目名派生隔离值，形态不与官方对齐。
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
                .unwrap_or_else(|| identifier("msg", &[scope, context.request_id().as_str()])),
            parent: value("x-parent-session-id")
                .or_else(|| value("parent_thread_id"))
                .map(scoped_session),
            affinity: digest(&[scope, project, session]),
        }
    }
}

/// 额度查询使用的稳定会话标识。
///
/// 额度查询不属于任何会话，官方 CLI 也没有对应的固定值；这里按账号派生一个稳定且形态
/// 合法的标识，让同一账号每次查询呈现同一身份，不伪造时间戳语义。
pub(crate) fn quota_session(account_id: &str) -> String {
    identifier("ses", &["quota", account_id])
}

/// 长度前缀分帧后取摘要，避免不同分段组合落到同一输入。
fn fingerprint(parts: &[&str]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    hash.finalize().into()
}

/// 十六进制摘要；项目标识与亲和键等网关内部值使用，不对上游表达语义。
pub(crate) fn digest(parts: &[&str]) -> String {
    let mut value = String::with_capacity(64);
    for byte in fingerprint(parts) {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

/// 官方 CLI 的 identifier 形态：`<前缀>_` 加 12 位十六进制、再加 14 位 base62，共 26 个字符。
///
/// 官方按毫秒时间戳递增生成这 26 位；网关要求同一逻辑请求在重试后保持同一身份，
/// 因此改为由摘要稳定派生等长字符串，只保留可辨识的形态，不伪造时间戳语义。
fn identifier(prefix: &str, parts: &[&str]) -> String {
    let fingerprint = fingerprint(parts);
    let mut value = String::with_capacity(prefix.len() + 27);
    value.push_str(prefix);
    value.push('_');
    for byte in &fingerprint[..6] {
        let _ = write!(value, "{byte:02x}");
    }
    for byte in &fingerprint[6..20] {
        value.push(IDENTIFIER_CHARS[usize::from(*byte % 62)] as char);
    }
    value
}
