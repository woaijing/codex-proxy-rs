//! 单账号冷流执行；跨账号重试、取消与下游提交由 Core 统一协调。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use futures::StreamExt as _;
use gateway_core::engine::AttemptContext;
use gateway_core::engine::provider::{
    Provider, ProviderCallMetadata, ProviderRequest, ProviderStream,
};
use gateway_core::error::{ProviderError, ProviderErrorKind};
use gateway_core::event::{ProviderEvent, ProviderResponseObservation};
use gateway_core::operation::Operation;
use gateway_core::provider_ports::{
    ProviderCooldown, ProviderSessionAffinityKey, ProviderStorePorts,
};
use gateway_core::routing::{ProviderKind, ProviderModelCapabilities};
use gateway_core::upstream::{UpstreamSendState, UpstreamTransport};
use gateway_protocol::openai::sse::SseEventDecoder;

use crate::catalog::{Catalog, Protocol};
use crate::identity::Identity;
use crate::selection::{Selector, error, infrastructure};
use crate::stream::{Decoder, protocol};

/// 官方 CLI 版本；出站身份头与 Dashboard 画像共用同一事实，不允许各自维护副本。
pub(crate) const CLIENT_VERSION: &str = "1.18.31";

/// 官方 CLI 客户端标识，对应 `x-opencode-client`。
pub(crate) const CLIENT_KIND: &str = "cli";

/// AI SDK 在 `postToApi` 内追加到 User-Agent 的片段，与 opencode 1.18.31 锁定的依赖一致：
/// `ai` 6.0.168 与 `@ai-sdk/openai-compatible` 2.0.41 都依赖 `@ai-sdk/provider-utils` 4.0.23，
/// 而该包按 `ai-sdk/provider-utils/${VERSION}` 拼接；`runtime/bun/<版本>` 来自
/// `getRuntimeEnvironmentUserAgent()` 对 Bun `navigator.userAgent` 的小写化，版本随 `packageManager: bun@1.3.14`。
const SDK_USER_AGENT_SUFFIX: &str = "ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14";

/// 出站 User-Agent。官方 CLI 自身只写 `opencode/<版本>`，其余片段由 SDK 层追加；
/// 升级 opencode 或 SDK 版本时三段都要重新核对，不能只改版本号。
pub(crate) fn user_agent() -> String {
    format!("opencode/{CLIENT_VERSION} {SDK_USER_AGENT_SUFFIX}")
}

/// OpenCode Zen / Go Provider；通过 Core 端口选择账号并持有租约。
pub struct OpenCodeProvider {
    selector: Arc<Selector>,
    endpoints: Arc<dyn OpenCodeEndpointPolicy>,
}

/// Provider 宿主控制的端点解析；不能从下游请求接收此策略。
pub trait OpenCodeEndpointPolicy: Send + Sync {
    fn endpoint(&self, product: &str, path: &str) -> String;
}

pub(crate) struct OfficialEndpoints;
impl OpenCodeEndpointPolicy for OfficialEndpoints {
    fn endpoint(&self, product: &str, path: &str) -> String {
        let tier = if product == "go" {
            crate::credential::Tier::Go
        } else {
            crate::credential::Tier::Zen
        };
        format!("{}/{}", tier.base_url(), path)
    }
}

impl OpenCodeProvider {
    pub(crate) fn new(
        kind: ProviderKind,
        ports: ProviderStorePorts,
        catalog: Arc<Catalog>,
        endpoints: Arc<dyn OpenCodeEndpointPolicy>,
    ) -> Self {
        Self {
            selector: Arc::new(Selector::new(kind, ports, catalog)),
            endpoints,
        }
    }
}

#[async_trait]
impl Provider for OpenCodeProvider {
    fn name(&self) -> &'static str {
        "opencode"
    }

    fn catalog_generation(&self) -> gateway_core::routing::ProviderCatalogGeneration {
        gateway_core::routing::ProviderCatalogGeneration::new(1)
    }

    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(self.selector.catalog.capabilities())
    }

    async fn execute(
        &self,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let Operation::Generate(generate) = request.operation() else {
            return Err(error(ProviderErrorKind::Unsupported));
        };
        let payload = generate.protocol_payload();
        let identity = Identity::new(payload, &context);
        let selected = Arc::new(
            self.selector
                .select(request.candidate(), &context, &identity.affinity)
                .await?,
        );
        let model_id = request
            .candidate()
            .upstream_model()
            .ok_or_else(|| error(ProviderErrorKind::InvalidRequest))?;
        let model = self
            .selector
            .catalog
            .find(selected.credential.tier, model_id.as_str())
            .ok_or_else(|| error(ProviderErrorKind::Unsupported))?;
        let body = crate::request::encode(payload, model)?;
        let upstream_protocol = model.protocol;
        let endpoint = self
            .endpoints
            .endpoint(selected.credential.tier.as_str(), upstream_protocol.path());
        let timeout = context
            .deadline()
            .duration_since(SystemTime::now())
            .map_err(|_| error(ProviderErrorKind::Timeout))?;
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(30))
            .timeout(timeout);
        if let Some(proxy) = selected.account.outbound_proxy() {
            builder = builder
                .proxy(reqwest::Proxy::all(proxy.expose_url()).map_err(|_| infrastructure())?);
        }
        let client = builder.build().map_err(|_| infrastructure())?;
        let mut upstream = client
            .post(endpoint)
            .json(&body)
            .header("accept", "text/event-stream")
            .header("user-agent", user_agent())
            .header("x-opencode-client", CLIENT_KIND)
            .header("x-opencode-project", &identity.project)
            .header("x-opencode-session", &identity.session)
            .header("x-opencode-request", &identity.request);
        if let Some(parent) = &identity.parent {
            upstream = upstream.header("x-parent-session-id", parent);
        }
        upstream = if upstream_protocol == Protocol::Messages {
            upstream
                .header("x-api-key", &selected.credential.api_key)
                .header("anthropic-version", "2023-06-01")
        } else {
            upstream.bearer_auth(&selected.credential.api_key)
        };
        let upstream = upstream
            .build()
            .map_err(|_| error(ProviderErrorKind::InvalidRequest))?;
        let transport = UpstreamTransport::new("http_sse").map_err(|_| infrastructure())?;
        let metadata = ProviderCallMetadata::new(
            self.selector.kind.clone(),
            model_id.clone(),
            selected.account.id().clone(),
            transport.clone(),
        );
        let selector = Arc::clone(&self.selector);
        let lease = Arc::clone(&selected);
        let model_id = model_id.clone();
        let events = async_stream::try_stream! {
            let remaining = context.deadline().duration_since(SystemTime::now())
                .map_err(|_| error(ProviderErrorKind::Timeout))?;
            let deadline = tokio::time::Instant::now() + remaining;
            // request.build 只编码材料；实际 HTTP 调用必须留在首次 poll 之后。
            let response = tokio::select! {
                biased;
                _ = context.cancellation().cancelled() => Err(error(ProviderErrorKind::Cancelled)),
                _ = tokio::time::sleep_until(deadline) => Err(ProviderError::new(ProviderErrorKind::Timeout, UpstreamSendState::Ambiguous)),
                response = client.execute(upstream) => response.map_err(|failure| {
                    ProviderError::new(if failure.is_timeout() {ProviderErrorKind::Timeout} else {ProviderErrorKind::Transport},
                        if failure.is_connect() {UpstreamSendState::NotSent} else {UpstreamSendState::Ambiguous})
                }),
            }?;
            let status = response.status();
            if !status.is_success() {
                let retry = response.headers().get("retry-after").and_then(|value| value.to_str().ok()).and_then(retry_after);
                let kind = match status.as_u16() {
                    401 => ProviderErrorKind::Unauthorized, 403 => ProviderErrorKind::PermissionDenied,
                    402 => ProviderErrorKind::QuotaExhausted, 429 => ProviderErrorKind::RateLimited,
                    500..=599 => ProviderErrorKind::Unavailable, _ => ProviderErrorKind::InvalidRequest,
                };
                if matches!(status.as_u16(), 401 | 402 | 403 | 429 | 500..=599) && selected.account.enabled() {
                    let delay = retry.unwrap_or(Duration::from_secs(if status.as_u16() == 429 {120} else {60}));
                    let until = SystemTime::now().checked_add(delay).unwrap_or(context.deadline());
                    selector.ports.cooldowns().put_if_later(ProviderCooldown::new(selected.account.id().clone(), selected.account.revision(), until))
                        .await.map_err(|_| ProviderError::new(ProviderErrorKind::ProviderInfrastructureUnavailable, UpstreamSendState::Sent))?;
                }
                let mut failure = ProviderError::new(kind, UpstreamSendState::Sent).with_status(status.as_u16());
                if matches!(status.as_u16(), 401 | 402 | 403 | 429) { failure = failure.with_replay_safe(); }
                if let Some(delay) = retry { failure = failure.with_retry_after(delay); }
                Err(failure)?;
            }
            if !response.headers().get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.split(';').next().is_some_and(|mime| mime.trim().eq_ignore_ascii_case("text/event-stream"))) {
                Err(protocol())?;
            }
            yield ProviderEvent::observation(ProviderResponseObservation::new(transport).with_status_code(status.as_u16()));
            let mut sse = SseEventDecoder::default().with_done_events();
            let mut decoder = Decoder::new(upstream_protocol, model_id.as_str());
            let mut chunks = response.bytes_stream();
            while !decoder.terminal {
                let chunk = tokio::select! {
                    biased;
                    _ = context.cancellation().cancelled() => Err(ProviderError::new(ProviderErrorKind::Cancelled, UpstreamSendState::Sent)),
                    _ = tokio::time::sleep_until(deadline) => Err(ProviderError::new(ProviderErrorKind::Timeout, UpstreamSendState::Sent)),
                    chunk = chunks.next() => Ok(chunk),
                }?;
                let Some(chunk) = chunk else { break; };
                let chunk = chunk.map_err(|failure| ProviderError::new(if failure.is_timeout() {ProviderErrorKind::Timeout} else {ProviderErrorKind::Transport}, UpstreamSendState::Sent))?;
                for event in sse.push(&chunk).map_err(|_| protocol())? {
                    let events = decoder.decode(event)?;
                    if decoder.terminal && selected.account.enabled() {
                        let key = ProviderSessionAffinityKey::try_new(identity.affinity.clone()).map_err(|_| protocol())?;
                        // 在交付终态前更新亲和；Core 消费终态后可以立即丢弃流。CAS 保留并发更新。
                        let affinity = selector.ports.session_affinity();
                        let _ = tokio::time::timeout(Duration::from_secs(1), affinity.compare_and_bind(
                            &selector.kind, &key, &selected.affinity_owner, selected.account.id(), Duration::from_secs(24 * 60 * 60)
                        )).await;
                    }
                    for event in events { yield event; }
                }
            }
            if !decoder.terminal { Err(protocol())?; }
        };
        Ok(ProviderStream::new(metadata, events, lease)
            .with_account_feedback(self.selector.ports.account_feedback()))
    }
}

pub(crate) fn retry_after(value: &str) -> Option<Duration> {
    value
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(value)
                .ok()?
                .duration_since(SystemTime::now())
                .ok()
        })
        .filter(|duration| !duration.is_zero())
}
