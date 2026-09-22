//! OpenCode Go 套餐额度：上游 usage 合同的解释与查询。
//!
//! 合同以官方 console 源码为准（`anomalyco/opencode` 的
//! `packages/console/app/src/routes/zen/go/v1/usage.ts` 与 `packages/console/core/src/subscription.ts`）：
//! `GET /zen/go/v1/usage` 返回 `usage.{rolling,weekly,monthly}`，每块为
//! `{ status, percent, resetsAt }`，`status` 只有 `ok` / `rate-limited` 两个取值。
//!
//! 官方判定逻辑决定了两件本模块必须照做的事实：
//!
//! - `status == "rate-limited"` 与 `percent == 100` **恒等价**。额度未用尽时 `status` 为 `ok`
//!   且 `percent` 是 `floor(min(100, 已用/额度*100))`，因已用严格小于额度而必然小于 100；
//!   用尽时 `status` 为 `rate-limited` 且 `percent` 恰为 100。所以两者取或不会把"上游说可用"
//!   读成触顶，只是容忍字段缺失。
//! - `resetsAt` 由"请求时刻 + 剩余秒数"算出，是**该窗口的重置时刻**而非固定边界；窗口长度由
//!   上游配置与订阅关系决定（5 小时窗口来自 `ZEN_LIMITS.rollingWindow`，周窗口是 UTC 周一对齐
//!   的 7 天，月窗口按订阅日锚定、天数随月份在 28–31 之间变化）。
//!
//! 官方 Go 文档（<https://opencode.ai/docs/go/>）另外声明订阅按 5 小时 / 每周 / 每月三个窗口
//! 限额，分别为月度额度的 20% / 50% / 100%，并说明额度按模型以月度美元金额定义；文档本身
//! 没有记载用量查询端点，端点与字段名以上面的源码为准。Zen 套餐没有对应路径
//! （同源 `/zen/v1/usage` 实测 404），因此不查询，也不推断。
//!
//! 边界：官方 console 把同一路由的请求按 Key 前缀分流——`oc_sk_` 开头的新 Key 会被转发到
//! 迁移后的推理服务（`ConsoleMigration.inferenceUrl`），旧 Key 才落到上面这段实现。两边都
//! 实测返回同一结构，但迁移服务不在可核对的源码范围内，其响应结构变化时这里不会自动跟上。
//!
//! 上游响应只给出比例与重置时刻，不含窗口时长、额度绝对值与模型维度；这里只解释可
//! 核验的部分，缺失字段保持未知，不用推断值填充。

use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use gateway_admin::model::provider_credentials::{
    ProviderQuotaWindow, ProviderQuotaWindowRole, QuotaLocalUsageAttribution,
};
use gateway_core::account::{OutboundProxy, QuotaEvidence, QuotaState};
use serde_json::{Map, Value};

/// 相对套餐 base URL 的额度路径。
pub(crate) const USAGE_PATH: &str = "usage";

/// 上游声明窗口已被限流时的 `status` 取值。
const RATE_LIMITED: &str = "rate-limited";

/// 上游对无权限 Key 返回的错误类型；用于与边缘节点的拦截页区分。
const ENTITLEMENT_ERROR: &str = "EntitlementError";

/// 连接与整体超时。额度查询是交互式诊断，不参与数据面重试。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// 上游声明的三个窗口。
///
/// `seconds` 只填写能确定的固定跨度：周窗口是 UTC 周一对齐的 7 天，长度固定；5 小时窗口按
/// 官方文档声明的跨度取值（上游由 `ZEN_LIMITS.rollingWindow` 配置决定，网关读不到该配置，
/// 配置调整后此值会偏大或偏小）。月窗口按订阅日锚定，天数随月份在 28–31 之间变化，没有固定
/// 秒数，因此留空。留空表示时长未知，该窗口不参与按窗口的用量统计。
struct WindowSpec {
    key: &'static str,
    group: &'static str,
    label: &'static str,
    role: ProviderQuotaWindowRole,
    seconds: Option<u64>,
}

const WINDOWS: [WindowSpec; 3] = [
    WindowSpec {
        key: "rolling",
        group: "shortTerm",
        label: "5小时额度",
        role: ProviderQuotaWindowRole::Primary,
        seconds: Some(5 * 60 * 60),
    },
    WindowSpec {
        key: "weekly",
        group: "shortTerm",
        label: "周额度",
        role: ProviderQuotaWindowRole::Secondary,
        seconds: Some(7 * 24 * 60 * 60),
    },
    WindowSpec {
        key: "monthly",
        group: "monthly",
        label: "月额度",
        role: ProviderQuotaWindowRole::Monthly,
        seconds: None,
    },
];

/// 额度查询失败；不携带上游正文与凭据。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuotaError {
    /// 上游拒绝了凭据。
    Unauthorized,
    /// 上游明确表示该 Key 没有 Go 订阅。
    NoPlan,
    /// 上游返回的正文不是可解释的额度合同。
    Invalid,
    /// 网络失败、上游 5xx 或其它未归类的拒绝。
    Upstream,
}

/// 从上游正文取出 Provider 私有的 usage 文档。
///
/// 缺少对象形态的 `usage` 说明响应不是额度合同；此时返回 `Invalid` 而不是空窗口，
/// 避免把"查不到"误报成"没有限额"。
pub(crate) fn usage_document(body: &Value) -> Result<Map<String, Value>, QuotaError> {
    body.get("usage")
        .and_then(Value::as_object)
        .cloned()
        .filter(|usage| !usage.is_empty())
        .ok_or(QuotaError::Invalid)
}

/// 把 Provider 私有的 usage 文档解释成公共额度窗口。
///
/// 刷新与读取共用同一入口，保证持久化文档重新读出后的投影与首次查询一致。
pub(crate) fn interpret(
    document: &Map<String, Value>,
) -> Result<Vec<ProviderQuotaWindow>, QuotaError> {
    let mut windows = Vec::new();
    for spec in &WINDOWS {
        let Some(source) = document.get(spec.key).and_then(Value::as_object) else {
            continue;
        };
        if let Some(window) = project_window(spec, source) {
            windows.push(window);
        }
    }
    if windows.is_empty() {
        return Err(QuotaError::Invalid);
    }
    Ok(windows)
}

/// 投影一个上游窗口；上游块内没有任何可解释字段时视为未声明该窗口。
fn project_window(spec: &WindowSpec, source: &Map<String, Value>) -> Option<ProviderQuotaWindow> {
    let percent = source
        .get("percent")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 100.0));
    let status = source.get("status").and_then(Value::as_str);
    let reset_at = source
        .get("resetsAt")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    if percent.is_none() && status.is_none() && reset_at.is_none() {
        return None;
    }
    Some(ProviderQuotaWindow {
        key: spec.key.to_owned(),
        group: spec.group.to_owned(),
        label: spec.label.to_owned(),
        limit_id: None,
        limit_name: None,
        role: Some(spec.role),
        // 官方文档按模型声明限额，上游响应不带模型维度；通用账号级用量无法归属到
        // 这些窗口，因此不声称窗口覆盖账号的全部请求。
        local_usage_attribution: QuotaLocalUsageAttribution::Unavailable,
        window_seconds: spec.seconds,
        used_percent: percent,
        reset_at,
        // 官方判定里 `status == "rate-limited"` 与 `percent == 100` 恒等价（未用尽时 percent
        // 必然小于 100），取或只是为了容忍字段缺失，不会把"上游说 ok"读成触顶。
        limit_reached: status == Some(RATE_LIMITED) || percent.is_some_and(|value| value >= 100.0),
        local_usage: None,
        provider_data: None,
    })
}

/// 窗口是否仍然触顶：上游标记触顶，且重置时刻尚未到达。
///
/// 比例是观测时刻的快照。上游的 `resetsAt` 是"观测时刻 + 剩余秒数"，落库后仍表示该窗口的
/// 重置时刻；重置时刻过去后窗口已经滚动，不能继续维持限流。
fn window_active(window: &ProviderQuotaWindow, now: SystemTime) -> bool {
    window.limit_reached
        && window
            .reset_at
            .is_none_or(|reset_at| SystemTime::from(reset_at) > now)
}

/// 快照级触顶事实：仍有未重置的窗口处于触顶状态。
pub(crate) fn limit_reached(windows: &[ProviderQuotaWindow], now: SystemTime) -> bool {
    windows.iter().any(|window| window_active(window, now))
}

/// 从窗口推导账号额度访问事实。
///
/// 上游没有"允许"字段，因此只在明确触顶时声明耗尽；其余情况保留未知，不把"仍有剩余
/// 比例"推断成"上游已确认可用"。重置时刻取触顶窗口中最早的一个，即账号最早恢复的时刻。
///
/// 这是保守投影：官方推理链路是"Go 限额超了就抛错，只有账号开了 Zen 余额回退（计费行的
/// `useBalance`）才吞掉该错误、改按余额计费"，所以触顶并不必然意味着请求会被拒绝。而该开关
/// 只存在于计费行，Go 数据面只暴露推理四类路由加 `models` 与 `usage`，没有任何按 API Key
/// 读取它的接口；用量响应也只有 `status` / `percent` / `resetsAt`（见官方 `formatUsage`）。
/// 因此网关无法区分"触顶且会被拒绝"与"触顶但仍可服务"，这里选择按触顶即排除调度，代价是
/// 启用回退的账号在窗口重置前不会被调度；触顶比例与重置时刻仍会展示给管理端。
pub(crate) fn access_state(windows: &[ProviderQuotaWindow], observed_at: SystemTime) -> QuotaState {
    let mut reached = false;
    let mut reset_at = None;
    for window in windows
        .iter()
        .filter(|window| window_active(window, observed_at))
    {
        reached = true;
        if let Some(reset) = window.reset_at {
            let reset = SystemTime::from(reset);
            reset_at = Some(reset_at.map_or(reset, |current: SystemTime| current.min(reset)));
        }
    }
    if reached {
        QuotaState::exhausted(QuotaEvidence::AccountLimitReached, observed_at, reset_at)
    } else {
        QuotaState::observed_unknown(observed_at)
    }
}

/// 查询上游额度并返回 Provider 私有的 usage 文档。
///
/// 只发送额度查询必需的头：官方 CLI 身份（上游边缘会拒绝匿名客户端）与按账号派生的
/// 稳定会话标识。上游拒绝或正文不可解释都映射为 `QuotaError`，不回显上游正文。
pub(crate) async fn fetch_usage(
    endpoint: &str,
    api_key: &str,
    session: &str,
    proxy: Option<&OutboundProxy>,
) -> Result<Map<String, Value>, QuotaError> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT);
    if let Some(proxy) = proxy {
        builder = builder
            .proxy(reqwest::Proxy::all(proxy.expose_url()).map_err(|_| QuotaError::Upstream)?);
    }
    let client = builder.build().map_err(|_| QuotaError::Upstream)?;
    let response = client
        .get(endpoint)
        .header("accept", "application/json")
        .header("user-agent", crate::provider::user_agent())
        .header("x-opencode-client", crate::provider::CLIENT_KIND)
        .header("x-opencode-session", session)
        .bearer_auth(api_key)
        .send()
        .await
        .map_err(|_| QuotaError::Upstream)?;
    let success = response.status().is_success();
    let status = response.status().as_u16();
    // 正文只读一次，成功与失败分支共用。
    let body = response.text().await.unwrap_or_default();
    if !success {
        return Err(rejection(status, &body));
    }
    let value: Value = serde_json::from_str(&body).map_err(|_| QuotaError::Invalid)?;
    usage_document(&value)
}

/// 把非成功响应映射为额度错误；只按上游自述的错误类型分类，不解释正文内容。
fn rejection(status: u16, body: &str) -> QuotaError {
    match status {
        401 => QuotaError::Unauthorized,
        // 403 既可能是"该 Key 没有 Go 订阅"，也可能是边缘节点的拦截页；
        // 只有上游明确给出 EntitlementError 时才当作套餐事实。
        403 if entitlement_denied(body) => QuotaError::NoPlan,
        _ => QuotaError::Upstream,
    }
}

/// 判断 403 正文是否为上游的套餐拒绝。
fn entitlement_denied(body: &str) -> bool {
    serde_json::from_str::<Value>(body)
        .ok()
        .is_some_and(|value| {
            value
                .get("error")
                .and_then(|error| error.get("type"))
                .and_then(Value::as_str)
                == Some(ENTITLEMENT_ERROR)
        })
}
