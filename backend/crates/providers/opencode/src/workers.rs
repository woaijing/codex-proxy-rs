//! OpenCode Provider 向 Host 贡献的后台 worker。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use gateway_admin::model::provider_credentials::ProviderQuotaRequest;
use gateway_admin::ports::provider::ProviderAdmin;
use gateway_core::account::{ProviderAccount, ProviderAccountStore};
use gateway_core::routing::ProviderKind;
use gateway_core::task::{
    ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerKind,
    WorkerLeaseRequest, WorkerRegistration, WorkerRunnable, WorkerSchedule, WorkerTaskError,
};

use crate::OpenCodeInitializeError;

/// 复核周期；与相邻 Provider 的额度 worker 保持同一量级。
const QUOTA_RECHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// 上游没有给出重置时刻时使用的保守复核间隔。
const EXHAUSTED_QUOTA_FALLBACK_RECHECK_INTERVAL: Duration = Duration::from_secs(10 * 60);

const WORKER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const WORKER_MAXIMUM_BACKOFF: Duration = Duration::from_secs(60);
const WORKER_LEASE_TTL: Duration = Duration::from_secs(15 * 60);
const WORKER_LEASE_RENEWAL: Duration = Duration::from_secs(5 * 60);

/// 构造 OpenCode 的后台任务贡献。
///
/// 模型目录随版本发布，没有需要周期性对齐的上游目录，因此这里只登记额度复核。
pub(crate) fn worker_contributions(
    kind: ProviderKind,
    accounts: Arc<dyn ProviderAccountStore>,
    admin: Arc<dyn ProviderAdmin>,
) -> Result<Vec<WorkerContribution>, OpenCodeInitializeError> {
    let id = WorkerId::try_new(WorkerKind::QuotaCatalogHealth, kind.as_str())
        .map_err(|_| OpenCodeInitializeError)?;
    Ok(vec![WorkerContribution::Registration(
        scheduled_registration(
            id,
            QUOTA_RECHECK_INTERVAL,
            Box::new(OpenCodeQuotaRecheckTask {
                kind,
                accounts,
                admin,
            }),
        )?,
    )])
}

fn scheduled_registration(
    id: WorkerId,
    interval: Duration,
    task: Box<dyn ScheduledTask>,
) -> Result<WorkerRegistration, OpenCodeInitializeError> {
    let schedule = WorkerSchedule::try_new(
        interval,
        WORKER_INITIAL_BACKOFF,
        WORKER_MAXIMUM_BACKOFF,
        WORKER_LEASE_TTL,
        WORKER_LEASE_RENEWAL,
    )
    .map_err(|_| OpenCodeInitializeError)?;
    let lease = WorkerLeaseRequest::try_new(id.clone(), WORKER_LEASE_TTL)
        .map_err(|_| OpenCodeInitializeError)?;
    WorkerRegistration::try_new(
        id,
        WorkerRunnable::Scheduled {
            schedule,
            lease: Some(lease),
            task,
        },
    )
    .map_err(|_| OpenCodeInitializeError)
}

struct OpenCodeQuotaRecheckTask {
    kind: ProviderKind,
    accounts: Arc<dyn ProviderAccountStore>,
    admin: Arc<dyn ProviderAdmin>,
}

impl ScheduledTask for OpenCodeQuotaRecheckTask {
    fn run_cycle(&self, context: WorkerCycleContext) -> BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let accounts = self
                .accounts
                .list_for_provider(&self.kind)
                .await
                .map_err(|_| WorkerTaskError::safe("OpenCode Provider accounts unavailable"))?;
            let now = SystemTime::now();
            let mut failures = 0_u64;
            for account in accounts.iter().filter(|account| recheck_due(account, now)) {
                if context.cancellation().is_cancelled() {
                    return Ok(());
                }
                if let Err(error) = self
                    .admin
                    .quota(ProviderQuotaRequest {
                        account_id: account.id().clone(),
                        refresh: true,
                        rolling_usage: None,
                    })
                    .await
                {
                    failures = failures.saturating_add(1);
                    // 复核失败保持既有耗尽结论：账号继续不可调度，不会因为查不到而放行。
                    tracing::warn!(
                        account_id = %account.id().as_str(),
                        quota_error = ?error.kind(),
                        "OpenCode quota recheck failed"
                    );
                }
            }
            if failures == 0 {
                Ok(())
            } else {
                Err(WorkerTaskError::safe("OpenCode quota recheck failed"))
            }
        })
    }
}

/// 判断账号是否需要重新求证额度。
///
/// 未耗尽账号的额度展示由管理端按需刷新，这里不引入周期性上游轮询；但已经写入的
/// 耗尽结论必须复核：`QuotaState::reset_at` 只是"何时重新求证"的提示，时间到期本身
/// 不是额度恢复证据，因此没有复核就没有任何路径能清除 `QuotaExhausted`，账号会被
/// `resolve_account_status` 永久排除出调度。
///
/// OpenCode 使用 API Key，没有 `access_token_expires_at` 事实，因此不能沿用 OAuth
/// Provider 的 token 到期过滤条件。
fn recheck_due(account: &ProviderAccount, now: SystemTime) -> bool {
    account.enabled()
        && account
            .quota()
            .exhaustion_refresh_due(now, EXHAUSTED_QUOTA_FALLBACK_RECHECK_INTERVAL)
}
