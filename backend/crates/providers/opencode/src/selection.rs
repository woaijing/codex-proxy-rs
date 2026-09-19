//! 复用 Core 的账号资格、权重与排队，Redis 冷却期间绝不兜底放行。

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use gateway_core::account::{
    AccountCandidate, AccountEligibilityPolicy, AccountSelectionContext, AccountSelector,
    ProviderAccount, ProviderAccountId,
};
use gateway_core::concurrency::{CapacityWait, ConcurrencyWaitQueue};
use gateway_core::engine::AttemptContext;
use gateway_core::error::{ProviderError, ProviderErrorKind};
use gateway_core::provider_ports::{
    ProviderLeaseAcquisition, ProviderLeaseGuard, ProviderLeaseRequest,
    ProviderSchedulingLeaseRequest, ProviderSessionAffinityKey, ProviderStorePorts,
};
use gateway_core::routing::{ProviderCandidate, ProviderKind};
use gateway_core::upstream::UpstreamSendState;
use serde_json::Value;

use crate::catalog::Catalog;
use crate::credential::{Credential, Tier};

pub(crate) struct Selected {
    pub(crate) account: ProviderAccount,
    pub(crate) credential: Credential,
    pub(crate) affinity_owner: ProviderAccountId,
    pub(crate) _lease: Box<dyn ProviderLeaseGuard>,
}

pub(crate) struct Selector {
    pub(crate) kind: ProviderKind,
    pub(crate) ports: ProviderStorePorts,
    pub(crate) catalog: Arc<Catalog>,
    waiting: ConcurrencyWaitQueue<ProviderAccountId>,
}

impl Selector {
    pub(crate) fn new(
        kind: ProviderKind,
        ports: ProviderStorePorts,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            kind,
            ports,
            catalog,
            waiting: ConcurrencyWaitQueue::default(),
        }
    }

    pub(crate) async fn select(
        &self,
        candidate: &ProviderCandidate,
        context: &AttemptContext,
        affinity: &str,
    ) -> Result<Selected, ProviderError> {
        let cancellation = context.cancellation();
        let duration = context
            .deadline()
            .duration_since(SystemTime::now())
            .map_err(|_| error(ProviderErrorKind::Timeout))?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(error(ProviderErrorKind::Cancelled)),
            result = tokio::time::timeout(duration, self.select_inner(candidate, context, affinity)) => result.map_err(|_| error(ProviderErrorKind::Timeout))?,
        }
    }

    async fn select_inner(
        &self,
        candidate: &ProviderCandidate,
        context: &AttemptContext,
        affinity: &str,
    ) -> Result<Selected, ProviderError> {
        let model = candidate
            .upstream_model()
            .ok_or_else(|| error(ProviderErrorKind::InvalidRequest))?;
        let diagnostic = context.is_diagnostic_required_account();
        let policy = context.account_selection_policy();
        let mut waiting = CapacityWait::new(
            &self.waiting,
            policy.queue_policy(),
            context.deadline(),
            context.concurrency_wait_budget(),
        );
        let affinity_key =
            ProviderSessionAffinityKey::try_new(affinity).map_err(|_| infrastructure())?;
        let preferred = self
            .ports
            .session_affinity()
            .load(&self.kind, &affinity_key)
            .await
            .map_err(|_| infrastructure())?;
        loop {
            let mut accounts = self
                .ports
                .accounts()
                .list_for_provider(&self.kind)
                .await
                .map_err(|_| infrastructure())?;
            if diagnostic
                && let Some(id) = context.required_account()
                && !accounts.iter().any(|account| account.id() == id)
                && let Some(account) = self
                    .ports
                    .accounts()
                    .get_account(id)
                    .await
                    .map_err(|_| infrastructure())?
                && account.provider() == &self.kind
            {
                accounts.push(account);
            }
            accounts.retain(|account| {
                let tier = if account.plan_type() == Some("go") {
                    Tier::Go
                } else {
                    Tier::Zen
                };
                !context.excluded_accounts().contains(account.id())
                    && context
                        .required_account()
                        .is_none_or(|id| id == account.id())
                    && (diagnostic
                        || candidate
                            .account_scope()
                            .allows_model(account.id(), model.as_str()))
                    && self.catalog.find(tier, model.as_str()).is_some()
            });
            let ids = accounts
                .iter()
                .map(|account| account.id().clone())
                .collect::<Vec<_>>();
            let scheduling = self
                .ports
                .leases()
                .load_state(context.client_api_key_ref(), &self.kind, &ids)
                .await
                .map_err(|_| infrastructure())?;
            let cooldown_port = self.ports.cooldowns();
            let cooldowns = futures::future::join_all(
                accounts
                    .iter()
                    .map(|account| cooldown_port.read(account.id())),
            )
            .await;
            let mut retry_after: Option<Duration> = None;
            let mut candidates = Vec::new();
            for (account, cooldown) in accounts.into_iter().zip(cooldowns) {
                // 冷却存储读取失败时关闭放行，不能把未知状态当作已恢复。
                let cooldown = cooldown.map_err(|_| infrastructure())?;
                if !diagnostic
                    && let Some(cooldown) = cooldown
                    && let Ok(remaining) = cooldown.until().duration_since(SystemTime::now())
                    && !remaining.is_zero()
                {
                    retry_after = Some(retry_after.map_or(remaining, |value| value.min(remaining)));
                    continue;
                }
                let health = self
                    .ports
                    .account_feedback()
                    .scheduling_signals(&self.kind, account.id());
                let signals = scheduling
                    .signals()
                    .get(account.id())
                    .cloned()
                    .ok_or_else(infrastructure)?
                    .with_runtime_health(health.0, health.1);
                candidates.push(AccountCandidate { account, signals });
            }
            let mut selection = AccountSelectionContext {
                policy,
                now: SystemTime::now(),
                excluded_accounts: context.excluded_accounts().clone(),
                preferred_account: context
                    .required_account()
                    .cloned()
                    .or_else(|| preferred.clone()),
                preferred_account_overrides_weight: false,
                round_robin_cursor: scheduling.round_robin_cursor(),
                eligibility: if diagnostic {
                    AccountEligibilityPolicy::BypassForDiagnostic
                } else {
                    AccountEligibilityPolicy::Enforce
                },
                account_scope: (!diagnostic).then(|| Arc::clone(candidate.account_scope())),
            };
            let wait_candidates = AccountSelector.wait_candidates(&candidates, &selection);
            for item in &candidates {
                if !waiting.can_try(item.account.id()) {
                    selection
                        .excluded_accounts
                        .insert(item.account.id().clone());
                }
            }
            while let Some(chosen) = AccountSelector.select(&candidates, &selection) {
                let account = &chosen.candidate().account;
                let acquisition = self
                    .ports
                    .leases()
                    .try_acquire(ProviderLeaseRequest::Scheduling(
                        ProviderSchedulingLeaseRequest::new(
                            self.kind.clone(),
                            account.id().clone(),
                            account.revision(),
                            account.effective_concurrency(policy.max_concurrent_per_account()),
                            policy.request_interval(),
                            context.deadline(),
                        ),
                    ))
                    .await
                    .map_err(|_| infrastructure())?;
                match acquisition {
                    ProviderLeaseAcquisition::Busy { retry_after: delay } => {
                        if let Some(delay) = delay {
                            retry_after = Some(retry_after.map_or(delay, |value| value.min(delay)));
                        }
                        selection.excluded_accounts.insert(account.id().clone());
                    }
                    ProviderLeaseAcquisition::Acquired(lease) => {
                        let loaded = self
                            .ports
                            .accounts()
                            .load_credential(account.id(), account.revision())
                            .await
                            .map_err(|_| infrastructure())?;
                        let credential = Credential::parse(Value::Object(
                            loaded.credential.expose_to_provider().clone(),
                        ))
                        .map_err(|_| infrastructure())?;
                        if self.catalog.find(credential.tier, model.as_str()).is_none() {
                            return Err(error(ProviderErrorKind::Unsupported));
                        }
                        let affinity_owner = self
                            .ports
                            .session_affinity()
                            .claim_or_load(
                                &self.kind,
                                &affinity_key,
                                account.id(),
                                Duration::from_secs(24 * 60 * 60),
                            )
                            .await
                            .map_err(|_| infrastructure())?;
                        return Ok(Selected {
                            account: loaded.account,
                            credential,
                            affinity_owner,
                            _lease: lease,
                        });
                    }
                }
            }
            if !diagnostic && policy.queue_policy().max_waiting > 0 && !wait_candidates.is_empty() {
                waiting
                    .wait(&wait_candidates)
                    .await
                    .map_err(|rejection| error(rejection.provider_kind()))?;
                continue;
            }
            let kind = if wait_candidates.is_empty() {
                ProviderErrorKind::NoEligibleAccount
            } else {
                ProviderErrorKind::AccountCapacityUnavailable
            };
            return Err(retry_after
                .map_or_else(|| error(kind), |delay| error(kind).with_retry_after(delay)));
        }
    }
}

pub(crate) fn error(kind: ProviderErrorKind) -> ProviderError {
    ProviderError::new(kind, UpstreamSendState::NotSent)
}
pub(crate) fn infrastructure() -> ProviderError {
    error(ProviderErrorKind::ProviderInfrastructureUnavailable)
}
