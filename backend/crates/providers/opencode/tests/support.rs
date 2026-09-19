use async_trait::async_trait;
use futures::future::BoxFuture;
use gateway_core::account::*;
use gateway_core::engine::provider::ProviderRequest;
use gateway_core::engine::{
    AccountAttemptContext, AttemptContext, ModelRequestId, RequestAttemptContext,
};
use gateway_core::error::{StoreError, StoreErrorKind};
use gateway_core::lifecycle::CancellationToken;
use gateway_core::operation::{GenerateRequest, Operation, OperationKind, ProtocolPayload};
use gateway_core::policy::ClientApiKeyId;
use gateway_core::provider_ports::*;
use gateway_core::routing::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[derive(Default)]
pub struct Store {
    pub accounts: Mutex<BTreeMap<ProviderAccountId, LoadedCredential>>,
    pub cooldowns: Mutex<BTreeMap<ProviderAccountId, ProviderCooldown>>,
    pub starts: AtomicUsize,
    pub active: Arc<AtomicUsize>,
    pub fail_cooldown: AtomicBool,
    pub affinity: Mutex<BTreeMap<String, ProviderAccountId>>,
}

pub fn ports(store: &Arc<Store>) -> ProviderStorePorts {
    ProviderStorePorts::new(
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
    )
}

#[async_trait]
impl ProviderAccountStore for Store {
    async fn create_account(&self, _account: NewProviderAccount) -> Result<(), StoreError> {
        panic!("unexpected account operation")
    }
    async fn get_account(
        &self,
        _account: &ProviderAccountId,
    ) -> Result<Option<ProviderAccount>, StoreError> {
        Ok(self
            .accounts
            .lock()
            .unwrap()
            .get(_account)
            .map(|loaded| loaded.account.clone()))
    }
    async fn list_accounts(&self) -> Result<Vec<ProviderAccount>, StoreError> {
        Ok(self
            .accounts
            .lock()
            .unwrap()
            .values()
            .map(|loaded| loaded.account.clone())
            .collect())
    }
    async fn list_for_provider(
        &self,
        _provider: &ProviderKind,
    ) -> Result<Vec<ProviderAccount>, StoreError> {
        Ok(self
            .accounts
            .lock()
            .unwrap()
            .values()
            .filter(|loaded| loaded.account.provider() == _provider && loaded.account.enabled())
            .map(|loaded| loaded.account.clone())
            .collect())
    }
    async fn list_refresh_candidates(
        &self,
        _query: ProviderRefreshQuery,
    ) -> Result<Vec<LoadedCredential>, StoreError> {
        panic!("unexpected account operation")
    }
    async fn load_credential(
        &self,
        _account: &ProviderAccountId,
        _expected_revision: CredentialRevision,
    ) -> Result<LoadedCredential, StoreError> {
        let loaded = self.load_current_credential(_account).await?;
        assert_eq!(loaded.account.revision(), _expected_revision);
        Ok(loaded)
    }
    async fn load_current_credential(
        &self,
        _account: &ProviderAccountId,
    ) -> Result<LoadedCredential, StoreError> {
        self.accounts
            .lock()
            .unwrap()
            .get(_account)
            .cloned()
            .ok_or_else(|| StoreError::new(StoreErrorKind::Unavailable))
    }
    async fn compare_and_swap_credential(
        &self,
        _update: CredentialCasUpdate,
    ) -> Result<CredentialCasOutcome, StoreError> {
        panic!("unexpected account operation")
    }
    async fn get_quotas(
        &self,
        _accounts: &[ProviderAccountId],
    ) -> Result<Vec<QuotaObservation>, StoreError> {
        panic!("unexpected account operation")
    }
    async fn compare_and_swap_quota(
        &self,
        _observation: QuotaObservation,
    ) -> Result<QuotaWriteOutcome, StoreError> {
        panic!("unexpected account operation")
    }
    async fn touch_quota_observation(
        &self,
        _touch: QuotaObservationTouch,
    ) -> Result<QuotaWriteOutcome, StoreError> {
        panic!("unexpected account operation")
    }
    async fn apply_quota_access(
        &self,
        _change: QuotaAccessChange,
    ) -> Result<QuotaWriteOutcome, StoreError> {
        panic!("unexpected account operation")
    }
    async fn apply_state_change(&self, _change: AccountStateChange) -> Result<(), StoreError> {
        panic!("unexpected account operation")
    }
    async fn update_account(&self, _update: ProviderAccountUpdate) -> Result<(), StoreError> {
        panic!("unexpected account operation")
    }
    async fn set_enabled(
        &self,
        _account: &ProviderAccountId,
        _enabled: bool,
    ) -> Result<(), StoreError> {
        panic!("unexpected account operation")
    }
    async fn delete_account(&self, _account: &ProviderAccountId) -> Result<(), StoreError> {
        panic!("unexpected account operation")
    }
}

impl ProviderLeasePort for Store {
    fn load_state<'a>(
        &'a self,
        _client_api_key_id: &'a ClientApiKeyId,
        _provider_kind: &'a ProviderKind,
        _accounts: &'a [ProviderAccountId],
    ) -> BoxFuture<'a, Result<ProviderSchedulingState, ProviderStoreError>> {
        Box::pin(async move {
            Ok(ProviderSchedulingState::new(
                _accounts
                    .iter()
                    .map(|id| {
                        (
                            id.clone(),
                            AccountRuntimeSignals {
                                in_flight: 0,
                                last_started_at: None,
                                quota_reset_at: None,
                                quota_remaining_rank: None,
                                cooldown: None,
                                failure_rate_basis_points: None,
                                first_output_latency_ms: None,
                            },
                        )
                    })
                    .collect(),
                0,
            ))
        })
    }
    fn try_acquire(
        &self,
        _request: ProviderLeaseRequest,
    ) -> BoxFuture<'_, Result<ProviderLeaseAcquisition, ProviderStoreError>> {
        Box::pin(async move {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            Ok(ProviderLeaseAcquisition::Acquired(Box::new(Lease(
                Arc::clone(&self.active),
            ))))
        })
    }
}

impl ProviderSessionAffinityPort for Store {
    fn load<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderAccountId>, ProviderStoreError>> {
        Box::pin(async move {
            Ok(self
                .affinity
                .lock()
                .unwrap()
                .get(_key.expose_to_store())
                .cloned())
        })
    }
    fn bind<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
        _account_id: &'a ProviderAccountId,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn claim_or_load<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
        _candidate_account_id: &'a ProviderAccountId,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        Box::pin(async move {
            Ok(self
                .affinity
                .lock()
                .unwrap()
                .entry(_key.expose_to_store().to_owned())
                .or_insert_with(|| _candidate_account_id.clone())
                .clone())
        })
    }
    fn compare_and_bind<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
        _expected_account_id: &'a ProviderAccountId,
        _replacement_account_id: &'a ProviderAccountId,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<ProviderAccountId, ProviderStoreError>> {
        Box::pin(async move {
            let mut entries = self.affinity.lock().unwrap();
            let old = entries
                .entry(_key.expose_to_store().to_owned())
                .or_insert_with(|| _replacement_account_id.clone());
            if old == _expected_account_id {
                *old = _replacement_account_id.clone();
            }
            Ok(old.clone())
        })
    }
    fn clear<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

impl ProviderSessionExclusionPort for Store {
    fn load<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderSessionExclusions>, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn record_failure<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
        _account_id: &'a ProviderAccountId,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<ProviderSessionExclusions, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn clear<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _key: &'a ProviderSessionAffinityKey,
        _expected_revision: &'a str,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

impl ProviderCatalogCachePort for Store {
    fn replace<'a>(
        &'a self,
        _key: &'a ProviderCatalogCacheKey,
        _catalog: &'a OpaqueProviderData,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn read<'a>(
        &'a self,
        _key: &'a ProviderCatalogCacheKey,
    ) -> BoxFuture<'a, Result<Option<OpaqueProviderData>, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

impl ProviderArtifactProfileCachePort for Store {
    fn replace_if_newer(
        &self,
        _profile: ProviderArtifactProfile,
        _ttl: Duration,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn read<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _artifact_key: &'a str,
    ) -> BoxFuture<'a, Result<Option<ProviderArtifactProfile>, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

impl ProviderCredentialStatePort for Store {
    fn replace(
        &self,
        _state: ProviderCredentialState,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn read<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<ProviderCredentialState>, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn clear<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn record_refresh_backoff<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _window: Duration,
    ) -> BoxFuture<'a, Result<u32, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn clear_refresh_backoff<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

impl ProviderCooldownPort for Store {
    fn put_if_later(
        &self,
        _cooldown: ProviderCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let mut entries = self.cooldowns.lock().unwrap();
            if entries
                .get(_cooldown.account_id())
                .is_none_or(|old| old.until() < _cooldown.until())
            {
                entries.insert(_cooldown.account_id().clone(), _cooldown);
                return Ok(true);
            }
            Ok(false)
        })
    }
    fn read<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<ProviderCooldown>, ProviderStoreError>> {
        Box::pin(async move {
            if self.fail_cooldown.load(Ordering::SeqCst) {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Unavailable,
                    "test",
                ));
            }
            Ok(self.cooldowns.lock().unwrap().get(_account_id).cloned())
        })
    }
    fn clear<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn put_scoped_if_later(
        &self,
        _cooldown: ProviderScopedCooldown,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn read_scoped<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _scope: &'a ProviderCooldownScope,
    ) -> BoxFuture<'a, Result<Option<ProviderScopedCooldown>, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn clear_scoped<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _scope: &'a ProviderCooldownScope,
        _through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn clear_all<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn record_capacity_failure<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _window: Duration,
        _in_flight: u32,
    ) -> BoxFuture<'a, Result<u32, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn clear_after_success<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
        _through_revision: CredentialRevision,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn capacity_peak_in_flight<'a>(
        &'a self,
        _account_id: &'a ProviderAccountId,
    ) -> BoxFuture<'a, Result<Option<u32>, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

impl ProviderRuntimePolicyPort for Store {
    fn load_refresh_policy(
        &self,
    ) -> BoxFuture<'_, Result<ProviderRefreshPolicy, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

impl OAuthPendingFlowPort for Store {
    fn put_if_absent(
        &self,
        _flow: NewOAuthPendingFlow,
    ) -> BoxFuture<'_, Result<OAuthPendingPutOutcome, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn claim_if_owner<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _flow: &'a OAuthPendingBinding,
        _owner: &'a OAuthPendingBinding,
        _claim: &'a OAuthPendingBinding,
        _claim_ttl: Duration,
    ) -> BoxFuture<'a, Result<OAuthPendingClaimOutcome, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn release_claim<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _flow: &'a OAuthPendingBinding,
        _owner: &'a OAuthPendingBinding,
        _claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingReleaseOutcome, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
    fn consume_claim<'a>(
        &'a self,
        _provider_kind: &'a ProviderKind,
        _flow: &'a OAuthPendingBinding,
        _owner: &'a OAuthPendingBinding,
        _claim: &'a OAuthPendingBinding,
    ) -> BoxFuture<'a, Result<OAuthPendingConsumeOutcome, ProviderStoreError>> {
        Box::pin(async { panic!("unexpected port operation") })
    }
}

struct Lease(Arc<AtomicUsize>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct Endpoint(pub String);
impl provider_opencode::OpenCodeEndpointPolicy for Endpoint {
    fn endpoint(&self, product: &str, path: &str) -> String {
        format!("{}/{product}/{path}", self.0)
    }
}

pub fn seed(store: &Store, suffix: &str, tier: &str) -> ProviderAccountId {
    let id = ProviderAccountId::new(format!("acct_{suffix}")).unwrap();
    let account = ProviderAccount::new(
        id.clone(),
        ProviderKind::new("opencode").unwrap(),
        suffix.to_owned(),
        None,
        "api_key".to_owned(),
        CredentialRevision::new(1).unwrap(),
        None,
    )
    .with_profile(None, None, Some(tier.to_owned()))
    .with_account_facts(
        true,
        CredentialState::Ready,
        QuotaState::unknown(),
        None,
        None,
    );
    let credential = PlaintextCredential::new(
        json!({"api_key":format!("test-key-{suffix}"),"tier":tier})
            .as_object()
            .unwrap()
            .clone(),
    );
    store.accounts.lock().unwrap().insert(
        id.clone(),
        LoadedCredential {
            account,
            credential,
        },
    );
    id
}

pub fn policy() -> AccountSelectionPolicy {
    AccountSelectionPolicy::new(
        RotationStrategy::RoundRobin,
        NonZeroU32::new(2).unwrap(),
        Duration::ZERO,
    )
}
pub fn context() -> AttemptContext {
    named_context("req_test", "key_test")
}
pub fn named_context(request_id: &str, client_key: &str) -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new(request_id).unwrap(),
            ClientApiKeyId::new(client_key).unwrap(),
        ),
        NonZeroU32::MIN,
        SystemTime::now() + Duration::from_secs(20),
        policy(),
        AccountAttemptContext::new(BTreeSet::new(), None, None),
        None,
        CancellationToken::new(),
    )
}
pub fn request(store: &Store, model: &str, body: serde_json::Value) -> ProviderRequest {
    request_with_context(store, model, body, serde_json::Map::new())
}
pub fn request_with_context(
    store: &Store,
    model: &str,
    body: serde_json::Value,
    context: serde_json::Map<String, serde_json::Value>,
) -> ProviderRequest {
    let kind = ProviderKind::new("opencode").unwrap();
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().unwrap().clone())
            .unwrap()
            .with_context(context),
    ));
    let scope = Arc::new(FrozenAccountScope::new(
        Arc::new(RuntimeAccountDirectory::new(
            store
                .accounts
                .lock()
                .unwrap()
                .keys()
                .map(|id| {
                    (
                        id.clone(),
                        RuntimeAccount::new(kind.clone(), BTreeSet::new()),
                    )
                })
                .collect(),
        )),
        ClientRoutingScope::all_accounts(),
    ));
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        policy(),
        vec![kind.clone()],
        vec![ProviderModel::new(
            kind,
            UpstreamModelId::new(model).unwrap(),
            ModelCapabilities::new([OperationKind::Generate].into(), None)
                .with_upstream_feature_validation(),
        )],
        vec![],
    )
    .unwrap();
    let plan = snapshot
        .plan(
            &PublicModelId::new(model).unwrap(),
            &operation,
            scope,
            &RoutingContext::default(),
        )
        .unwrap();
    ProviderRequest::new(operation, plan.candidates()[0].clone())
}
