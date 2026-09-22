//! Provider 只准备凭据事实，管理用例统一提交审计、配置与凭据事务。

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_admin::model::accounts::CredentialState;
use gateway_admin::model::observability::{
    CalculatedBillingBreakdown, DashboardWireAttribute, DashboardWireProfile, DashboardWireTarget,
    ProviderBillingInput,
};
use gateway_admin::model::provider_credentials::*;
use gateway_admin::ports::provider::{ProviderAdmin, ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::account::{
    LoadedCredential, OpaqueProviderData, ProviderAccountId, ProviderAccountStore,
    QuotaObservation, QuotaState, QuotaWriteOutcome,
};
use gateway_core::operation::{GenerateRequest, Operation, ProtocolPayload};
use gateway_core::routing::{ProviderKind, UpstreamModelId};
use serde_json::{Map, Value, json};

use crate::catalog::Catalog;
use crate::credential::{Credential, Tier, invalid};
use crate::provider::OpenCodeEndpointPolicy;
use crate::{identity, quota};

pub(crate) struct OpenCodeAdmin {
    kind: ProviderKind,
    accounts: Arc<dyn ProviderAccountStore>,
    catalog: Arc<Catalog>,
    endpoints: Arc<dyn OpenCodeEndpointPolicy>,
}

impl OpenCodeAdmin {
    pub(crate) fn new(
        kind: ProviderKind,
        accounts: Arc<dyn ProviderAccountStore>,
        catalog: Arc<Catalog>,
        endpoints: Arc<dyn OpenCodeEndpointPolicy>,
    ) -> Self {
        Self {
            kind,
            accounts,
            catalog,
            endpoints,
        }
    }

    async fn loaded(&self, id: &ProviderAccountId) -> Result<LoadedCredential, ProviderAdminError> {
        let loaded = self
            .accounts
            .load_current_credential(id)
            .await
            .map_err(|_| ProviderAdminError::new(ProviderAdminErrorKind::Unavailable))?;
        if loaded.account.provider() != &self.kind {
            return Err(ProviderAdminError::new(ProviderAdminErrorKind::NotFound));
        }
        Ok(loaded)
    }

    async fn credential(&self, id: &ProviderAccountId) -> Result<Credential, ProviderAdminError> {
        Credential::parse(Value::Object(
            self.loaded(id)
                .await?
                .credential
                .expose_to_provider()
                .clone(),
        ))
    }

    /// 查询上游 Go 套餐额度；端点策略与数据面共用，测试可注入本地端点。
    async fn refresh_usage(
        &self,
        loaded: &LoadedCredential,
        credential: &Credential,
    ) -> Result<Map<String, Value>, ProviderAdminError> {
        let endpoint = self
            .endpoints
            .endpoint(credential.tier.as_str(), quota::USAGE_PATH);
        let session = identity::quota_session(loaded.account.id().as_str());
        quota::fetch_usage(
            &endpoint,
            &credential.api_key,
            &session,
            loaded.account.outbound_proxy(),
        )
        .await
        .map_err(map_quota_error)
    }

    /// 以凭据 revision 为界写回额度观测；并发轮换造成的冲突不覆盖新凭据的观测。
    async fn store_usage(
        &self,
        loaded: &LoadedCredential,
        document: Map<String, Value>,
        observed_at: DateTime<Utc>,
        state: QuotaState,
    ) -> Result<(), ProviderAdminError> {
        let outcome = self
            .accounts
            .compare_and_swap_quota(QuotaObservation {
                account_id: loaded.account.id().clone(),
                expected_revision: loaded.account.revision(),
                quota: OpaqueProviderData::new(document),
                plan_type: None,
                observed_at: observed_at.into(),
                state,
            })
            .await
            .map_err(|_| ProviderAdminError::new(ProviderAdminErrorKind::Unavailable))?;
        match outcome {
            QuotaWriteOutcome::Updated => Ok(()),
            QuotaWriteOutcome::Conflict => {
                Err(ProviderAdminError::new(ProviderAdminErrorKind::Conflict))
            }
        }
    }

    /// 读取已持久化的额度观测；没有观测时返回 `None`。
    async fn stored_usage(
        &self,
        id: &ProviderAccountId,
    ) -> Result<Option<QuotaObservation>, ProviderAdminError> {
        Ok(self
            .accounts
            .get_quotas(std::slice::from_ref(id))
            .await
            .map_err(|_| ProviderAdminError::new(ProviderAdminErrorKind::Unavailable))?
            .pop())
    }
}

#[async_trait]
impl ProviderAdmin for OpenCodeAdmin {
    fn provider_kind(&self) -> &ProviderKind {
        &self.kind
    }
    async fn account_unavailable(&self, _: &ProviderAccountId) {}
    fn dashboard_wire_profile(&self) -> Option<DashboardWireProfile> {
        Some(DashboardWireProfile {
            provider: self.kind.as_str().to_owned(),
            product: "OpenCode CLI".to_owned(),
            version: crate::provider::CLIENT_VERSION.to_owned(),
            build: None,
            // 官方 CLI 只在身份头里声明版本与客户端类型，不声明系统、架构或终端；
            // 这些是协议兼容身份而非设备指纹，因此不推断运行环境，统一保留未知标记。
            target: DashboardWireTarget {
                os_type: "—".to_owned(),
                os_version: "—".to_owned(),
                arch: "—".to_owned(),
                terminal: "—".to_owned(),
            },
            user_agent: crate::provider::user_agent(),
            attributes: vec![DashboardWireAttribute {
                label: "客户端标识".to_owned(),
                value: crate::provider::CLIENT_KIND.to_owned(),
            }],
            // 身份是随版本发布的固定常量，没有可核验的运行时快照，也不做发布渠道对齐检查。
            verified_at: None,
            release: None,
        })
    }
    fn calculated_billing(
        &self,
        _: &ProviderBillingInput,
    ) -> Result<Option<CalculatedBillingBreakdown>, ProviderAdminError> {
        Ok(None)
    }

    fn connection_test_operation(
        &self,
        model: &UpstreamModelId,
        input: &str,
    ) -> Result<Operation, ProviderAdminError> {
        let body = json!({"model": model.as_str(), "input": input, "stream": true, "max_output_tokens": 128})
            .as_object().cloned().ok_or_else(invalid)?;
        Ok(Operation::Generate(GenerateRequest::from_protocol_payload(
            ProtocolPayload::json_object("openai", body).map_err(|_| invalid())?,
        )))
    }

    async fn prepare_import(
        &self,
        command: PrepareCredentialImport,
    ) -> Result<PreparedCredentialImport, ProviderAdminError> {
        let document = command.document.expose_to_provider().expose_to_provider();
        let entries = document
            .get("accounts")
            .and_then(Value::as_array)
            .ok_or_else(invalid)?;
        if entries.is_empty() || entries.len() > 1000 {
            return Err(invalid());
        }
        let mut credentials = Vec::with_capacity(entries.len());
        for entry in entries {
            let mut material = entry.as_object().cloned().ok_or_else(invalid)?;
            let name = material
                .remove("name")
                .and_then(|name| name.as_str().map(str::to_owned))
                .filter(|name| !name.trim().is_empty() && name.len() <= 256)
                .ok_or_else(invalid)?;
            let credential = Credential::parse(Value::Object(material))?;
            credentials.push(PreparedCredentialCreate {
                model_access: None,
                outbound_proxy: command.default_outbound_proxy.clone(),
                account_id: ProviderAccountId::new(format!("acct_{}", uuid::Uuid::new_v4()))
                    .map_err(|_| invalid())?,
                provider_kind: self.kind.clone(),
                name,
                email: None,
                upstream_user_id: None,
                upstream_account_id: None,
                plan_type: Some(credential.tier.as_str().to_owned()),
                authentication_kind: "api_key".to_owned(),
                provider_material: credential.document(),
                has_refresh_token: false,
                access_token_expires_at: None,
                next_refresh_at: None,
                enabled: true,
                credential_state: CredentialState::Ready,
                credential_observed_at: Utc::now(),
            });
        }
        Ok(PreparedCredentialImport {
            provider_kind: self.kind.clone(),
            credentials,
        })
    }

    async fn prepare_rotation(
        &self,
        command: PrepareCredentialRotation,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        if command.account.provider_kind != self.kind
            || command.account.authentication_kind != "api_key"
        {
            return Err(invalid());
        }
        let id = ProviderAccountId::new(command.account.id).map_err(|_| invalid())?;
        let current = self.credential(&id).await?;
        let mut material = command
            .provider_material
            .expose_to_provider()
            .expose_to_provider()
            .clone();
        if material
            .get("api_key")
            .is_none_or(|value| value.as_str().is_some_and(str::is_empty))
        {
            material.insert("api_key".to_owned(), Value::String(current.api_key));
        }
        material
            .entry("tier")
            .or_insert_with(|| Value::String(current.tier.as_str().to_owned()));
        let credential = Credential::parse(Value::Object(material))?;
        Ok(PreparedCredentialRotation::new(
            PreparedCredentialRotationFacts {
                account_id: id,
                provider_kind: self.kind.clone(),
                expected_credential_revision: command.account.credential_revision,
                replacement_identity: None,
                name: command.account.name,
                email: None,
                plan_type: Some(credential.tier.as_str().to_owned()),
                preserve_profile: false,
                provider_material: credential.document(),
                has_refresh_token: false,
                access_token_expires_at: None,
                next_refresh_at: None,
            },
            Box::new(KeyRotationGuard),
        ))
    }

    async fn account_configuration(
        &self,
        id: &ProviderAccountId,
    ) -> Result<Option<ProviderDocument>, ProviderAdminError> {
        let credential = self.credential(id).await?;
        Ok(Some(ProviderDocument::new(
            gateway_core::account::OpaqueProviderData::new(serde_json::Map::from_iter([(
                "tier".to_owned(),
                Value::String(credential.tier.as_str().to_owned()),
            )])),
        )))
    }

    async fn models(
        &self,
        id: &ProviderAccountId,
        _: bool,
    ) -> Result<ProviderModels, ProviderAdminError> {
        let credential = self.credential(id).await?;
        let models = self
            .catalog
            .models
            .iter()
            .filter(|model| model.tier == credential.tier)
            .map(|model| {
                Ok(ProviderModel {
                    id: UpstreamModelId::new(model.id.clone()).map_err(|_| invalid())?,
                    name: model.name.clone(),
                })
            })
            .collect::<Result<_, ProviderAdminError>>()?;
        Ok(ProviderModels {
            models,
            observed_at: None,
        })
    }

    async fn export_credentials(
        &self,
        credentials: Vec<ProviderExportCredentialInput>,
    ) -> Result<ProviderExport, ProviderAdminError> {
        let mut account_ids = Vec::with_capacity(credentials.len());
        let mut entries = Vec::with_capacity(credentials.len());
        for entry in credentials {
            if entry.account.provider_kind != self.kind {
                return Err(invalid());
            }
            account_ids.push(ProviderAccountId::new(entry.account.id).map_err(|_| invalid())?);
            let credential = Credential::parse(Value::Object(
                entry
                    .provider_material
                    .expose_to_provider()
                    .expose_to_provider()
                    .clone(),
            ))?;
            entries.push(json!({"name": entry.account.name, "api_key": credential.api_key, "tier": credential.tier}));
        }
        Ok(ProviderExport {
            provider_kind: self.kind.clone(),
            account_ids,
            document: ProviderDocument::new(gateway_core::account::OpaqueProviderData::new(
                serde_json::Map::from_iter([
                    ("provider".to_owned(), Value::String("opencode".to_owned())),
                    ("accounts".to_owned(), Value::Array(entries)),
                ]),
            )),
        })
    }

    async fn quota(
        &self,
        request: ProviderQuotaRequest,
    ) -> Result<ProviderQuota, ProviderAdminError> {
        let loaded = self.loaded(&request.account_id).await?;
        let credential = Credential::parse(Value::Object(
            loaded.credential.expose_to_provider().clone(),
        ))?;
        // 只有 Go 套餐存在可验证的额度合同；Zen 没有对应端点（同源路径 404），
        // 空窗口表示未知，不能伪造剩余额度。
        if credential.tier != Tier::Go {
            return Ok(empty_quota(credential.tier));
        }
        let (document, observed_at) = if request.refresh {
            let document = self.refresh_usage(&loaded, &credential).await?;
            let observed_at = Utc::now();
            let windows = quota::interpret(&document).map_err(map_quota_error)?;
            let state = quota::access_state(&windows, observed_at.into());
            self.store_usage(&loaded, document.clone(), observed_at, state)
                .await?;
            (document, observed_at)
        } else {
            // 未要求刷新时只读已持久化的观测，避免面板轮询反复打上游。
            let Some(observation) = self.stored_usage(&request.account_id).await? else {
                return Ok(empty_quota(credential.tier));
            };
            // 展示的是观测时刻而不是本次读取时刻，否则旧快照会被显示成刚刚刷新。
            let observed_at = DateTime::<Utc>::from(observation.observed_at);
            (observation.quota.into_inner(), observed_at)
        };
        let windows = quota::interpret(&document).map_err(map_quota_error)?;
        Ok(ProviderQuota {
            plan_type: Some(credential.tier.as_str().to_owned()),
            observed_at: Some(observed_at),
            refresh_token_expires_at: None,
            // 触顶是快照事实，但要按当前时间判断窗口是否已经滚动过去。
            limit_reached: quota::limit_reached(&windows, SystemTime::now()),
            windows,
            provider_data: None,
        })
    }
    async fn start_authorization(
        &self,
        _: PendingAuthorizationMutation,
    ) -> Result<AuthorizationStarted, ProviderAdminError> {
        Err(unsupported())
    }
    async fn complete_authorization(
        &self,
        _: CompleteAuthorization,
    ) -> Result<PreparedAuthorizationCommit, ProviderAdminError> {
        Err(unsupported())
    }
    async fn prepare_refresh(
        &self,
        _: PrepareCredentialRefresh,
    ) -> Result<PreparedCredentialRotation, ProviderAdminError> {
        Err(unsupported())
    }
}

fn unsupported() -> ProviderAdminError {
    ProviderAdminError::new(ProviderAdminErrorKind::Unsupported)
}

/// 没有可查询的额度合同时的空投影；空窗口表示未知，不能伪造剩余额度。
fn empty_quota(tier: Tier) -> ProviderQuota {
    ProviderQuota {
        plan_type: Some(tier.as_str().to_owned()),
        observed_at: None,
        refresh_token_expires_at: None,
        windows: Vec::new(),
        limit_reached: false,
        provider_data: None,
    }
}

/// 额度失败只映射为可公开的静态提示；上游正文与凭据不进入管理错误。
fn map_quota_error(error: quota::QuotaError) -> ProviderAdminError {
    use ProviderAdminErrorKind as Kind;
    use quota::QuotaError as Error;
    let (kind, message) = match error {
        Error::Unauthorized => (Kind::Invalid, "OpenCode 额度查询凭据无效，请检查账号授权"),
        Error::NoPlan => (Kind::Invalid, "该 Key 没有 OpenCode Go 订阅，无法查询额度"),
        Error::Invalid => (Kind::Invalid, "OpenCode 额度数据无效，请检查账号授权"),
        Error::Upstream => (
            Kind::Unavailable,
            "OpenCode 额度查询失败，请检查出站连接与上游服务",
        ),
    };
    ProviderAdminError::new(kind).with_public_message(message)
}

struct KeyRotationGuard;
impl CredentialCommitGuard for KeyRotationGuard {
    fn finish(self: Box<Self>) {}
}
