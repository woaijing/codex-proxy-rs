//! Provider 只准备凭据事实，管理用例统一提交审计、配置与凭据事务。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use gateway_admin::model::accounts::CredentialState;
use gateway_admin::model::observability::{
    CalculatedBillingBreakdown, DashboardWireProfile, ProviderBillingInput,
};
use gateway_admin::model::provider_credentials::*;
use gateway_admin::ports::provider::{ProviderAdmin, ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::account::{ProviderAccountId, ProviderAccountStore};
use gateway_core::operation::{GenerateRequest, Operation, ProtocolPayload};
use gateway_core::routing::{ProviderKind, UpstreamModelId};
use serde_json::{Value, json};

use crate::catalog::Catalog;
use crate::credential::{Credential, invalid};

pub(crate) struct OpenCodeAdmin {
    kind: ProviderKind,
    accounts: Arc<dyn ProviderAccountStore>,
    catalog: Arc<Catalog>,
}

impl OpenCodeAdmin {
    pub(crate) fn new(
        kind: ProviderKind,
        accounts: Arc<dyn ProviderAccountStore>,
        catalog: Arc<Catalog>,
    ) -> Self {
        Self {
            kind,
            accounts,
            catalog,
        }
    }

    async fn credential(&self, id: &ProviderAccountId) -> Result<Credential, ProviderAdminError> {
        let loaded = self
            .accounts
            .load_current_credential(id)
            .await
            .map_err(|_| ProviderAdminError::new(ProviderAdminErrorKind::Unavailable))?;
        if loaded.account.provider() != &self.kind {
            return Err(ProviderAdminError::new(ProviderAdminErrorKind::NotFound));
        }
        Credential::parse(Value::Object(
            loaded.credential.expose_to_provider().clone(),
        ))
    }
}

#[async_trait]
impl ProviderAdmin for OpenCodeAdmin {
    fn provider_kind(&self) -> &ProviderKind {
        &self.kind
    }
    async fn account_unavailable(&self, _: &ProviderAccountId) {}
    fn dashboard_wire_profile(&self) -> Option<DashboardWireProfile> {
        None
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
        let credential = self.credential(&request.account_id).await?;
        // 官方没有可验证的 Key 额度查询合同，空窗口表示未知，不能伪造剩余额度。
        Ok(ProviderQuota {
            plan_type: Some(credential.tier.as_str().to_owned()),
            observed_at: None,
            refresh_token_expires_at: None,
            windows: Vec::new(),
            limit_reached: false,
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

struct KeyRotationGuard;
impl CredentialCommitGuard for KeyRotationGuard {
    fn finish(self: Box<Self>) {}
}
