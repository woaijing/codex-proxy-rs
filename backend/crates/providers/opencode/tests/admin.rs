use crate::support::{Store, ports, seed};
use gateway_admin::model::provider_credentials::{PrepareCredentialImport, ProviderDocument};
use gateway_core::account::OpaqueProviderData;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn rotation_preserves_omitted_key_and_export_can_be_imported_again() {
    use gateway_admin::model::accounts::AccountRecord;
    use gateway_admin::model::provider_credentials::{
        PrepareCredentialRotation, ProviderExportCredentialInput,
    };
    use gateway_core::account::{AccountModelAccess, AccountWeight, CredentialState, QuotaState};
    let store = Arc::new(Store::default());
    seed(&store, "a", "zen");
    let admin = provider_opencode::initialize(ports(&store))
        .unwrap()
        .admin_provider();
    let now = chrono::Utc::now();
    let record = AccountRecord {
        id: "acct_a".into(),
        provider_kind: gateway_core::routing::ProviderKind::new("opencode").unwrap(),
        groups: vec![],
        name: "test account".into(),
        notes: None,
        email: None,
        upstream_user_id: None,
        upstream_account_id: None,
        plan_type: Some("zen".into()),
        authentication_kind: "api_key".into(),
        credential_revision: gateway_admin::model::Revision::new(1).unwrap(),
        has_refresh_token: false,
        access_token_expires_at: None,
        next_refresh_at: None,
        enabled: true,
        concurrency_limit: None,
        weight: AccountWeight::default(),
        model_access: AccountModelAccess::default(),
        outbound_proxy: None,
        credential_state: CredentialState::Ready,
        credential_observed_at: now,
        quota: QuotaState::unknown(),
        last_error_reason: None,
        last_error_message: None,
        created_at: now,
        updated_at: now,
    };
    let rotated = admin
        .prepare_rotation(PrepareCredentialRotation {
            account: record.clone(),
            provider_material: document(json!({"tier":"go"})),
        })
        .await
        .unwrap();
    assert_eq!(
        rotated.facts().expected_credential_revision,
        record.credential_revision
    );
    assert_eq!(
        rotated
            .facts()
            .provider_material
            .expose_to_provider()
            .expose_to_provider()["api_key"],
        "test-key-a"
    );
    assert_eq!(rotated.facts().plan_type.as_deref(), Some("go"));
    let exported = admin
        .export_credentials(vec![ProviderExportCredentialInput {
            account: record.clone(),
            provider_material: rotated.facts().provider_material.clone(),
        }])
        .await
        .unwrap();
    let imported = admin
        .prepare_import(PrepareCredentialImport {
            document: exported.document,
            default_outbound_proxy: None,
        })
        .await
        .unwrap();
    assert_eq!(imported.credentials[0].plan_type.as_deref(), Some("go"));
    assert_eq!(
        imported.credentials[0].provider_material,
        rotated.facts().provider_material
    );
    let mut wrong_provider = record;
    wrong_provider.provider_kind = gateway_core::routing::ProviderKind::new("openai").unwrap();
    assert!(
        admin
            .prepare_rotation(PrepareCredentialRotation {
                account: wrong_provider,
                provider_material: document(json!({"api_key":"new-key"}))
            })
            .await
            .is_err()
    );
}

fn document(value: serde_json::Value) -> ProviderDocument {
    ProviderDocument::new(OpaqueProviderData::new(value.as_object().unwrap().clone()))
}

#[tokio::test]
async fn key_import_preserves_product_and_public_configuration_hides_secret() {
    let store = Arc::new(Store::default());
    let admin = provider_opencode::initialize(ports(&store))
        .unwrap()
        .admin_provider();
    let prepared = admin
        .prepare_import(PrepareCredentialImport {
            default_outbound_proxy: None,
            document: ProviderDocument::new(OpaqueProviderData::new(
                json!({"accounts":[{"name":"Go key","tier":"go","api_key":"test-secret"}]})
                    .as_object()
                    .unwrap()
                    .clone(),
            )),
        })
        .await
        .unwrap();
    assert_eq!(prepared.credentials[0].authentication_kind, "api_key");
    assert_eq!(prepared.credentials[0].plan_type.as_deref(), Some("go"));
    assert!(!format!("{prepared:?}").contains("test-secret"));
    let id = seed(&store, "a", "go");
    let configuration = admin.account_configuration(&id).await.unwrap().unwrap();
    assert_eq!(
        configuration.expose_to_provider().expose_to_provider(),
        json!({"tier":"go"}).as_object().unwrap()
    );
}

#[tokio::test]
async fn import_rejects_header_injection_and_unknown_product() {
    let admin = provider_opencode::initialize(ports(&Arc::new(Store::default())))
        .unwrap()
        .admin_provider();
    for material in [
        json!({"name":"test","tier":"go","api_key":"key\r\nx-inject: test"}),
        json!({"name":"test","tier":"unknown","api_key":"key"}),
    ] {
        assert!(
            admin
                .prepare_import(PrepareCredentialImport {
                    default_outbound_proxy: None,
                    document: ProviderDocument::new(OpaqueProviderData::new(
                        json!({"accounts":[material]}).as_object().unwrap().clone()
                    ))
                })
                .await
                .is_err()
        );
    }
}
