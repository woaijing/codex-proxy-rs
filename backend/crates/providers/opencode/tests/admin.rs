use crate::support::{Endpoint, Store, USAGE_PATH, canonical_usage, ports, seed};
use gateway_admin::model::provider_credentials::{
    PrepareCredentialImport, ProviderDocument, ProviderQuota, ProviderQuotaRequest,
};
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
async fn dashboard_profile_reports_shipped_cli_identity_without_release_channel() {
    use gateway_admin::model::observability::DashboardWireAttribute;
    let admin = provider_opencode::initialize(ports(&Arc::new(Store::default())))
        .unwrap()
        .admin_provider();
    let profile = admin.dashboard_wire_profile().expect("wire profile");
    assert_eq!(profile.provider, "opencode");
    assert_eq!(profile.product, "OpenCode CLI");
    assert_eq!(profile.version, "1.18.31");
    assert_eq!(
        profile.user_agent,
        "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"
    );
    assert_eq!(
        profile.attributes,
        vec![DashboardWireAttribute {
            label: "客户端标识".to_owned(),
            value: "cli".to_owned(),
        }]
    );
    // 官方 CLI 不声明设备指纹，也没有可对齐的发布渠道。
    assert_eq!(profile.target.os_type, "—");
    assert_eq!(profile.target.terminal, "—");
    assert!(profile.verified_at.is_none());
    assert!(profile.release.is_none());
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

async fn admin_with_endpoint(
    store: &Arc<Store>,
    base: String,
) -> Arc<dyn gateway_admin::ports::provider::ProviderAdmin> {
    provider_opencode::initialize_with_endpoint_policy(ports(store), Arc::new(Endpoint(base)))
        .unwrap()
        .admin_provider()
}

async fn quota_of(
    admin: &Arc<dyn gateway_admin::ports::provider::ProviderAdmin>,
    id: &gateway_core::account::ProviderAccountId,
    refresh: bool,
) -> Result<ProviderQuota, gateway_admin::ports::provider::ProviderAdminError> {
    admin
        .quota(ProviderQuotaRequest {
            account_id: id.clone(),
            refresh,
            rolling_usage: None,
        })
        .await
}

#[tokio::test]
async fn quota_refresh_projects_go_windows_and_persists_provider_document() {
    use gateway_admin::model::provider_credentials::{
        ProviderQuotaWindowRole, QuotaLocalUsageAttribution,
    };
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let store = Arc::new(Store::default());
    let id = seed(&store, "a", "go");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        // 出站身份头是额度查询的一部分：边缘节点会拒绝匿名客户端。
        .and(header(
            "user-agent",
            "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14",
        ))
        .and(header("x-opencode-client", "cli"))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(1)
        .mount(&server)
        .await;

    let admin = admin_with_endpoint(&store, server.uri()).await;
    let quota = quota_of(&admin, &id, true).await.unwrap();

    assert_eq!(quota.plan_type.as_deref(), Some("go"));
    assert!(quota.observed_at.is_some());
    assert!(!quota.limit_reached);
    let windows = quota
        .windows
        .iter()
        .map(|window| {
            (
                window.key.as_str(),
                window.group.as_str(),
                window.label.as_str(),
                window.role,
                window.used_percent,
                window.window_seconds,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        windows,
        vec![
            (
                "rolling",
                "shortTerm",
                "5小时额度",
                Some(ProviderQuotaWindowRole::Primary),
                Some(12.0),
                Some(5 * 60 * 60)
            ),
            (
                "weekly",
                "shortTerm",
                "周额度",
                Some(ProviderQuotaWindowRole::Secondary),
                Some(57.0),
                Some(7 * 24 * 60 * 60)
            ),
            // 月窗口按自然月定义，没有固定秒数：时长未知就不填，避免参与按窗口统计。
            (
                "monthly",
                "monthly",
                "月额度",
                Some(ProviderQuotaWindowRole::Monthly),
                Some(3.0),
                None
            ),
        ]
    );
    // 官方按模型声明限额，上游响应不带模型维度，通用账号级用量无法归属。
    assert!(
        quota
            .windows
            .iter()
            .all(|window| window.local_usage_attribution == QuotaLocalUsageAttribution::Unavailable)
    );
    assert!(quota.windows.iter().all(|window| window.reset_at.is_some()));

    // 上游正文原样持久化，供后续 refresh=false 读取。
    let stored = store.quota(&id).expect("persisted quota");
    assert_eq!(
        stored.quota.into_inner(),
        *canonical_usage()["usage"].as_object().unwrap()
    );

    let requests = server.received_requests().await.expect("requests");
    let session = requests[0]
        .headers
        .get("x-opencode-session")
        .expect("session header")
        .to_str()
        .unwrap();
    // 额度查询不属于任何会话，按账号派生稳定标识；形态与官方 identifier 一致。
    assert!(
        session.starts_with("ses_") && session.len() == 30,
        "{session}"
    );
    assert_eq!(
        requests[0]
            .headers
            .get("authorization")
            .expect("authorization")
            .to_str()
            .unwrap(),
        "Bearer test-key-a"
    );
}

#[tokio::test]
async fn quota_read_returns_persisted_windows_without_querying_upstream_again() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let store = Arc::new(Store::default());
    let id = seed(&store, "a", "go");
    let server = MockServer::start().await;
    // 面板轮询只读缓存：整场只允许一次真实上游请求。
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(1)
        .mount(&server)
        .await;

    let admin = admin_with_endpoint(&store, server.uri()).await;
    let refreshed = quota_of(&admin, &id, true).await.unwrap();
    let cached = quota_of(&admin, &id, false).await.unwrap();

    assert_eq!(cached.windows, refreshed.windows);
    assert_eq!(cached.plan_type.as_deref(), Some("go"));
    // 读取沿用观测时刻，不能把旧快照标成本次读取的时间。
    assert_eq!(cached.observed_at, refreshed.observed_at);
    assert!(cached.observed_at.is_some());
}

#[tokio::test]
async fn quota_read_without_observation_reports_unknown_instead_of_zero_usage() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let store = Arc::new(Store::default());
    let id = seed(&store, "a", "go");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(0)
        .mount(&server)
        .await;

    let admin = admin_with_endpoint(&store, server.uri()).await;
    let quota = quota_of(&admin, &id, false).await.unwrap();

    // 空窗口表示未知；不能用 0% 冒充"没有用量"。
    assert!(quota.windows.is_empty());
    assert!(quota.observed_at.is_none());
    assert!(!quota.limit_reached);
    assert_eq!(quota.plan_type.as_deref(), Some("go"));
}

#[tokio::test]
async fn quota_zen_tier_is_not_queried_because_no_endpoint_exists() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let store = Arc::new(Store::default());
    let id = seed(&store, "a", "zen");
    let server = MockServer::start().await;
    // Zen 同源路径不存在（实测 404），因此连请求都不发。
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(0)
        .mount(&server)
        .await;

    let admin = admin_with_endpoint(&store, server.uri()).await;
    let quota = quota_of(&admin, &id, true).await.unwrap();

    assert!(quota.windows.is_empty());
    assert_eq!(quota.plan_type.as_deref(), Some("zen"));
    assert!(store.quota(&id).is_none());
}

#[tokio::test]
async fn quota_reached_window_exhausts_account_only_until_it_resets() {
    use gateway_core::account::{QuotaAccessState, QuotaEvidence};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // 触顶且重置时刻未到 → 账号耗尽，并在最早重置时刻恢复。
    let store = Arc::new(Store::default());
    let id = seed(&store, "a", "go");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"usage": {
            "rolling": {"status": "rate-limited", "percent": 100, "resetsAt": "2099-01-01T00:00:00Z"}
        }})))
        .mount(&server)
        .await;
    let admin = admin_with_endpoint(&store, server.uri()).await;
    let quota = quota_of(&admin, &id, true).await.unwrap();
    assert!(quota.limit_reached);
    let state = store.quota(&id).expect("persisted quota").state;
    assert_eq!(state.access(), QuotaAccessState::Exhausted);
    assert_eq!(state.evidence(), Some(QuotaEvidence::AccountLimitReached));
    assert!(state.reset_at().is_some());

    // 重置时刻已过 → 窗口已经滚动，不能继续维持限流。
    let store = Arc::new(Store::default());
    let id = seed(&store, "b", "go");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"usage": {
            "rolling": {"status": "rate-limited", "percent": 100, "resetsAt": "2020-01-01T00:00:00Z"}
        }})))
        .mount(&server)
        .await;
    let admin = admin_with_endpoint(&store, server.uri()).await;
    let quota = quota_of(&admin, &id, true).await.unwrap();
    assert!(!quota.limit_reached);
    assert_eq!(
        store.quota(&id).expect("persisted quota").state.access(),
        QuotaAccessState::Unknown
    );
}

#[tokio::test]
async fn quota_failures_map_to_distinct_admin_kinds_without_leaking_upstream_body() {
    use gateway_admin::ports::provider::ProviderAdminErrorKind as Kind;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // 401 拒绝、403 套餐缺失、403 边缘拦截、以及非额度合同的 200 正文。
    let cases = [
        (
            ResponseTemplate::new(401).set_body_json(
                json!({"type":"error","error":{"type":"AuthError","message":"Missing API key."}}),
            ),
            Kind::Invalid,
        ),
        (
            ResponseTemplate::new(403).set_body_json(json!({
                "type": "error",
                "error": {"type": "EntitlementError", "message": "OpenCode Go subscription required."}
            })),
            Kind::Invalid,
        ),
        (
            ResponseTemplate::new(403).set_body_string("<html>blocked</html>"),
            Kind::Unavailable,
        ),
        (
            ResponseTemplate::new(200).set_body_json(json!({"error": "not a usage contract"})),
            Kind::Invalid,
        ),
        (ResponseTemplate::new(503), Kind::Unavailable),
    ];

    for (index, (response, expected)) in cases.into_iter().enumerate() {
        let store = Arc::new(Store::default());
        let id = seed(&store, &format!("case{index}"), "go");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(USAGE_PATH))
            .respond_with(response)
            .mount(&server)
            .await;
        let admin = admin_with_endpoint(&store, server.uri()).await;
        let error = quota_of(&admin, &id, true)
            .await
            .expect_err("quota failure");
        assert_eq!(error.kind(), expected, "case {index}");
        // 上游正文不进入管理错误，只保留明确标记为可公开的静态提示。
        assert!(error.message().is_none(), "case {index}");
        assert!(error.public_message().is_some(), "case {index}");
        // 失败不写回额度观测。
        assert!(store.quota(&id).is_none(), "case {index}");
    }
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
