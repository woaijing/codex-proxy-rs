use crate::support::{Endpoint, Store, USAGE_PATH, canonical_usage, ports, seed, seed_with_quota};
use gateway_core::account::{QuotaAccessState, QuotaEvidence, QuotaState};
use gateway_core::lifecycle::CancellationToken;
use gateway_core::task::{
    WorkerContribution, WorkerCycleContext, WorkerKind, WorkerRunnable, WorkerTaskError,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn bundle_with_endpoint(store: &Arc<Store>, base: String) -> provider_opencode::ProviderBundle {
    provider_opencode::initialize_with_endpoint_policy(ports(store), Arc::new(Endpoint(base)))
        .expect("OpenCode bundle")
}

/// 取出 OpenCode 的额度复核任务并执行一个周期。
async fn run_quota_recheck_cycle(
    bundle: &mut provider_opencode::ProviderBundle,
) -> Result<(), WorkerTaskError> {
    let registration = bundle
        .take_worker_contributions()
        .into_iter()
        .find_map(|contribution| match contribution {
            WorkerContribution::Registration(registration)
                if registration.id.kind() == WorkerKind::QuotaCatalogHealth
                    && registration.id.owner() == "opencode" =>
            {
                Some(registration)
            }
            WorkerContribution::Registration(_) | WorkerContribution::Disabled { .. } => None,
        })
        .expect("OpenCode quota recheck worker");
    let WorkerRunnable::Scheduled { task, .. } = registration.runnable else {
        panic!("OpenCode quota recheck worker must be scheduled");
    };
    task.run_cycle(WorkerCycleContext::new(
        registration.id,
        None,
        CancellationToken::new(),
    ))
    .await
}

/// 已确认耗尽、且上游重置时刻已经过去的账号。
fn exhausted_quota(reset_at: Option<SystemTime>) -> QuotaState {
    QuotaState::exhausted(
        QuotaEvidence::AccountLimitReached,
        SystemTime::now() - Duration::from_secs(60),
        reset_at,
    )
}

#[tokio::test]
async fn bundle_exposes_quota_recheck_worker_and_drains_contributions_once() {
    let mut bundle =
        bundle_with_endpoint(&Arc::new(Store::default()), String::from("http://unused"));

    let contributions = bundle.take_worker_contributions();
    assert_eq!(contributions.len(), 1);
    let WorkerContribution::Registration(registration) = &contributions[0] else {
        panic!("OpenCode must contribute a registration");
    };
    assert_eq!(registration.id.kind(), WorkerKind::QuotaCatalogHealth);
    assert_eq!(registration.id.owner(), "opencode");
    assert!(matches!(
        &registration.runnable,
        WorkerRunnable::Scheduled { .. }
    ));

    // 贡献只能交付一次，避免 Host 重复登记同一个 worker。
    assert!(bundle.take_worker_contributions().is_empty());
}

#[tokio::test]
async fn quota_recheck_clears_exhaustion_once_the_window_reset_has_passed() {
    let store = Arc::new(Store::default());
    let id = seed_with_quota(
        &store,
        "go_reset",
        "go",
        exhausted_quota(Some(SystemTime::now() - Duration::from_secs(30))),
    );
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(1)
        .mount(&server)
        .await;

    let mut bundle = bundle_with_endpoint(&store, server.uri());
    run_quota_recheck_cycle(&mut bundle)
        .await
        .expect("recheck cycle");

    // 复核后上游不再声明触顶，账号必须重新可调度，否则会永久停在 QuotaExhausted。
    assert_eq!(store.account_quota(&id).access(), QuotaAccessState::Unknown);
    assert_eq!(
        store.quota(&id).expect("persisted quota").state.access(),
        QuotaAccessState::Unknown
    );
}

#[tokio::test]
async fn quota_recheck_skips_accounts_that_are_not_exhausted() {
    let store = Arc::new(Store::default());
    seed(&store, "healthy", "go");
    let server = MockServer::start().await;
    // 未耗尽账号的额度展示由管理端按需刷新，worker 不引入周期性上游轮询。
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(0)
        .mount(&server)
        .await;

    let mut bundle = bundle_with_endpoint(&store, server.uri());
    run_quota_recheck_cycle(&mut bundle)
        .await
        .expect("recheck cycle");
}

#[tokio::test]
async fn quota_recheck_waits_until_the_declared_reset_moment() {
    let store = Arc::new(Store::default());
    seed_with_quota(
        &store,
        "go_waiting",
        "go",
        exhausted_quota(Some(SystemTime::now() + Duration::from_secs(3600))),
    );
    let server = MockServer::start().await;
    // 上游给出了明确重置时刻，未到之前不必求证。
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(0)
        .mount(&server)
        .await;

    let mut bundle = bundle_with_endpoint(&store, server.uri());
    run_quota_recheck_cycle(&mut bundle)
        .await
        .expect("recheck cycle");
}

#[tokio::test]
async fn quota_recheck_keeps_exhaustion_when_upstream_is_unreachable() {
    let store = Arc::new(Store::default());
    let id = seed_with_quota(
        &store,
        "go_down",
        "go",
        exhausted_quota(Some(SystemTime::now() - Duration::from_secs(30))),
    );
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;

    let mut bundle = bundle_with_endpoint(&store, server.uri());
    let error = run_quota_recheck_cycle(&mut bundle)
        .await
        .expect_err("recheck failure must surface to Host health");

    // 查不到额度不等于额度已恢复：保留耗尽结论，账号继续不参与调度。
    assert_eq!(error.as_safe_str(), "OpenCode quota recheck failed");
    assert_eq!(
        store.account_quota(&id).access(),
        QuotaAccessState::Exhausted
    );
    assert!(store.quota(&id).is_none());
}

#[tokio::test]
async fn quota_recheck_reports_account_listing_failure_without_querying_upstream() {
    let store = Arc::new(Store::default());
    seed_with_quota(&store, "go_unlisted", "go", exhausted_quota(None));
    store.fail_listing.store(true, Ordering::SeqCst);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(USAGE_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(canonical_usage()))
        .expect(0)
        .mount(&server)
        .await;

    let mut bundle = bundle_with_endpoint(&store, server.uri());
    let error = run_quota_recheck_cycle(&mut bundle)
        .await
        .expect_err("listing failure must surface to Host health");

    assert_eq!(
        error.as_safe_str(),
        "OpenCode Provider accounts unavailable"
    );
}
