use crate::support::{Endpoint, Store, context, ports, request, seed};
use futures::StreamExt as _;
use gateway_core::error::ProviderErrorKind;
use gateway_core::event::GatewayEvent;
use gateway_core::provider_ports::ProviderCooldown;
use serde_json::json;
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, SystemTime};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn identity_scopes_projects_clients_and_parent_sessions_with_stable_message_ids() {
    use crate::support::{named_context, request_with_context};
    use base64::Engine as _;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(CHAT, "text/event-stream"))
        .mount(&server)
        .await;
    let store = Arc::new(Store::default());
    seed(&store, "a", "zen");
    let provider = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint(server.uri())),
    )
    .unwrap()
    .core_provider();
    for (client, project, session, parent, message) in [
        ("key_a", "project_a", "root", "", "msg_1"),
        ("key_a", "project_a", "child", "root", "msg_2"),
        ("key_a", "project_a", "root", "", "msg_3"),
        ("key_b", "project_a", "root", "", "msg_1"),
        ("key_a", "project_b", "root", "", "msg_1"),
        ("key_a", "project_a", "root", "", ""),
    ] {
        let headers = [
            ("x-opencode-project", project),
            ("x-opencode-session", session),
            ("x-parent-session-id", parent),
        ]
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| json!([key, base64::engine::general_purpose::STANDARD.encode(value)]))
        .collect::<Vec<_>>();
        let payload =
            json!({"model":"big-pickle", "input":[{"role":"user","id":message,"content":"hi"}]});
        let events = provider
            .execute(
                request_with_context(
                    &store,
                    "big-pickle",
                    payload,
                    json!({"opaque_request_headers":headers})
                        .as_object()
                        .unwrap()
                        .clone(),
                ),
                named_context(&format!("req_{message}"), client),
            )
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(events.iter().all(Result::is_ok), "{events:?}");
    }
    let requests = server.received_requests().await.unwrap();
    let header = |index: usize, name: &str| requests[index].headers[name].to_str().unwrap();
    assert_eq!(
        header(0, "x-opencode-session"),
        header(1, "x-parent-session-id")
    );
    assert_eq!(
        header(0, "x-opencode-session"),
        header(2, "x-opencode-session")
    );
    assert_ne!(
        header(0, "x-opencode-session"),
        header(3, "x-opencode-session")
    );
    assert_ne!(
        header(0, "x-opencode-session"),
        header(4, "x-opencode-session")
    );
    assert_eq!(header(1, "x-opencode-request"), "msg_2");
    assert!(!requests[0].headers.contains_key("x-parent-session-id"));
    assert_eq!(
        header(0, "user-agent"),
        "opencode/1.18.31 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"
    );
    // 官方 identifier 为前缀加 12 位十六进制再加 14 位 base62，共 26 个字符。
    let shape = |value: &str, prefix: &str| {
        let rest = value
            .strip_prefix(prefix)
            .unwrap_or_else(|| panic!("{value}"));
        assert_eq!(rest.len(), 26, "{value}");
        assert!(
            rest[..12].bytes().all(|byte| byte.is_ascii_hexdigit())
                && rest[12..].bytes().all(|byte| byte.is_ascii_alphanumeric()),
            "{value}"
        );
    };
    shape(header(0, "x-opencode-session"), "ses_");
    shape(header(1, "x-parent-session-id"), "ses_");
    // 客户端未提供请求关联时按网关请求 ID 稳定派生，仍保持官方形态。
    shape(header(5, "x-opencode-request"), "msg_");
}

#[tokio::test]
async fn successful_fallback_updates_affinity_before_completed_is_delivered() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(CHAT, "text/event-stream"))
        .mount(&server)
        .await;
    let store = Arc::new(Store::default());
    let first = seed(&store, "a", "zen");
    let provider = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint(server.uri())),
    )
    .unwrap()
    .core_provider();
    let make = || {
        request(
            &store,
            "big-pickle",
            json!({"input":"hi","prompt_cache_key":"conversation"}),
        )
    };
    drop(provider.execute(make(), context()).await.unwrap());
    let second = seed(&store, "b", "zen");
    store.cooldowns.lock().unwrap().insert(
        first.clone(),
        ProviderCooldown::new(
            first,
            gateway_core::account::CredentialRevision::new(1).unwrap(),
            SystemTime::now() + Duration::from_secs(60),
        ),
    );
    let mut stream = provider.execute(make(), context()).await.unwrap();
    while let Some(event) = stream.next().await {
        if event
            .unwrap()
            .canonical_facts()
            .iter()
            .any(|fact| matches!(fact, GatewayEvent::Completed(_)))
        {
            break;
        }
    }
    assert_eq!(
        store.affinity.lock().unwrap().values().next(),
        Some(&second)
    );
    drop(stream);
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn account_proxy_carries_request_and_cancellation_before_poll_never_sends() {
    let proxy = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(CHAT, "text/event-stream"))
        .mount(&proxy)
        .await;
    let store = Arc::new(Store::default());
    let id = seed(&store, "a", "zen");
    {
        let mut accounts = store.accounts.lock().unwrap();
        let loaded = accounts.get_mut(&id).unwrap();
        loaded.account = loaded.account.clone().with_outbound_proxy(Some(
            gateway_core::account::OutboundProxy::parse(&proxy.uri()).unwrap(),
        ));
    }
    let provider = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint("http://opencode-test.invalid".into())),
    )
    .unwrap()
    .core_provider();
    let make = || request(&store, "big-pickle", json!({"input":"hi"}));
    let events = provider
        .execute(make(), context())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let response = downstream_response(&events);
    assert_eq!(response["output"][0]["content"][0]["text"], "hello");
    assert_eq!(proxy.received_requests().await.unwrap().len(), 1);
    let cancelled = context();
    let token = cancelled.cancellation().clone();
    let mut stream = provider.execute(make(), cancelled).await.unwrap();
    token.cancel();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().kind(),
        ProviderErrorKind::Cancelled
    );
    drop(stream);
    assert_eq!(proxy.received_requests().await.unwrap().len(), 1);
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_responses_preserve_wire_and_chat_rejects_unrepresentable_continuation() {
    let server = MockServer::start().await;
    let body=[json!({"type":"response.created","response":{"id":"resp_native","model":"gpt-5.4","status":"in_progress"}}),json!({"type":"response.completed","response":{"id":"resp_native","model":"gpt-5.4","status":"completed","output":[],"usage":{"input_tokens":4,"output_tokens":2,"total_tokens":6}}})]
        .iter().map(|event|format!("data: {event}\n\n")).collect::<String>();
    Mock::given(path("/zen/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
        .mount(&server)
        .await;
    let store = Arc::new(Store::default());
    seed(&store, "a", "zen");
    let provider = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint(server.uri())),
    )
    .unwrap()
    .core_provider();
    let events = provider
        .execute(request(&store, "gpt-5.4", json!({"input":"hi"})), context())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(
        events
            .iter()
            .filter_map(|event| event.as_ref().ok())
            .flat_map(|event| event.canonical_facts())
            .any(|event| matches!(event,GatewayEvent::Usage(usage) if usage.total_tokens==Some(6)))
    );
    let result = provider
        .execute(
            request(
                &store,
                "big-pickle",
                json!({"input":"hi","previous_response_id":"resp_old"}),
            ),
            context(),
        )
        .await;
    assert!(matches!(result,Err(error) if error.kind()==ProviderErrorKind::Unsupported));
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

const CHAT: &str = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":2,\"total_tokens\":10}}\n\ndata: [DONE]\n\n";

#[tokio::test]
async fn cold_stream_sends_identity_and_releases_lease_after_completion() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/zen/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(CHAT, "text/event-stream"))
        .mount(&server)
        .await;
    let store = Arc::new(Store::default());
    seed(&store, "a", "zen");
    let bundle = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint(server.uri())),
    )
    .unwrap();
    let provider = bundle.core_provider();
    let input = json!({"model":"big-pickle","input":"hi","metadata":{"x-opencode-session":"child","x-parent-session-id":"root","x-opencode-project":"project"}});
    let stream = provider
        .execute(request(&store, "big-pickle", input), context())
        .await
        .unwrap();
    assert!(server.received_requests().await.unwrap().is_empty());
    assert_eq!(store.active.load(Ordering::SeqCst), 1);
    let events = stream.collect::<Vec<_>>().await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(
        events
            .iter()
            .filter_map(|event| event.as_ref().ok())
            .flat_map(|event| event.canonical_facts())
            .any(|event| matches!(event,GatewayEvent::TextDelta(delta) if delta.text=="hello"))
    );
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
    let received = server.received_requests().await.unwrap();
    let headers = &received[0].headers;
    for name in [
        "x-opencode-session",
        "x-opencode-request",
        "x-opencode-project",
        "x-parent-session-id",
        "x-opencode-client",
        "user-agent",
    ] {
        assert!(headers.contains_key(name), "{name}");
    }
    assert_eq!(headers["authorization"], "Bearer test-key-a");
    assert!(!headers.contains_key("x-api-key"));
    assert!(!headers.contains_key("x-session-affinity"));
    assert_eq!(
        received[0].body_json::<serde_json::Value>().unwrap()["messages"][0]["content"][0]["text"],
        "hi"
    );
}

#[tokio::test]
async fn retry_after_cools_key_and_all_cooling_never_acquires_another_lease() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "3600"))
        .mount(&server)
        .await;
    let store = Arc::new(Store::default());
    let id = seed(&store, "a", "zen");
    let provider = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint(server.uri())),
    )
    .unwrap()
    .core_provider();
    let make = || {
        request(
            &store,
            "big-pickle",
            json!({"model":"big-pickle","input":"hi"}),
        )
    };
    let mut stream = provider.execute(make(), context()).await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind(), ProviderErrorKind::RateLimited);
    drop(stream);
    assert!(
        store.cooldowns.lock().unwrap()[&id]
            .until()
            .duration_since(SystemTime::now())
            .unwrap()
            > Duration::from_secs(3590)
    );
    let result = provider.execute(make(), context()).await;
    assert!(
        matches!(result,Err(error) if error.kind()==ProviderErrorKind::NoEligibleAccount && error.retry_after().is_some())
    );
    assert_eq!(store.starts.load(Ordering::SeqCst), 1);
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn cooling_account_is_skipped_and_cooldown_storage_failure_is_closed() {
    let store = Arc::new(Store::default());
    let first = seed(&store, "a", "zen");
    let second = seed(&store, "b", "zen");
    store.cooldowns.lock().unwrap().insert(
        first.clone(),
        ProviderCooldown::new(
            first,
            gateway_core::account::CredentialRevision::new(1).unwrap(),
            SystemTime::now() + Duration::from_secs(3600),
        ),
    );
    let provider = provider_opencode::initialize(ports(&store))
        .unwrap()
        .core_provider();
    let make = || {
        request(
            &store,
            "big-pickle",
            json!({"model":"big-pickle","input":"hi"}),
        )
    };
    let stream = provider.execute(make(), context()).await.unwrap();
    assert_eq!(stream.metadata().provider_account_id(), &second);
    drop(stream);
    store.fail_cooldown.store(true, Ordering::SeqCst);
    assert!(
        matches!(provider.execute(make(),context()).await,Err(error) if error.kind()==ProviderErrorKind::ProviderInfrastructureUnavailable)
    );
    assert_eq!(store.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn messages_tool_call_and_usage_are_translated_on_go_specific_route() {
    let server = MockServer::start().await;
    let events=[
        json!({"type":"message_start","message":{"id":"msg_upstream","model":"minimax-m2.7","usage":{"input_tokens":10,"cache_read_input_tokens":3}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_1","name":"weather","input":{}}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Paris\"}"}}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}),
        json!({"type":"message_stop"}),
    ].iter().map(|value|format!("data: {value}\n\n")).collect::<String>();
    Mock::given(path("/go/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(events, "text/event-stream"))
        .mount(&server)
        .await;
    let store = Arc::new(Store::default());
    seed(&store, "a", "go");
    let provider = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint(server.uri())),
    )
    .unwrap()
    .core_provider();
    let body = json!({"model":"minimax-m2.7","input":"weather?","tools":[{"type":"function","name":"weather","parameters":{"type":"object"}}]});
    let events = provider
        .execute(request(&store, "minimax-m2.7", body), context())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let response = downstream_response(&events);
    assert_eq!(response["output"][0]["type"], "function_call");
    assert_eq!(response["output"][0]["name"], "weather");
    assert_eq!(response["output"][0]["arguments"], "{\"city\":\"Paris\"}");
    assert_eq!(response["usage"]["total_tokens"], 20);
    let facts = events
        .iter()
        .filter_map(|event| event.as_ref().ok())
        .flat_map(|event| event.canonical_facts())
        .collect::<Vec<_>>();
    assert!(facts.iter().any(|event|matches!(event,GatewayEvent::ToolCallDelta(delta) if delta.arguments_delta.contains("Paris"))));
    assert!(facts.iter().any(|event|matches!(event,GatewayEvent::Usage(usage) if usage.input_tokens==Some(13) && usage.output_tokens==Some(7))));
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests[0].headers["x-api-key"], "test-key-a");
    assert!(!requests[0].headers.contains_key("authorization"));
    assert_eq!(
        requests[0].body_json::<serde_json::Value>().unwrap()["tools"][0]["input_schema"]["type"],
        "object"
    );
}

fn downstream_response(
    events: &[Result<gateway_core::event::ProviderEvent, gateway_core::error::ProviderError>],
) -> serde_json::Value {
    let mut encoder = gateway_api::openai::responses::OpenAiResponsesEncoder::new();
    let mut sse = String::new();
    for event in events {
        for frame in encoder.push_sse(event.as_ref().unwrap()) {
            sse.push_str(std::str::from_utf8(&frame).unwrap());
        }
    }
    assert!(sse.contains("response.output_item.added"));
    assert!(sse.contains("response.output_item.done"));
    assert!(encoder.is_completed());
    encoder.finish().unwrap()
}

#[tokio::test]
async fn parallel_tool_history_and_reasoning_length_limit_reach_responses_client() {
    let server = MockServer::start().await;
    let body=[json!({"choices":[{"index":0,"delta":{"reasoning_content":"thinking"}}]}), json!({"choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":"length"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}})]
        .iter().map(|event|format!("data: {event}\n\n")).collect::<String>()+"data: [DONE]\n\n";
    Mock::given(path("/zen/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
        .mount(&server)
        .await;
    let store = Arc::new(Store::default());
    seed(&store, "a", "zen");
    let provider = provider_opencode::initialize_with_endpoint_policy(
        ports(&store),
        Arc::new(Endpoint(server.uri())),
    )
    .unwrap()
    .core_provider();
    let history = json!({"input":[
        {"role":"user","content":"parallel tools"},
        {"type":"function_call","call_id":"call_1","name":"one","arguments":"{}"},
        {"type":"function_call","call_id":"call_2","name":"two","arguments":"{}"},
        {"type":"function_call_output","call_id":"call_1","output":"first"},
        {"type":"function_call_output","call_id":"call_2","output":"second"}
    ]});
    let events = provider
        .execute(request(&store, "minimax-m2.7", history), context())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    let response = downstream_response(&events);
    assert_eq!(response["status"], "incomplete");
    assert_eq!(
        response["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    assert_eq!(response["output"][0]["summary"][0]["text"], "thinking");
    assert_eq!(response["output"][1]["content"][0]["text"], "partial");
    let sent = server.received_requests().await.unwrap();
    let body = sent[0].body_json::<serde_json::Value>().unwrap();
    assert_eq!(
        body["messages"][1]["tool_calls"].as_array().unwrap().len(),
        2
    );
    assert_eq!(body["messages"][2]["tool_call_id"], "call_1");
    assert_eq!(body["messages"][3]["tool_call_id"], "call_2");
}

#[tokio::test]
async fn truncated_stream_and_done_without_finish_are_errors() {
    for body in [
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"}}]}\n\ndata: [DONE]\n\n",
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .mount(&server)
            .await;
        let store = Arc::new(Store::default());
        seed(&store, "a", "zen");
        let provider = provider_opencode::initialize_with_endpoint_policy(
            ports(&store),
            Arc::new(Endpoint(server.uri())),
        )
        .unwrap()
        .core_provider();
        let events = provider
            .execute(
                request(
                    &store,
                    "big-pickle",
                    json!({"model":"big-pickle","input":"hi"}),
                ),
                context(),
            )
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(
            matches!(events.last(),Some(Err(error)) if error.kind()==ProviderErrorKind::Protocol)
        );
        assert!(
            !events
                .iter()
                .filter_map(|event| event.as_ref().ok())
                .flat_map(|event| event.canonical_facts())
                .any(|event| matches!(event, GatewayEvent::Completed(_)))
        );
    }
}
