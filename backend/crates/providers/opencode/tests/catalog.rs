use crate::support::{Store, ports};
use gateway_core::operation::{CapabilityRequirements, OperationKind};
use std::sync::Arc;

#[tokio::test]
async fn shared_models_respect_the_smaller_product_limits_before_account_selection() {
    let store = Arc::new(Store::default());
    let models = provider_opencode::initialize(ports(&store))
        .unwrap()
        .core_provider()
        .query_model_capabilities()
        .await
        .unwrap();
    let model = models
        .iter()
        .find(|model| model.upstream_model().as_str() == "glm-5.1")
        .unwrap();
    let requirements = CapabilityRequirements::new(OperationKind::Generate)
        .with_requested_output_tokens(Some(32768));
    assert!(
        model
            .capabilities()
            .match_requirements(&requirements)
            .is_some()
    );
    assert!(
        model
            .capabilities()
            .match_requirements(&requirements.with_requested_output_tokens(Some(32769)))
            .is_none()
    );
    assert_eq!(
        model.presentation().unwrap().context_window_tokens(),
        Some(202752)
    );
}
