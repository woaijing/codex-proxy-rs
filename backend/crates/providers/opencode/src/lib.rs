//! OpenCode Zen / Go API Key 的数据面与管理面边界。

mod admin;
mod catalog;
mod credential;
mod identity;
mod provider;
mod request;
mod response;
mod selection;
mod stream;

use std::sync::Arc;

use gateway_admin::ports::provider::ProviderAdmin;
use gateway_core::engine::provider::Provider;
use gateway_core::provider_ports::ProviderStorePorts;
use gateway_core::routing::ProviderKind;

pub use provider::{OpenCodeEndpointPolicy, OpenCodeProvider};

/// 组装根持有的 OpenCode 能力。
pub struct ProviderBundle {
    provider: Arc<OpenCodeProvider>,
    admin: Arc<admin::OpenCodeAdmin>,
}

/// 使用现有账号、租约、代理与冷却端口初始化。
pub fn initialize(ports: ProviderStorePorts) -> Result<ProviderBundle, OpenCodeInitializeError> {
    initialize_with_endpoint_policy(ports, Arc::new(provider::OfficialEndpoints))
}

/// 注入受信任的端点策略，供嵌入宿主及独立协议验收使用。
pub fn initialize_with_endpoint_policy(
    ports: ProviderStorePorts,
    endpoints: Arc<dyn OpenCodeEndpointPolicy>,
) -> Result<ProviderBundle, OpenCodeInitializeError> {
    let kind = ProviderKind::new("opencode").map_err(|_| OpenCodeInitializeError)?;
    let catalog = Arc::new(catalog::Catalog::bundled().map_err(|_| OpenCodeInitializeError)?);
    let provider = Arc::new(OpenCodeProvider::new(
        kind.clone(),
        ports.clone(),
        Arc::clone(&catalog),
        endpoints,
    ));
    let admin = Arc::new(admin::OpenCodeAdmin::new(kind, ports.accounts(), catalog));
    Ok(ProviderBundle { provider, admin })
}

impl ProviderBundle {
    #[must_use]
    pub fn core_provider(&self) -> Arc<dyn Provider> {
        self.provider.clone()
    }

    #[must_use]
    pub fn admin_provider(&self) -> Arc<dyn ProviderAdmin> {
        self.admin.clone()
    }
}

/// 初始化失败不包含凭据或请求内容。
#[derive(Debug, thiserror::Error)]
#[error("OpenCode provider initialization failed")]
pub struct OpenCodeInitializeError;
