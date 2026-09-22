//! OpenCode Zen / Go API Key 的数据面与管理面边界。

mod admin;
mod catalog;
mod credential;
mod identity;
mod provider;
mod quota;
mod request;
mod response;
mod selection;
mod stream;
mod workers;

use std::sync::Arc;

use gateway_admin::ports::provider::ProviderAdmin;
use gateway_core::engine::provider::Provider;
use gateway_core::provider_ports::ProviderStorePorts;
use gateway_core::routing::ProviderKind;
use gateway_core::task::WorkerContribution;

pub use provider::{OpenCodeEndpointPolicy, OpenCodeProvider};

/// 组装根持有的 OpenCode 能力。
pub struct ProviderBundle {
    provider: Arc<OpenCodeProvider>,
    admin: Arc<admin::OpenCodeAdmin>,
    worker_contributions: Vec<WorkerContribution>,
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
        Arc::clone(&endpoints),
    ));
    // 管理面的额度查询与数据面共用端点策略，测试可注入本地端点而不打真实上游。
    let admin = Arc::new(admin::OpenCodeAdmin::new(
        kind.clone(),
        ports.accounts(),
        catalog,
        endpoints,
    ));
    // 额度复核与数据面共用同一套账号端口，注册表按 Provider 名区分 owner。
    let worker_contributions = workers::worker_contributions(
        kind,
        ports.accounts(),
        Arc::clone(&admin) as Arc<dyn ProviderAdmin>,
    )?;
    Ok(ProviderBundle {
        provider,
        admin,
        worker_contributions,
    })
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

    /// 取出后台任务贡献；只能调用一次，与其它 Bundle 的贡献一并交给 Host。
    pub fn take_worker_contributions(&mut self) -> Vec<WorkerContribution> {
        std::mem::take(&mut self.worker_contributions)
    }
}

/// 初始化失败不包含凭据或请求内容。
#[derive(Debug, thiserror::Error)]
#[error("OpenCode provider initialization failed")]
pub struct OpenCodeInitializeError;
