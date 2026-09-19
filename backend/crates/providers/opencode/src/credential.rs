//! 仅解析 Provider 私有材料；持久化与事务由 Store 负责。

use gateway_admin::model::provider_credentials::ProviderDocument;
use gateway_admin::ports::provider::{ProviderAdminError, ProviderAdminErrorKind};
use gateway_core::account::OpaqueProviderData;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Tier {
    #[default]
    Zen,
    Go,
}

impl Tier {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Zen => "zen",
            Self::Go => "go",
        }
    }

    pub(crate) const fn base_url(self) -> &'static str {
        match self {
            Self::Zen => "https://opencode.ai/zen/v1",
            Self::Go => "https://opencode.ai/zen/go/v1",
        }
    }
}

// 不派生 Debug，防止认证材料被错误上下文或日志输出。
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Credential {
    pub(crate) api_key: String,
    #[serde(default)]
    pub(crate) tier: Tier,
}

impl Credential {
    pub(crate) fn parse(value: Value) -> Result<Self, ProviderAdminError> {
        let credential: Self = serde_json::from_value(value).map_err(|_| invalid())?;
        if credential.api_key.trim() != credential.api_key
            || credential.api_key.is_empty()
            || credential.api_key.len() > 4096
            || !credential
                .api_key
                .bytes()
                .all(|byte| byte.is_ascii_graphic())
        {
            return Err(invalid());
        }
        Ok(credential)
    }

    pub(crate) fn document(&self) -> ProviderDocument {
        ProviderDocument::new(OpaqueProviderData::new(serde_json::Map::from_iter([
            ("api_key".to_owned(), Value::String(self.api_key.clone())),
            (
                "tier".to_owned(),
                Value::String(self.tier.as_str().to_owned()),
            ),
        ])))
    }
}

pub(crate) fn invalid() -> ProviderAdminError {
    ProviderAdminError::new(ProviderAdminErrorKind::Invalid)
}
