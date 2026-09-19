//! 官方目录快照按产品保留协议，不能由模型名称猜测上游端点。

use gateway_core::operation::{Feature, OperationKind};
use gateway_core::routing::{
    ModelCapabilities, ModelPresentation, ProviderModelCapabilities, SupportLevel, UpstreamModelId,
};
use serde::Deserialize;

use crate::credential::Tier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Protocol {
    Responses,
    Chat,
    Messages,
}

impl Protocol {
    pub(crate) const fn path(self) -> &'static str {
        match self {
            Self::Responses => "responses",
            Self::Chat => "chat/completions",
            Self::Messages => "messages",
        }
    }
}

#[derive(Clone, Deserialize)]
pub(crate) struct Model {
    pub(crate) tier: Tier,
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) protocol: Protocol,
    pub(crate) tools: bool,
    pub(crate) vision: bool,
    pub(crate) reasoning: bool,
    pub(crate) context: u64,
    pub(crate) output: u64,
}

pub(crate) struct Catalog {
    pub(crate) models: Vec<Model>,
}

impl Catalog {
    pub(crate) fn bundled() -> Result<Self, serde_json::Error> {
        Ok(Self {
            models: serde_json::from_str(include_str!("../assets/models.json"))?,
        })
    }

    pub(crate) fn find(&self, tier: Tier, id: &str) -> Option<&Model> {
        self.models
            .iter()
            .find(|model| model.tier == tier && model.id == id)
    }

    pub(crate) fn capabilities(&self) -> Vec<ProviderModelCapabilities> {
        let mut common = std::collections::BTreeMap::<&str, Model>::new();
        for model in &self.models {
            // 路由目录尚未选择产品，同名模型只能承诺各产品共有的能力和上限。
            common
                .entry(&model.id)
                .and_modify(|shared| {
                    shared.context = shared.context.min(model.context);
                    shared.output = shared.output.min(model.output);
                    shared.tools &= model.tools;
                    shared.vision &= model.vision;
                    shared.reasoning &= model.reasoning;
                })
                .or_insert_with(|| model.clone());
        }
        let mut models = Vec::with_capacity(common.len());
        for model in common.into_values() {
            let Ok(id) = UpstreamModelId::new(model.id.clone()) else {
                continue;
            };
            let support = |enabled| {
                if enabled {
                    SupportLevel::Native
                } else {
                    SupportLevel::Unsupported
                }
            };
            let capabilities =
                ModelCapabilities::new([OperationKind::Generate].into(), Some(model.output))
                    .with_feature(Feature::Tools, support(model.tools))
                    .with_feature(Feature::Vision, support(model.vision))
                    .with_feature(Feature::Reasoning, support(model.reasoning));
            models.push(
                ProviderModelCapabilities::new(id, capabilities).with_presentation(
                    ModelPresentation::new(Some(model.name), None)
                        .with_context_window_tokens(Some(model.context))
                        .with_image_input(model.vision)
                        .with_agent_tools(model.tools, model.tools),
                ),
            );
        }
        models
    }
}
