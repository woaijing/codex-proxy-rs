//! 三种上游 SSE 协议的增量转换；只有明确终态才能完成响应。

use std::collections::BTreeMap;

use gateway_core::error::{ProviderError, ProviderErrorKind};
use gateway_core::event::{
    ContentItem, ContentKind, FinishReason, GatewayEvent, ProtocolWireEvent, ProviderEvent,
    ReasoningDelta, ResponseMeta, TextDelta, ToolCallDelta,
};
use gateway_core::metering::Usage;
use gateway_core::upstream::UpstreamSendState;
use gateway_protocol::openai::events::extract_usage;
use serde_json::Value;

use crate::catalog::Protocol;

pub(crate) struct Decoder {
    protocol: Protocol,
    meta: ResponseMeta,
    started: bool,
    pub(crate) terminal: bool,
    finish: FinishReason,
    finish_seen: bool,
    content: BTreeMap<String, u32>,
    tools: BTreeMap<u64, (String, String)>,
    usage: Usage,
    writer: crate::response::ResponseWriter,
}

impl Decoder {
    pub(crate) fn new(protocol: Protocol, model: &str) -> Self {
        Self {
            protocol,
            meta: ResponseMeta::new(format!("resp_{}", uuid::Uuid::new_v4().simple()), model),
            started: false,
            terminal: false,
            finish: FinishReason::Stop,
            finish_seen: false,
            content: BTreeMap::new(),
            tools: BTreeMap::new(),
            usage: Usage::new(),
            writer: crate::response::ResponseWriter::default(),
        }
    }

    pub(crate) fn decode(
        &mut self,
        event: gateway_protocol::openai::sse::SseEvent,
    ) -> Result<Vec<ProviderEvent>, ProviderError> {
        if self.terminal {
            return Ok(Vec::new());
        }
        if event.data == "[DONE]" {
            if self.protocol != Protocol::Chat || !self.started || !self.finish_seen {
                return Err(protocol());
            }
            self.terminal = true;
            return self.writer.encode(vec![
                GatewayEvent::Usage(self.usage.clone()),
                GatewayEvent::Completed(self.meta.clone().with_finish_reason(self.finish)),
            ]);
        }
        if event.data.trim().is_empty() {
            return Ok(Vec::new());
        }
        let value: Value = serde_json::from_str(&event.data).map_err(|_| protocol())?;
        if value.get("error").is_some()
            || value.get("type").and_then(Value::as_str) == Some("error")
        {
            return Err(protocol());
        }
        let mut facts = Vec::new();
        match self.protocol {
            Protocol::Responses => {
                let kind = value
                    .get("type")
                    .and_then(Value::as_str)
                    .or(event.event.as_deref())
                    .unwrap_or("");
                if matches!(kind, "response.failed" | "response.error") {
                    return Err(protocol());
                }
                if !self.started
                    && let Some(response) = value.get("response")
                {
                    if let Some(id) = response.get("id").and_then(Value::as_str) {
                        self.meta = ResponseMeta::new(
                            id,
                            response
                                .get("model")
                                .and_then(Value::as_str)
                                .unwrap_or("unknown"),
                        );
                    }
                    self.start(&mut facts);
                }
                if let Some(usage) = value
                    .get("response")
                    .and_then(|response| response.get("usage"))
                {
                    self.observe_usage(usage);
                    facts.push(GatewayEvent::Usage(self.usage.clone()));
                }
                if matches!(kind, "response.completed" | "response.incomplete") {
                    self.start(&mut facts);
                    self.terminal = true;
                    facts.push(GatewayEvent::Completed(
                        self.meta
                            .clone()
                            .with_finish_reason(if kind == "response.incomplete" {
                                FinishReason::Length
                            } else {
                                FinishReason::Stop
                            }),
                    ));
                }
                let wire = ProtocolWireEvent::json_with_sse_metadata(
                    "openai",
                    event.event,
                    value,
                    event.id,
                    event.retry,
                )
                .map_err(|_| protocol())?;
                return Ok(vec![ProviderEvent::canonical_with_wire(facts, wire)]);
            }
            Protocol::Chat => self.chat(&value, &mut facts)?,
            Protocol::Messages => self.messages(&value, &mut facts)?,
        }
        self.writer.encode(facts)
    }

    fn start(&mut self, facts: &mut Vec<GatewayEvent>) {
        if !self.started {
            self.started = true;
            facts.push(GatewayEvent::Started(self.meta.clone()));
        }
    }

    fn index(
        &mut self,
        key: String,
        kind: ContentKind,
        facts: &mut Vec<GatewayEvent>,
    ) -> Result<u32, ProviderError> {
        if let Some(index) = self.content.get(&key) {
            return Ok(*index);
        }
        let index = u32::try_from(self.content.len()).map_err(|_| protocol())?;
        if index > 4096 {
            return Err(protocol());
        }
        self.content.insert(key, index);
        facts.push(GatewayEvent::ContentAdded(ContentItem::new(index, kind)));
        Ok(index)
    }

    fn text(
        &mut self,
        text: &str,
        reasoning: bool,
        key: String,
        facts: &mut Vec<GatewayEvent>,
    ) -> Result<(), ProviderError> {
        if text.is_empty() {
            return Ok(());
        }
        self.start(facts);
        let index = self.index(
            key,
            if reasoning {
                ContentKind::Reasoning
            } else {
                ContentKind::Text
            },
            facts,
        )?;
        facts.push(if reasoning {
            GatewayEvent::ReasoningDelta(ReasoningDelta {
                content_index: index,
                text: text.to_owned(),
            })
        } else {
            GatewayEvent::TextDelta(TextDelta {
                content_index: index,
                text: text.to_owned(),
            })
        });
        Ok(())
    }

    fn tool(
        &mut self,
        block: u64,
        arguments: &str,
        facts: &mut Vec<GatewayEvent>,
    ) -> Result<(), ProviderError> {
        let (id, name) = self.tools.get(&block).cloned().ok_or_else(protocol)?;
        if id.is_empty() || name.is_empty() {
            return Err(protocol());
        }
        self.start(facts);
        let index = self.index(format!("tool:{block}"), ContentKind::ToolCall, facts)?;
        facts.push(GatewayEvent::ToolCallDelta(ToolCallDelta {
            content_index: index,
            call_id: id,
            name: Some(name),
            arguments_delta: arguments.to_owned(),
        }));
        Ok(())
    }

    fn chat(&mut self, value: &Value, facts: &mut Vec<GatewayEvent>) -> Result<(), ProviderError> {
        if let Some(usage) = value.get("usage").filter(|value| !value.is_null()) {
            self.observe_usage(usage);
        }
        let Some(choices) = value.get("choices").and_then(Value::as_array) else {
            return Err(protocol());
        };
        for choice in choices {
            if choice.get("index").and_then(Value::as_u64).unwrap_or(0) != 0 {
                return Err(protocol());
            }
            self.start(facts);
            if let Some(delta) = choice.get("delta") {
                if let Some(text) = delta.get("content").and_then(Value::as_str) {
                    self.text(text, false, "text".into(), facts)?;
                }
                if let Some(text) = delta
                    .get("reasoning_content")
                    .or_else(|| delta.get("reasoning"))
                    .and_then(Value::as_str)
                {
                    self.text(text, true, "reasoning".into(), facts)?;
                }
                if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
                    for call in calls {
                        let block = call
                            .get("index")
                            .and_then(Value::as_u64)
                            .ok_or_else(protocol)?;
                        let entry = self.tools.entry(block).or_default();
                        if let Some(id) = call.get("id").and_then(Value::as_str) {
                            entry.0 = id.to_owned();
                        }
                        if let Some(name) = call.pointer("/function/name").and_then(Value::as_str) {
                            entry.1.push_str(name);
                        }
                        self.tool(
                            block,
                            call.pointer("/function/arguments")
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            facts,
                        )?;
                    }
                }
            }
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                self.finish = finish_reason(reason);
                self.finish_seen = true;
            }
        }
        Ok(())
    }

    fn messages(
        &mut self,
        value: &Value,
        facts: &mut Vec<GatewayEvent>,
    ) -> Result<(), ProviderError> {
        match value.get("type").and_then(Value::as_str).unwrap_or("") {
            "ping" => {}
            "message_start" => {
                if let Some(message) = value.get("message") {
                    if let Some(id) = message.get("id").and_then(Value::as_str) {
                        self.meta = ResponseMeta::new(
                            id,
                            message
                                .get("model")
                                .and_then(Value::as_str)
                                .unwrap_or("unknown"),
                        );
                    }
                    if let Some(usage) = message.get("usage") {
                        self.observe_anthropic_usage(usage);
                    }
                }
                self.start(facts);
            }
            "content_block_start" => {
                let index = value
                    .get("index")
                    .and_then(Value::as_u64)
                    .ok_or_else(protocol)?;
                let block = value.get("content_block").ok_or_else(protocol)?;
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => self.text(
                        block.get("text").and_then(Value::as_str).unwrap_or(""),
                        false,
                        format!("text:{index}"),
                        facts,
                    )?,
                    Some("thinking") => self.text(
                        block.get("thinking").and_then(Value::as_str).unwrap_or(""),
                        true,
                        format!("reasoning:{index}"),
                        facts,
                    )?,
                    Some("redacted_thinking") => {}
                    Some("tool_use") => {
                        self.tools.insert(
                            index,
                            (
                                block
                                    .get("id")
                                    .and_then(Value::as_str)
                                    .ok_or_else(protocol)?
                                    .to_owned(),
                                block
                                    .get("name")
                                    .and_then(Value::as_str)
                                    .ok_or_else(protocol)?
                                    .to_owned(),
                            ),
                        );
                        let initial = block
                            .get("input")
                            .filter(|input| {
                                input.as_object().is_some_and(|object| !object.is_empty())
                            })
                            .map(Value::to_string)
                            .unwrap_or_default();
                        self.tool(index, &initial, facts)?;
                    }
                    _ => return Err(protocol()),
                }
            }
            "content_block_delta" => {
                let index = value
                    .get("index")
                    .and_then(Value::as_u64)
                    .ok_or_else(protocol)?;
                let delta = value.get("delta").ok_or_else(protocol)?;
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => self.text(
                        delta
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(protocol)?,
                        false,
                        format!("text:{index}"),
                        facts,
                    )?,
                    Some("thinking_delta") => self.text(
                        delta
                            .get("thinking")
                            .and_then(Value::as_str)
                            .ok_or_else(protocol)?,
                        true,
                        format!("reasoning:{index}"),
                        facts,
                    )?,
                    Some("input_json_delta") => self.tool(
                        index,
                        delta
                            .get("partial_json")
                            .and_then(Value::as_str)
                            .ok_or_else(protocol)?,
                        facts,
                    )?,
                    Some("signature_delta") => {}
                    _ => return Err(protocol()),
                }
            }
            "message_delta" => {
                if let Some(reason) = value.pointer("/delta/stop_reason").and_then(Value::as_str) {
                    self.finish = finish_reason(reason);
                    self.finish_seen = true;
                }
                if let Some(usage) = value.get("usage") {
                    self.observe_anthropic_usage(usage);
                }
            }
            "message_stop" => {
                if !self.started || !self.finish_seen {
                    return Err(protocol());
                }
                self.terminal = true;
                facts.push(GatewayEvent::Usage(self.usage.clone()));
                facts.push(GatewayEvent::Completed(
                    self.meta.clone().with_finish_reason(self.finish),
                ));
            }
            "content_block_stop" => {}
            _ => return Err(protocol()),
        }
        Ok(())
    }

    fn observe_usage(&mut self, value: &Value) {
        let Some(usage) = extract_usage(&serde_json::json!({"usage":value})) else {
            return;
        };
        self.usage.merge(&Usage {
            input_tokens: Some(usage.input_tokens),
            output_tokens: Some(usage.output_tokens),
            cached_tokens: Some(usage.cached_tokens),
            cache_write_tokens: Some(usage.cache_write_tokens),
            reasoning_tokens: Some(usage.reasoning_tokens),
            total_tokens: Some(usage.total_tokens),
            ..Usage::new()
        });
    }

    fn observe_anthropic_usage(&mut self, value: &Value) {
        let read = value.get("cache_read_input_tokens").and_then(Value::as_u64);
        let write = value
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64);
        self.usage.merge(&Usage {
            input_tokens: value
                .get("input_tokens")
                .and_then(Value::as_u64)
                .and_then(|input| {
                    input
                        .checked_add(read.unwrap_or(0))?
                        .checked_add(write.unwrap_or(0))
                }),
            output_tokens: value.get("output_tokens").and_then(Value::as_u64),
            cached_tokens: read,
            cache_write_tokens: write,
            ..Usage::new()
        });
    }
}

fn finish_reason(reason: &str) -> FinishReason {
    match reason {
        "length" | "max_tokens" => FinishReason::Length,
        "tool_calls" | "tool_use" => FinishReason::ToolCall,
        "content_filter" => FinishReason::ContentFilter,
        _ => FinishReason::Stop,
    }
}

pub(crate) fn protocol() -> ProviderError {
    ProviderError::new(ProviderErrorKind::Protocol, UpstreamSendState::Sent)
}
