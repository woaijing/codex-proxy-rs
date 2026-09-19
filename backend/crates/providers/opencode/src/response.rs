//! 翻译协议必须提供完整 Responses wire；API 不会从计量用 canonical facts 合成客户端正文。

use std::collections::BTreeMap;

use gateway_core::error::ProviderError;
use gateway_core::event::{
    ContentKind, FinishReason, GatewayEvent, ProtocolWireEvent, ProviderEvent, ResponseMeta,
};
use gateway_core::metering::Usage;
use serde_json::{Value, json};

use crate::stream::protocol;

#[derive(Default)]
pub(crate) struct ResponseWriter {
    meta: Option<ResponseMeta>,
    items: BTreeMap<u32, Item>,
    usage: Usage,
    sequence: u64,
    bytes: usize,
    pending_facts: Vec<GatewayEvent>,
}

struct Item {
    kind: ContentKind,
    id: String,
    call_id: String,
    name: String,
    text: String,
    added: bool,
}

impl Item {
    fn value(&self, completed: bool) -> Value {
        let status = if completed {
            "completed"
        } else {
            "in_progress"
        };
        match self.kind {
            ContentKind::ToolCall => {
                json!({"id":self.id,"type":"function_call","status":status,"call_id":self.call_id,"name":self.name,"arguments":self.text})
            }
            ContentKind::Reasoning => {
                json!({"id":self.id,"type":"reasoning","summary":[{"type":"summary_text","text":self.text}]})
            }
            _ => {
                json!({"id":self.id,"type":"message","role":"assistant","status":status,"content":[{"type":"output_text","text":self.text,"annotations":[]}]})
            }
        }
    }
}

impl ResponseWriter {
    pub(crate) fn encode(
        &mut self,
        facts: Vec<GatewayEvent>,
    ) -> Result<Vec<ProviderEvent>, ProviderError> {
        let mut result = Vec::new();
        for fact in facts {
            let mut wires = self.observe(&fact)?;
            if wires.is_empty() {
                self.pending_facts.push(fact);
                continue;
            }
            let terminal = matches!(fact, GatewayEvent::Completed(_));
            let fact_index = if terminal { wires.len() - 1 } else { 0 };
            let mut fact = Some(fact);
            for (index, mut value) in wires.drain(..).enumerate() {
                value["sequence_number"] = json!(self.sequence);
                self.sequence = self.sequence.checked_add(1).ok_or_else(protocol)?;
                let event_type = value
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(protocol)?
                    .to_owned();
                let wire = ProtocolWireEvent::json("openai", Some(event_type), value)
                    .map_err(|_| protocol())?;
                let canonical = if index == fact_index {
                    let mut facts = std::mem::take(&mut self.pending_facts);
                    facts.extend(fact.take());
                    facts
                } else {
                    Vec::new()
                };
                result.push(ProviderEvent::canonical_with_wire(canonical, wire));
            }
        }
        Ok(result)
    }

    fn observe(&mut self, fact: &GatewayEvent) -> Result<Vec<Value>, ProviderError> {
        match fact {
            GatewayEvent::Started(meta) => {
                self.meta = Some(meta.clone());
                Ok(vec![
                    json!({"type":"response.created","response":self.snapshot(false, None)?}),
                ])
            }
            GatewayEvent::ContentAdded(content) => {
                let item = Item {
                    kind: content.kind(),
                    id: format!(
                        "item_{}_{}",
                        self.meta.as_ref().ok_or_else(protocol)?.response_id(),
                        content.index()
                    ),
                    call_id: String::new(),
                    name: String::new(),
                    text: String::new(),
                    added: content.kind() != ContentKind::ToolCall,
                };
                let wires = if item.added {
                    let (kind, part) = if item.kind == ContentKind::Reasoning {
                        (
                            "response.reasoning_summary_part.added",
                            json!({"type":"summary_text","text":""}),
                        )
                    } else {
                        (
                            "response.content_part.added",
                            json!({"type":"output_text","text":"","annotations":[]}),
                        )
                    };
                    let mut part_event = json!({"type":kind,"item_id":item.id,"output_index":content.index(),"part":part});
                    part_event[if item.kind == ContentKind::Reasoning {
                        "summary_index"
                    } else {
                        "content_index"
                    }] = json!(0);
                    vec![
                        json!({"type":"response.output_item.added","output_index":content.index(),"item":item.value(false)}),
                        part_event,
                    ]
                } else {
                    Vec::new()
                };
                self.items.insert(content.index(), item);
                Ok(wires)
            }
            GatewayEvent::TextDelta(delta) => self.text(delta.content_index, &delta.text, false),
            GatewayEvent::ReasoningDelta(delta) => {
                self.text(delta.content_index, &delta.text, true)
            }
            GatewayEvent::ToolCallDelta(delta) => {
                self.reserve(delta.arguments_delta.len())?;
                let item = self
                    .items
                    .get_mut(&delta.content_index)
                    .ok_or_else(protocol)?;
                let mut wires = Vec::new();
                if !item.added {
                    item.call_id = delta.call_id.clone();
                    item.name = delta.name.clone().ok_or_else(protocol)?;
                    item.added = true;
                    wires.push(json!({"type":"response.output_item.added","output_index":delta.content_index,"item":item.value(false)}));
                }
                item.text.push_str(&delta.arguments_delta);
                wires.push(json!({"type":"response.function_call_arguments.delta","item_id":item.id,"output_index":delta.content_index,"delta":delta.arguments_delta}));
                Ok(wires)
            }
            GatewayEvent::Usage(usage) => {
                self.usage.merge(usage);
                Ok(Vec::new())
            }
            GatewayEvent::Completed(meta) => {
                let mut wires = Vec::new();
                for (index, item) in &self.items {
                    let mut done = json!({"item_id":item.id,"output_index":index});
                    match item.kind {
                        ContentKind::ToolCall => {
                            done["type"] = json!("response.function_call_arguments.done");
                            done["arguments"] = json!(item.text);
                        }
                        ContentKind::Reasoning => {
                            done["type"] = json!("response.reasoning_summary_text.done");
                            done["summary_index"] = json!(0);
                            done["text"] = json!(item.text);
                        }
                        _ => {
                            done["type"] = json!("response.output_text.done");
                            done["content_index"] = json!(0);
                            done["text"] = json!(item.text);
                        }
                    }
                    wires.push(done);
                    if item.kind != ContentKind::ToolCall {
                        let mut done = json!({"item_id":item.id,"output_index":index});
                        if item.kind == ContentKind::Reasoning {
                            done["type"] = json!("response.reasoning_summary_part.done");
                            done["summary_index"] = json!(0);
                            done["part"] = item.value(true)["summary"][0].clone();
                        } else {
                            done["type"] = json!("response.content_part.done");
                            done["content_index"] = json!(0);
                            done["part"] = item.value(true)["content"][0].clone();
                        }
                        wires.push(done);
                    }
                    wires.push(json!({"type":"response.output_item.done","output_index":index,"item":item.value(true)}));
                }
                let incomplete = matches!(
                    meta.finish_reason(),
                    Some(FinishReason::Length | FinishReason::ContentFilter)
                );
                wires.push(json!({"type":if incomplete {"response.incomplete"}else{"response.completed"},"response":self.snapshot(true,meta.finish_reason())?}));
                Ok(wires)
            }
            _ => Err(protocol()),
        }
    }

    fn text(
        &mut self,
        index: u32,
        text: &str,
        reasoning: bool,
    ) -> Result<Vec<Value>, ProviderError> {
        self.reserve(text.len())?;
        let item = self.items.get_mut(&index).ok_or_else(protocol)?;
        item.text.push_str(text);
        let mut event = json!({"type":if reasoning {"response.reasoning_summary_text.delta"}else{"response.output_text.delta"},"item_id":item.id,"output_index":index,"delta":text});
        event[if reasoning {
            "summary_index"
        } else {
            "content_index"
        }] = json!(0);
        Ok(vec![event])
    }

    fn reserve(&mut self, bytes: usize) -> Result<(), ProviderError> {
        // 完整 JSON 响应和终态快照需要累积正文；超限必须报错，不能截断后宣称成功。
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|total| *total <= 32 * 1024 * 1024)
            .ok_or_else(protocol)?;
        Ok(())
    }

    fn snapshot(
        &self,
        completed: bool,
        reason: Option<FinishReason>,
    ) -> Result<Value, ProviderError> {
        let meta = self.meta.as_ref().ok_or_else(protocol)?;
        let incomplete = match reason {
            Some(FinishReason::Length) => Some("max_output_tokens"),
            Some(FinishReason::ContentFilter) => Some("content_filter"),
            _ => None,
        };
        let total = self.usage.total_tokens.or_else(|| {
            self.usage
                .input_tokens?
                .checked_add(self.usage.output_tokens?)
        });
        let usage = if self.usage.input_tokens.is_none() && self.usage.output_tokens.is_none() {
            Value::Null
        } else {
            json!({"input_tokens":self.usage.input_tokens,"output_tokens":self.usage.output_tokens,"total_tokens":total,"input_tokens_details":{"cached_tokens":self.usage.cached_tokens.unwrap_or(0)},"output_tokens_details":{"reasoning_tokens":self.usage.reasoning_tokens.unwrap_or(0)}})
        };
        Ok(
            json!({"id":meta.response_id(),"object":"response","model":meta.model(),"status":if !completed{"in_progress"}else if incomplete.is_some(){"incomplete"}else{"completed"},"output":self.items.values().map(|item|item.value(completed)).collect::<Vec<_>>(),"usage":usage,"error":null,"incomplete_details":incomplete.map(|reason|json!({"reason":reason}))}),
        )
    }
}
