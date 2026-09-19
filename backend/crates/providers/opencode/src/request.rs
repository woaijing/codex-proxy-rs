//! 将现有 Responses 入站语义转换为官方模型目录指定的协议。

use gateway_core::error::{ProviderError, ProviderErrorKind};
use gateway_core::operation::ProtocolPayload;
use serde_json::{Map, Value, json};

use crate::catalog::{Model, Protocol};
use crate::selection::error;

pub(crate) fn encode(payload: &ProtocolPayload, model: &Model) -> Result<Value, ProviderError> {
    if payload.protocol() != "openai" {
        return Err(invalid());
    }
    let source = payload.body();
    if model.protocol == Protocol::Responses {
        let mut body = source.clone();
        body.insert("model".into(), Value::String(model.id.clone()));
        body.insert("stream".into(), Value::Bool(true));
        return Ok(Value::Object(body));
    }
    if source
        .get("previous_response_id")
        .is_some_and(|value| !value.is_null())
        || source.get("background") == Some(&Value::Bool(true))
    {
        return Err(error(ProviderErrorKind::Unsupported));
    }
    let anthropic = model.protocol == Protocol::Messages;
    if anthropic
        && (source
            .get("reasoning")
            .and_then(|value| value.get("effort"))
            .is_some_and(|value| !value.is_null() && value.as_str() != Some("none"))
            || source
                .get("text")
                .and_then(|value| value.get("format"))
                .and_then(|value| value.get("type"))
                .is_some_and(|value| value.as_str() != Some("text")))
    {
        return Err(error(ProviderErrorKind::Unsupported));
    }
    let mut body = Map::from_iter([
        ("model".into(), json!(model.id)),
        ("stream".into(), json!(true)),
    ]);
    let mut messages = Vec::new();
    let mut system = Vec::new();
    if let Some(instructions) = source.get("instructions").filter(|value| !value.is_null()) {
        let text = instructions.as_str().ok_or_else(invalid)?;
        if anthropic {
            system.push(json!({"type":"text", "text":text}));
        } else {
            messages.push(json!({"role":"system", "content":text}));
        }
    }
    let input = match source.get("input") {
        Some(Value::String(text)) => vec![json!({"role":"user", "content":text})],
        Some(Value::Array(items)) => items.clone(),
        _ => return Err(invalid()),
    };
    for item in input {
        match item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message")
        {
            "message" => {
                let role = item
                    .get("role")
                    .and_then(Value::as_str)
                    .ok_or_else(invalid)?;
                let content = content(item.get("content").ok_or_else(invalid)?, anthropic)?;
                if anthropic && matches!(role, "system" | "developer") {
                    system.extend(content);
                } else {
                    let role = if role == "developer" { "system" } else { role };
                    if !matches!(role, "user" | "assistant" | "system") {
                        return Err(invalid());
                    }
                    messages.push(json!({"role":role, "content":content}));
                }
            }
            "function_call" => {
                let id = required(&item, "call_id")?;
                let name = required(&item, "name")?;
                let arguments = required(&item, "arguments")?;
                if anthropic {
                    let arguments: Value =
                        serde_json::from_str(arguments).map_err(|_| invalid())?;
                    if !arguments.is_object() {
                        return Err(invalid());
                    }
                    messages.push(json!({"role":"assistant", "content":[{"type":"tool_use", "id":id,"name":name,"input":arguments}]}));
                } else {
                    messages.push(json!({"role":"assistant", "content":null, "tool_calls":[{"id":id,"type":"function","function":{"name":name,"arguments":arguments}}]}));
                }
            }
            "function_call_output" => {
                let id = required(&item, "call_id")?;
                let output = item.get("output").ok_or_else(invalid)?;
                if anthropic {
                    messages.push(json!({"role":"user", "content":[{"type":"tool_result","tool_use_id":id,"content":content(output, true)?}]}));
                } else {
                    messages.push(json!({"role":"tool", "tool_call_id":id,"content":if output.is_string() {output.clone()} else {json!(content(output, false)?)} }));
                }
            }
            // Responses 的加密推理项不能作为另一协议的签名块重放。
            "reasoning" => {}
            _ => return Err(error(ProviderErrorKind::Unsupported)),
        }
    }
    if messages.is_empty() {
        return Err(invalid());
    }
    if anthropic {
        messages = merge_messages(messages);
        if !system.is_empty() {
            body.insert("system".into(), json!(system));
        }
        body.insert(
            "max_tokens".into(),
            source
                .get("max_output_tokens")
                .cloned()
                .unwrap_or_else(|| json!(model.output.min(8192))),
        );
    } else {
        messages = merge_chat_tool_calls(messages);
        body.insert("stream_options".into(), json!({"include_usage":true}));
        if let Some(tokens) = source.get("max_output_tokens") {
            body.insert("max_tokens".into(), tokens.clone());
        }
        if let Some(effort) = source
            .get("reasoning")
            .and_then(|value| value.get("effort"))
        {
            body.insert("reasoning_effort".into(), effort.clone());
        }
        if let Some(format) = source.get("text").and_then(|value| value.get("format")) {
            let mut format = format.as_object().cloned().ok_or_else(invalid)?;
            if format.get("type").and_then(Value::as_str) == Some("json_schema") {
                format.remove("type");
                body.insert(
                    "response_format".into(),
                    json!({"type":"json_schema","json_schema":format}),
                );
            } else {
                body.insert("response_format".into(), json!(format));
            }
        }
        if let Some(parallel) = source.get("parallel_tool_calls") {
            body.insert("parallel_tool_calls".into(), parallel.clone());
        }
    }
    body.insert("messages".into(), Value::Array(messages));
    for key in ["temperature", "top_p"] {
        if let Some(value) = source.get(key) {
            body.insert(key.into(), value.clone());
        }
    }
    if let Some(tools) = source.get("tools") {
        let tools = tools
            .as_array()
            .ok_or_else(invalid)?
            .iter()
            .map(|tool| {
                if tool.get("type").and_then(Value::as_str) != Some("function") {
                    return Err(error(ProviderErrorKind::Unsupported));
                }
                let name = required(tool, "name")?;
                let mut function = Map::from_iter([("name".into(), json!(name))]);
                for key in ["description", "parameters", "strict"] {
                    if let Some(value) = tool.get(key) {
                        if anthropic && key == "strict" {
                            continue;
                        }
                        function.insert(
                            if anthropic && key == "parameters" {
                                "input_schema"
                            } else {
                                key
                            }
                            .into(),
                            value.clone(),
                        );
                    }
                }
                if anthropic {
                    function
                        .entry("input_schema")
                        .or_insert_with(|| json!({"type":"object","properties":{}}));
                    Ok(json!(function))
                } else {
                    Ok(json!({"type":"function","function":function}))
                }
            })
            .collect::<Result<Vec<_>, ProviderError>>()?;
        body.insert("tools".into(), json!(tools));
    }
    if let Some(choice) = source.get("tool_choice") {
        let choice = if let Some(mode) = choice.as_str() {
            if anthropic {
                json!({"type":match mode {"required" => "any", "auto" => "auto", "none" => "none", _ => return Err(invalid())}})
            } else {
                choice.clone()
            }
        } else if choice.get("type").and_then(Value::as_str) == Some("function") {
            let name = required(choice, "name")?;
            if anthropic {
                json!({"type":"tool","name":name})
            } else {
                json!({"type":"function","function":{"name":name}})
            }
        } else {
            return Err(error(ProviderErrorKind::Unsupported));
        };
        body.insert("tool_choice".into(), choice);
    }
    Ok(Value::Object(body))
}

fn content(value: &Value, anthropic: bool) -> Result<Vec<Value>, ProviderError> {
    if let Some(text) = value.as_str() {
        return Ok(vec![json!({"type":"text","text":text})]);
    }
    value.as_array().ok_or_else(invalid)?.iter().map(|part| {
        match part.get("type").and_then(Value::as_str) {
            Some("input_text" | "output_text" | "text") => Ok(json!({"type":"text","text":required(part,"text")?})),
            Some("input_image") => {
                let url = required(part, "image_url")?;
                if !anthropic { return Ok(json!({"type":"image_url","image_url":{"url":url,"detail":part.get("detail").and_then(Value::as_str).unwrap_or("auto")}})); }
                let source = if let Some(data) = url.strip_prefix("data:") {
                    let (media_type, data) = data.split_once(";base64,").ok_or_else(invalid)?;
                    json!({"type":"base64","media_type":media_type,"data":data})
                } else if url.starts_with("https://") || url.starts_with("http://") { json!({"type":"url","url":url}) }
                else { return Err(invalid()); };
                Ok(json!({"type":"image","source":source}))
            }
            _ => Err(error(ProviderErrorKind::Unsupported)),
        }
    }).collect()
}

fn merge_messages(messages: Vec<Value>) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::new();
    for mut message in messages {
        if let Some(last) = merged.last_mut()
            && last.get("role") == message.get("role")
            && let Some(content) = last.get_mut("content").and_then(Value::as_array_mut)
            && let Some(next) = message.get_mut("content").and_then(Value::as_array_mut)
        {
            content.append(next);
        } else {
            merged.push(message);
        }
    }
    merged
}

fn merge_chat_tool_calls(messages: Vec<Value>) -> Vec<Value> {
    let mut merged: Vec<Value> = Vec::new();
    for mut message in messages {
        // Responses 将并行工具调用表示为多个 item，Chat 要求同一 assistant 消息列出这些调用。
        if message.get("role").and_then(Value::as_str) == Some("assistant")
            && let Some(calls) = message.get_mut("tool_calls").and_then(Value::as_array_mut)
            && let Some(last) = merged
                .last_mut()
                .filter(|last| last.get("role").and_then(Value::as_str) == Some("assistant"))
        {
            if let Some(previous) = last.get_mut("tool_calls").and_then(Value::as_array_mut) {
                previous.append(calls);
            } else {
                last["tool_calls"] = json!(std::mem::take(calls));
            }
        } else {
            merged.push(message);
        }
    }
    merged
}

fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str, ProviderError> {
    value.get(key).and_then(Value::as_str).ok_or_else(invalid)
}
fn invalid() -> ProviderError {
    error(ProviderErrorKind::InvalidRequest)
}
