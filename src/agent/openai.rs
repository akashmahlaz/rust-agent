//! OpenAI Chat Completions streaming client with tool-call support.
//!
//! Yields a stream of `OpenAiEvent` items. The runner is responsible for
//! turning these into AI SDK UI Message Stream Protocol frames, accumulating
//! tool call arguments, executing tools, and looping.

use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::Client;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::model_caps;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    /// Provider-native message content. May be a plain string OR an array of
    /// structured parts (e.g. `[{"type":"text",...},{"type":"image_url",...}]`)
    /// to support vision attachments. OpenAI Chat Completions and the
    /// Responses API both accept either shape for `user` messages; Anthropic
    /// has a dedicated transformer in [`super::anthropic`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
}

impl ChatMessage {
    /// Returns the textual content if and only if this message's content is a
    /// plain string. Returns `None` when content is missing OR when it's a
    /// structured parts array (callers needing parts should match on the raw
    /// `content` value directly).
    pub fn text_content(&self) -> Option<&str> {
        self.content.as_ref().and_then(|v| v.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub function: ToolCallFunction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallFunction {
    pub name: String,
    pub arguments: String,
}

pub fn requires_responses_api(model: &str) -> bool {
    let model = model.to_ascii_lowercase();
    model.starts_with("gpt-5") || model.contains("codex") || model.contains("5.3")
}

#[derive(Debug, Clone)]
pub enum OpenAiEvent {
    /// Streaming text delta from the assistant.
    TextDelta(String),
    /// Streaming reasoning/thinking chunk (e.g. DeepSeek-R1 `reasoning_content`,
    /// or text inside `<think>...</think>` tags from MiniMax / Qwen-thinking).
    ReasoningDelta(String),
    /// A tool call has begun (we got an id + name).
    ToolCallBegin {
        index: usize,
        id: String,
        name: String,
    },
    /// Partial tool input JSON streamed by the provider before invocation.
    ToolCallInputDelta { id: String, arguments: String },
    /// Provider token usage for the current model request.
    Usage {
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
    },
    /// Provider request was rate limited and retried before streaming began.
    ProviderRetry {
        attempt: usize,
        max_attempts: usize,
        delay_ms: u64,
        message: String,
    },
    /// Provider-side request id captured from response headers (e.g. OpenAI
    /// `x-request-id`, Anthropic `request-id`). Useful for log correlation.
    ProviderRequestId(String),
    /// Stream finished. The accumulated tool calls (if any) are returned so
    /// the runner can dispatch and loop.
    Finished {
        finish_reason: String,
        tool_calls: Vec<ToolCall>,
    },
    /// The underlying TCP connection was reset mid-stream (e.g. Anthropic load
    /// balancer closing idle connections, Windows OS error 10054).
    /// `retriable` is true when no content was emitted yet so the runner can
    /// transparently retry the step without the user seeing anything.
    StreamInterrupted {
        retriable: bool,
        message: String,
    },
}

#[derive(Debug, Clone)]
struct ProviderRetryInfo {
    attempt: usize,
    max_attempts: usize,
    delay_ms: u64,
    message: String,
}

/// State machine that splits a stream of text chunks into reasoning vs visible
/// segments based on `<think>...</think>` markers. Handles tag boundaries that
/// straddle SSE chunks.
#[derive(Default)]
pub struct ThinkSplitter {
    inside: bool,
    /// Pending characters that might be the start of a `<think>` or `</think>`
    /// tag (held back until we know for sure).
    pending: String,
}

impl ThinkSplitter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one chunk; returns ordered (is_reasoning, text) segments.
    pub fn feed(&mut self, chunk: &str) -> Vec<(bool, String)> {
        let mut input = std::mem::take(&mut self.pending);
        input.push_str(chunk);
        let mut out: Vec<(bool, String)> = Vec::new();
        let mut buf = String::new();
        let bytes = input.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let rest = &input[i..];
            let needle = if self.inside { "</think>" } else { "<think>" };
            if rest.starts_with(needle) {
                if !buf.is_empty() {
                    out.push((self.inside, std::mem::take(&mut buf)));
                }
                self.inside = !self.inside;
                i += needle.len();
                continue;
            }
            // If the rest could be the start of either tag, hold it for next chunk.
            if rest.len() < needle.len() && needle.starts_with(rest) {
                self.pending = rest.to_owned();
                break;
            }
            // Also hold for the *other* tag possibility (so we don't emit `<` then
            // discover `<think>` next chunk).
            let other = if self.inside { "<think>" } else { "</think>" };
            if rest.len() < other.len() && other.starts_with(rest) {
                self.pending = rest.to_owned();
                break;
            }
            // Otherwise consume one char.
            let ch = rest.chars().next().unwrap();
            buf.push(ch);
            i += ch.len_utf8();
        }
        if !buf.is_empty() {
            out.push((self.inside, buf));
        }
        out
    }
}

/// Returns a sensible max output token limit based on the provider/model.
/// Some providers (MiniMax, Groq) have very low defaults if not specified.
fn provider_max_output_tokens(_base_url: &str, model: &str) -> Option<u64> {
    // Don't send max_tokens for o1/o3 reasoning models (they use max_completion_tokens)
    if model.contains("o1") || model.contains("o3") {
        return None;
    }
    let tokens: u64 = if model.contains("gpt-4o") {
        16_384
    } else if model.contains("claude") {
        8_192
    } else if model.contains("gemini") {
        8_192
    } else {
        // Safe default for MiniMax, Groq, DeepSeek, OpenRouter, etc.
        8_192
    };
    Some(tokens)
}

/// Returns true if the provider is known to support `stream_options: { include_usage: true }`.
fn supports_stream_options(base_url: &str) -> bool {
    base_url.contains("api.openai.com")
        || base_url.contains("api.groq.com")
        || base_url.contains("api.deepseek.com")
}

/// Map UI-level reasoning hint ("none"|"auto"|"low"|"medium"|"high") to
/// OpenAI's `reasoning_effort` value. Returns `None` to omit the field
/// (so non-reasoning models aren't broken by an unexpected key).
fn openai_reasoning_effort(level: Option<&str>) -> Option<&'static str> {
    match level.map(str::to_ascii_lowercase).as_deref() {
        Some("low") => Some("low"),
        Some("medium") => Some("medium"),
        Some("high") => Some("high"),
        // "auto" lets us send `medium` so the model actually emits reasoning;
        // OpenAI defaults to `medium` already, but being explicit guarantees it.
        Some("auto") => Some("medium"),
        _ => None,
    }
}

pub async fn stream_chat(
    client: &Client,
    api_key: &str,
    base_url: &str,
    model: &str,
    messages: &[ChatMessage],
    tools: &[Value],
    reasoning_level: Option<&str>,
) -> Result<impl Stream<Item = Result<OpenAiEvent>> + use<>> {
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    tracing::debug!(model = %model, base_url = %base_url, "openai_stream_start");

    let mut body = serde_json::json!({
        "model": model,
        "stream": true,
        "messages": messages,
    });
    if supports_stream_options(base_url) {
        body["stream_options"] = serde_json::json!({ "include_usage": true });
    }
    if let Some(max_tokens) = provider_max_output_tokens(base_url, model) {
        body["max_tokens"] = serde_json::json!(max_tokens);
    }
    // Only include tools/tool_choice when there are actual tools defined.
    // Sending tool_choice:"auto" with an empty array is rejected by many providers.
    if !tools.is_empty() {
        body["tools"] = serde_json::json!(tools);
        body["tool_choice"] = serde_json::json!("auto");
    }
    if let Some(effort) = openai_reasoning_effort(reasoning_level) {
        // Gate by model: gpt-4o / gpt-4.1 / gpt-3.5 reject this field with
        // 400 "Unrecognized request argument supplied: reasoning_effort".
        // Only o-series, gpt-5*, codex* accept it.
        if model_caps::supports_reasoning_effort(model) {
            body["reasoning_effort"] = serde_json::json!(effort);
        } else {
            tracing::debug!(
                model = %model,
                level = %reasoning_level.unwrap_or(""),
                "reasoning_effort not supported by model — omitting"
            );
        }
    }

    let (response, retries) =
        send_openai_stream_request(client, api_key, &url, &body, "openai").await?;

    let request_id = extract_request_id(response.headers());
    let byte_stream = response.bytes_stream();

    let prefix: Vec<Result<OpenAiEvent>> = retries
        .into_iter()
        .map(|retry| {
            Ok(OpenAiEvent::ProviderRetry {
                attempt: retry.attempt,
                max_attempts: retry.max_attempts,
                delay_ms: retry.delay_ms,
                message: retry.message,
            })
        })
        .chain(request_id.map(|id| Ok(OpenAiEvent::ProviderRequestId(id))))
        .collect();

    Ok(futures::stream::iter(prefix).chain(parse_sse(byte_stream)))
}

pub async fn stream_responses(
    client: &Client,
    api_key: &str,
    base_url: &str,
    model: &str,
    messages: &[ChatMessage],
    tools: &[Value],
    reasoning_level: Option<&str>,
) -> Result<impl Stream<Item = Result<OpenAiEvent>> + use<>> {
    let url = format!("{}/responses", base_url.trim_end_matches('/'));
    let (instructions, input) = to_responses_input(messages);
    let response_tools = to_responses_tools(tools);

    let mut body = serde_json::json!({
        "model": model,
        "stream": true,
        "input": input,
    });
    if let Some(instructions) = instructions {
        body["instructions"] = serde_json::json!(instructions);
    }
    if !response_tools.is_empty() {
        body["tools"] = serde_json::json!(response_tools);
        body["tool_choice"] = serde_json::json!("auto");
        body["parallel_tool_calls"] = serde_json::json!(true);
    }
    if let Some(effort) = openai_reasoning_effort(reasoning_level) {
        // Gate by model. `stream_responses` is only routed to gpt-5*/codex*
        // via `requires_responses_api`, so this is defensive — but matches
        // the Chat Completions path and prevents future routing changes
        // from regressing into a 400.
        if model_caps::supports_reasoning_effort(model) {
            // `summary: "auto"` is REQUIRED to receive
            // `response.reasoning_summary_text.delta` SSE events.
            // Without it, gpt-5/o-series do reasoning silently and emit no delta.
            body["reasoning"] = serde_json::json!({ "effort": effort, "summary": "auto" });
        } else {
            tracing::debug!(
                model = %model,
                level = %reasoning_level.unwrap_or(""),
                "reasoning_effort not supported by model — omitting"
            );
        }
    } else {
        // Even when no explicit effort is set, request reasoning summaries so
        // gpt-5 / o-series surface their thinking instead of staying silent.
        body["reasoning"] = serde_json::json!({ "summary": "auto" });
    }

    let (response, retries) =
        send_openai_stream_request(client, api_key, &url, &body, "openai responses").await?;

    let request_id = extract_request_id(response.headers());
    let byte_stream = response.bytes_stream();

    let prefix: Vec<Result<OpenAiEvent>> = retries
        .into_iter()
        .map(|retry| {
            Ok(OpenAiEvent::ProviderRetry {
                attempt: retry.attempt,
                max_attempts: retry.max_attempts,
                delay_ms: retry.delay_ms,
                message: retry.message,
            })
        })
        .chain(request_id.map(|id| Ok(OpenAiEvent::ProviderRequestId(id))))
        .collect();

    Ok(futures::stream::iter(prefix).chain(parse_responses_sse(byte_stream)))
}

/// Extract the provider-assigned request id from response headers.
/// OpenAI uses `x-request-id`; Anthropic uses `request-id`. We try both.
fn extract_request_id(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get("x-request-id")
        .or_else(|| headers.get("request-id"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

async fn send_openai_stream_request(
    client: &Client,
    api_key: &str,
    url: &str,
    body: &Value,
    label: &str,
) -> Result<(Response, Vec<ProviderRetryInfo>)> {
    const MAX_ATTEMPTS: usize = 4;
    let mut retries = Vec::new();

    for attempt in 1..=MAX_ATTEMPTS {
        let response = client
            .post(url)
            .bearer_auth(api_key)
            .header("content-type", "application/json")
            .json(body)
            .send()
            .await
            .with_context(|| format!("sending {label} request"))?;

        let status = response.status();
        if status.is_success() {
            return Ok((response, retries));
        }

        let retry_after = retry_after_delay(&response);
        let text = response.text().await.unwrap_or_default();
        if status.as_u16() == 429 && attempt < MAX_ATTEMPTS {
            let delay = retry_after
                .or_else(|| retry_delay_from_body(&text))
                .unwrap_or_else(|| Duration::from_millis(750 * attempt as u64));
            tracing::warn!(
                attempt,
                max_attempts = MAX_ATTEMPTS,
                retry_after_ms = delay.as_millis() as u64,
                "{label} rate limited; retrying"
            );
            retries.push(ProviderRetryInfo {
                attempt,
                max_attempts: MAX_ATTEMPTS,
                delay_ms: delay.as_millis() as u64,
                message: format!("{label} was rate limited; retried request"),
            });
            tokio::time::sleep(delay).await;
            continue;
        }

        return Err(anyhow!("{label} error {status}: {text}"));
    }

    Err(anyhow!("{label} error: exhausted retry attempts"))
}

fn retry_after_delay(response: &Response) -> Option<Duration> {
    let value = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    let seconds = value.parse::<f64>().ok()?;
    Some(Duration::from_secs_f64(seconds.max(0.0)))
}

fn retry_delay_from_body(body: &str) -> Option<Duration> {
    let marker = "Please try again in ";
    let start = body.find(marker)? + marker.len();
    let rest = &body[start..];
    let end = rest.find('s')?;
    let seconds = rest[..end].trim().parse::<f64>().ok()?;
    Some(Duration::from_secs_f64(seconds.max(0.0) + 0.25))
}

fn to_responses_tools(tools: &[Value]) -> Vec<Value> {
    tools
        .iter()
        .filter_map(|tool| {
            let function = tool.get("function")?;
            Some(serde_json::json!({
                "type": "function",
                "name": function.get("name")?,
                "description": function.get("description").and_then(Value::as_str).unwrap_or(""),
                "parameters": function.get("parameters").cloned().unwrap_or_else(|| serde_json::json!({ "type": "object", "additionalProperties": true })),
                "strict": false,
            }))
        })
        .collect()
}

fn to_responses_input(messages: &[ChatMessage]) -> (Option<String>, Vec<Value>) {
    let mut instructions = None;
    let mut input = Vec::new();

    for message in messages {
        match message.role.as_str() {
            "system" => {
                if instructions.is_none() {
                    instructions = message.text_content().map(str::to_owned);
                }
            }
            "user" | "assistant" => {
                // Pass-through structured array content (vision parts etc.)
                // for user role; assistants still always send a plain string.
                if let Some(value) = message.content.as_ref() {
                    let is_empty = match value {
                        Value::String(s) => s.is_empty(),
                        Value::Array(a) => a.is_empty(),
                        _ => true,
                    };
                    if !is_empty {
                        input.push(serde_json::json!({
                            "role": message.role,
                            "content": value,
                        }));
                    }
                }
                if message.role == "assistant" {
                    if let Some(tool_calls) = &message.tool_calls {
                        for tool_call in tool_calls {
                            input.push(serde_json::json!({
                                "type": "function_call",
                                "id": format!("fc_{}", tool_call.id),
                                "call_id": tool_call.id,
                                "name": tool_call.function.name,
                                "arguments": tool_call.function.arguments,
                            }));
                        }
                    }
                }
            }
            "tool" => {
                if let Some(call_id) = &message.tool_call_id {
                    input.push(serde_json::json!({
                        "type": "function_call_output",
                        "call_id": call_id,
                        "output": message.text_content().unwrap_or(""),
                    }));
                }
            }
            _ => {}
        }
    }

    (instructions, input)
}

fn parse_sse(
    upstream: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static + Unpin,
) -> impl Stream<Item = Result<OpenAiEvent>> {
    async_stream::try_stream! {
        let mut upstream = upstream;
        let mut buffer = String::new();
        let mut tool_call_indices: HashMap<usize, ToolCall> = HashMap::new();
        let mut splitter = ThinkSplitter::new();
        let mut pending_finish_reason: Option<String> = None;
        let mut chunk_count: u64 = 0;
        let mut had_any_content = false;

        while let Some(chunk) = upstream.next().await {
            let chunk = chunk.context("reading OpenAI stream")?;
            chunk_count += 1;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            loop {
                let Some(idx) = buffer.find("\n\n") else { break };
                let event = buffer[..idx].to_owned();
                buffer.drain(..idx + 2);

                for line in event.lines() {
                    let Some(payload) = line.strip_prefix("data:") else { continue };
                    let payload = payload.trim();
                    if payload.is_empty() {
                        continue;
                    }
                    if payload == "[DONE]" {
                        let mut tool_calls: Vec<(usize, ToolCall)> =
                            tool_call_indices.drain().collect();
                        tool_calls.sort_by_key(|(i, _)| *i);
                        let finish_reason = pending_finish_reason.take().unwrap_or_else(|| {
                            if tool_calls.is_empty() {
                                "stop".to_owned()
                            } else {
                                "tool_calls".to_owned()
                            }
                        });
                        tracing::info!(saw_done = true, chunks_received = chunk_count, "openai_stream_end");
                        yield OpenAiEvent::Finished {
                            finish_reason,
                            tool_calls: tool_calls.into_iter().map(|(_, c)| c).collect(),
                        };
                        return;
                    }

                    let json: Value = match serde_json::from_str(payload) {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(payload = %payload, error = %e, "openai_stream_parse_error");
                            continue;
                        }
                    };

                    // Detect provider-level error objects in the stream
                    if let Some(err_obj) = json.get("error") {
                        tracing::warn!(error = %err_obj, "openai_stream_error_chunk");
                    }

                    if let Some(usage) = json.get("usage") {
                        let prompt_tokens = usage.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0);
                        let completion_tokens = usage.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0);
                        let total_tokens = usage.get("total_tokens").and_then(Value::as_u64).unwrap_or(prompt_tokens + completion_tokens);
                        if total_tokens > 0 {
                            yield OpenAiEvent::Usage { prompt_tokens, completion_tokens, total_tokens };
                        }
                    }

                    let Some(choice) = json
                        .get("choices")
                        .and_then(|c| c.get(0))
                    else {
                        continue;
                    };

                    let delta = choice.get("delta");

                    // DeepSeek-R1 / o1 / Groq reasoning models emit a separate
                    // `reasoning_content` field (sometimes `reasoning`).
                    if let Some(text) = delta
                        .and_then(|d| d.get("reasoning_content").or_else(|| d.get("reasoning")))
                        .and_then(|c| c.as_str())
                    {
                        if !text.is_empty() {
                            yield OpenAiEvent::ReasoningDelta(text.to_owned());
                        }
                    }

                    if let Some(text) = delta
                        .and_then(|d| d.get("content"))
                        .and_then(|c| c.as_str())
                    {
                        if !text.is_empty() {
                            // MiniMax / Qwen-thinking emit thinking inline as
                            // <think>...</think> inside content. Split it out.
                            for (is_reasoning, segment) in splitter.feed(text) {
                                if segment.is_empty() {
                                    continue;
                                }
                                if is_reasoning {
                                    yield OpenAiEvent::ReasoningDelta(segment);
                                } else {
                                    yield OpenAiEvent::TextDelta(segment);
                                }
                            }
                        }
                    }

                    if let Some(tcs) = delta
                        .and_then(|d| d.get("tool_calls"))
                        .and_then(|t| t.as_array())
                    {
                        for tc in tcs {
                            let index = tc
                                .get("index")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0) as usize;
                            let id_opt = tc.get("id").and_then(|v| v.as_str()).map(str::to_owned);
                            let name_opt = tc
                                .get("function")
                                .and_then(|f| f.get("name"))
                                .and_then(|v| v.as_str())
                                .map(str::to_owned);
                            let arg_delta = tc
                                .get("function")
                                .and_then(|f| f.get("arguments"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("");

                            let entry = tool_call_indices.entry(index).or_insert_with(|| ToolCall {
                                id: String::new(),
                                kind: "function".to_owned(),
                                function: ToolCallFunction {
                                    name: String::new(),
                                    arguments: String::new(),
                                },
                            });

                            if let Some(id) = id_opt.clone() {
                                if entry.id.is_empty() {
                                    entry.id = id.clone();
                                }
                            }
                            if let Some(name) = name_opt.clone() {
                                if entry.function.name.is_empty() {
                                    entry.function.name = name.clone();
                                }
                            }
                            entry.function.arguments.push_str(arg_delta);

                            if id_opt.is_some() || name_opt.is_some() {
                                yield OpenAiEvent::ToolCallBegin {
                                    index,
                                    id: entry.id.clone(),
                                    name: entry.function.name.clone(),
                                };
                            }
                            if !entry.id.is_empty() && !entry.function.name.is_empty() && !entry.function.arguments.is_empty() {
                                yield OpenAiEvent::ToolCallInputDelta {
                                    id: entry.id.clone(),
                                    arguments: entry.function.arguments.clone(),
                                };
                            }
                        }
                    }

                    if let Some(reason) = choice
                        .get("finish_reason")
                        .and_then(|r| r.as_str())
                    {
                        pending_finish_reason = Some(reason.to_owned());
                    }

                    // Track that we received meaningful content from the stream
                    had_any_content = true;
                }
            }
        }

        // Stream ended without receiving [DONE]
        if had_any_content {
            tracing::warn!(chunks_received = chunk_count, "openai stream ended without [DONE] sentinel");
            let mut tool_calls: Vec<(usize, ToolCall)> =
                tool_call_indices.drain().collect();
            tool_calls.sort_by_key(|(i, _)| *i);
            let finish_reason = pending_finish_reason.take().unwrap_or_else(|| {
                if tool_calls.is_empty() {
                    "stop".to_owned()
                } else {
                    "tool_calls".to_owned()
                }
            });
            tracing::info!(saw_done = false, chunks_received = chunk_count, "openai_stream_end");
            yield OpenAiEvent::Finished {
                finish_reason,
                tool_calls: tool_calls.into_iter().map(|(_, c)| c).collect(),
            };
        } else {
            tracing::info!(saw_done = false, chunks_received = chunk_count, "openai_stream_end (empty)");
        }
    }
}

fn parse_responses_sse(
    upstream: impl Stream<Item = reqwest::Result<Bytes>> + Send + 'static + Unpin,
) -> impl Stream<Item = Result<OpenAiEvent>> {
    async_stream::try_stream! {
        let mut upstream = upstream;
        let mut buffer = String::new();
        let mut tool_call_indices: HashMap<usize, ToolCall> = HashMap::new();

        while let Some(chunk) = upstream.next().await {
            let chunk = chunk.context("reading OpenAI responses stream")?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            loop {
                let Some(idx) = buffer.find("\n\n") else { break };
                let event = buffer[..idx].to_owned();
                buffer.drain(..idx + 2);

                for line in event.lines() {
                    let Some(payload) = line.strip_prefix("data:") else { continue };
                    let payload = payload.trim();
                    if payload.is_empty() || payload == "[DONE]" {
                        continue;
                    }

                    let json: Value = match serde_json::from_str(payload) {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    let event_type = json.get("type").and_then(Value::as_str).unwrap_or("");

                    match event_type {
                        "response.output_text.delta" => {
                            if let Some(text) = json.get("delta").and_then(Value::as_str) {
                                if !text.is_empty() {
                                    yield OpenAiEvent::TextDelta(text.to_owned());
                                }
                            }
                        }
                        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                            if let Some(text) = json.get("delta").and_then(Value::as_str) {
                                if !text.is_empty() {
                                    yield OpenAiEvent::ReasoningDelta(text.to_owned());
                                }
                            }
                        }
                        "response.output_item.added" => {
                            if let Some(item) = json.get("item").filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call")) {
                                let index = json.get("output_index").and_then(Value::as_u64).unwrap_or(0) as usize;
                                let id = item
                                    .get("call_id")
                                    .or_else(|| item.get("id"))
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_owned();
                                let name = item.get("name").and_then(Value::as_str).unwrap_or("").to_owned();
                                let arguments = item.get("arguments").and_then(Value::as_str).unwrap_or("").to_owned();
                                tool_call_indices.insert(index, ToolCall {
                                    id: id.clone(),
                                    kind: "function".to_owned(),
                                    function: ToolCallFunction { name: name.clone(), arguments },
                                });
                                yield OpenAiEvent::ToolCallBegin { index, id, name };
                            }
                        }
                        "response.function_call_arguments.delta" => {
                            let index = json.get("output_index").and_then(Value::as_u64).unwrap_or(0) as usize;
                            let delta = json.get("delta").and_then(Value::as_str).unwrap_or("");
                            if let Some(tool_call) = tool_call_indices.get_mut(&index) {
                                tool_call.function.arguments.push_str(delta);
                                if !tool_call.id.is_empty() && !tool_call.function.name.is_empty() && !tool_call.function.arguments.is_empty() {
                                    yield OpenAiEvent::ToolCallInputDelta {
                                        id: tool_call.id.clone(),
                                        arguments: tool_call.function.arguments.clone(),
                                    };
                                }
                            }
                        }
                        "response.function_call_arguments.done" => {
                            let index = json.get("output_index").and_then(Value::as_u64).unwrap_or(0) as usize;
                            if let Some(arguments) = json.get("arguments").and_then(Value::as_str) {
                                if let Some(tool_call) = tool_call_indices.get_mut(&index) {
                                    tool_call.function.arguments = arguments.to_owned();
                                }
                            }
                        }
                        "response.output_item.done" => {
                            if let Some(item) = json.get("item").filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call")) {
                                let index = json.get("output_index").and_then(Value::as_u64).unwrap_or(0) as usize;
                                let id = item
                                    .get("call_id")
                                    .or_else(|| item.get("id"))
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_owned();
                                let name = item.get("name").and_then(Value::as_str).unwrap_or("").to_owned();
                                let arguments = item.get("arguments").and_then(Value::as_str).unwrap_or("").to_owned();
                                tool_call_indices.insert(index, ToolCall {
                                    id,
                                    kind: "function".to_owned(),
                                    function: ToolCallFunction { name, arguments },
                                });
                            }
                        }
                        "response.completed" => {
                            if let Some(usage) = json.get("response").and_then(|response| response.get("usage")) {
                                let prompt_tokens = usage.get("input_tokens").and_then(Value::as_u64).unwrap_or(0);
                                let completion_tokens = usage.get("output_tokens").and_then(Value::as_u64).unwrap_or(0);
                                let total_tokens = usage.get("total_tokens").and_then(Value::as_u64).unwrap_or(prompt_tokens + completion_tokens);
                                if total_tokens > 0 {
                                    yield OpenAiEvent::Usage { prompt_tokens, completion_tokens, total_tokens };
                                }
                            }
                            let mut tool_calls: Vec<(usize, ToolCall)> = tool_call_indices.drain().collect();
                            tool_calls.sort_by_key(|(index, _)| *index);
                            let tool_calls: Vec<ToolCall> = tool_calls.into_iter().map(|(_, tool_call)| tool_call).collect();
                            let finish_reason = if tool_calls.is_empty() { "stop" } else { "tool_calls" }.to_owned();
                            yield OpenAiEvent::Finished { finish_reason, tool_calls };
                            return;
                        }
                        "response.failed" | "response.incomplete" => {
                            let message = json
                                .get("response")
                                .and_then(|response| response.get("error"))
                                .and_then(|error| error.get("message"))
                                .and_then(Value::as_str)
                                .unwrap_or(event_type);
                            Err(anyhow!("openai responses error: {message}"))?;
                        }
                        "error" => {
                            let message = json
                                .get("message")
                                .or_else(|| json.get("error").and_then(|error| error.get("message")))
                                .and_then(Value::as_str)
                                .unwrap_or("unknown OpenAI responses stream error");
                            Err(anyhow!("openai responses stream error: {message}"))?;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
