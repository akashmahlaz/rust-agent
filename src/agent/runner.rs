//! Per-run agent task: drives the LLM ↔ tool loop, persists every event +
//! every message, and broadcasts SSE frames to subscribed clients.
//!
//! Long-session friendly:
//!   * MAX_STEPS configurable via `OPERON_AGENT_MAX_STEPS` (default 200).
//!   * Conversation history is reloaded from Postgres at the start of each
//!     run so multi-turn sessions keep full context.
//!   * The runner survives client disconnects — its work is independent of
//!     SSE subscribers; clients can resume by replaying from `last_seq`.
//!   * `RunHandle::cancel()` aborts the loop cooperatively.
//!
//! Persistence layout:
//!   `messages.parts` is an array of stream-part shapes (the same shapes the
//!   `useStreamEvents` hydrator consumes), e.g.
//!   `{type:"text-delta", text:"..."}` and
//!   `{type:"tool-call-output-available", toolCallId, toolName, args, result, state:"output-available"}`.
//!   This makes UI reload trivial *and* lets us reconstruct OpenAI ChatMessage
//!   history without a second column.

use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use futures::StreamExt;
use reqwest::Client;
use serde_json::{Value, json};
use sqlx::{Pool, Postgres, Row};
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    anthropic, context, events,
    openai::{self, ChatMessage, OpenAiEvent, ToolCall, ToolCallFunction},
    prompt::build_system_message,
    tools::{self, AgentContext, Workspace},
    types::{AgentEvent, AttachmentInput, RunId, RunStatus},
};

/// Build a present-tense + past-tense pair for a tool call so the UI can
/// show "Reading file `foo.ts`…" while running and "Read file `foo.ts`"
/// after. Mirrors Copilot's `invocationMessage` / `pastTenseMessage`.
fn tool_messages(name: &str, args: &Value) -> (String, String) {
    let target = args
        .get("path")
        .or_else(|| args.get("target"))
        .or_else(|| args.get("query"))
        .or_else(|| args.get("command"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let target_md = if target.is_empty() {
        String::new()
    } else {
        format!(" `{}`", target)
    };
    let owner = args.get("owner").and_then(|v| v.as_str()).unwrap_or("");
    let repo = args.get("repo").and_then(|v| v.as_str()).unwrap_or("");
    let repo_target = if owner.is_empty() || repo.is_empty() {
        String::new()
    } else {
        format!(" `{}/{}`", owner, repo)
    };
    let repo_path_target = if owner.is_empty() || repo.is_empty() || target.is_empty() {
        repo_target.clone()
    } else {
        format!(" `{}/{}/{}`", owner, repo, target)
    };
    match name {
        "read_file" => (
            format!("Reading{}", target_md),
            format!("Read{}", target_md),
        ),
        "write_file" => (
            format!("Writing{}", target_md),
            format!("Wrote{}", target_md),
        ),
        "apply_patch" => ("Applying edits".to_owned(), "Applied edits".to_owned()),
        "list_dir" => (
            format!("Listing{}", target_md),
            format!("Listed{}", target_md),
        ),
        "search" => (
            format!("Searching{}", target_md),
            format!("Searched{}", target_md),
        ),
        "exec" => (format!("Running{}", target_md), format!("Ran{}", target_md)),
        "github_get_status" => (
            "Checking GitHub connection".to_owned(),
            "Checked GitHub connection".to_owned(),
        ),
        "github_list_repos" => (
            "Listing your repositories".to_owned(),
            "Listed your repositories".to_owned(),
        ),
        "github_get_repo" => (
            format!("Reading{}", repo_target),
            format!("Read{}", repo_target),
        ),
        "github_list_contents" => (
            format!("Listing{}", repo_path_target),
            format!("Listed{}", repo_path_target),
        ),
        "github_read_file" => (
            format!("Reading{}", repo_path_target),
            format!("Read{}", repo_path_target),
        ),
        "github_search_code" => (
            format!("Searching GitHub code{}", target_md),
            format!("Searched GitHub code{}", target_md),
        ),
        "github_list_branches" => (
            format!("Listing branches{}", repo_target),
            format!("Listed branches{}", repo_target),
        ),
        "github_list_issues" => (
            format!("Listing issues{}", repo_target),
            format!("Listed issues{}", repo_target),
        ),
        "github_list_pull_requests" => (
            format!("Listing pull requests{}", repo_target),
            format!("Listed pull requests{}", repo_target),
        ),
        other => (format!("Running `{}`", other), format!("Ran `{}`", other)),
    }
}

fn normalize_tool_arguments(arguments: &str) -> Value {
    let trimmed = arguments.trim();
    if trimmed.is_empty() {
        return json!({});
    }

    match serde_json::from_str::<Value>(trimmed) {
        Ok(value) if value.is_object() => value,
        Ok(_) | Err(_) => json!({}),
    }
}

fn canonical_tool_call(tool_call: &ToolCall) -> ToolCall {
    let mut normalized = tool_call.clone();
    normalized.function.arguments =
        normalize_tool_arguments(&tool_call.function.arguments).to_string();
    normalized
}

const BROADCAST_CAPACITY: usize = 1024;
const DEFAULT_MAX_STEPS: usize = 200;
const DEFAULT_SUBAGENT_MAX_STEPS: usize = 40;
const OPENAI_MAX_TOOLS: usize = 128;

fn provider_tool_definitions(
    provider: &str,
    channel: &str,
) -> Vec<Value> {
    let mut definitions = tools::tool_definitions(channel);

    if provider != "anthropic" && definitions.len() > OPENAI_MAX_TOOLS {
        let omitted = definitions.len() - OPENAI_MAX_TOOLS;
        let omitted_names: Vec<String> = definitions
            .iter()
            .skip(OPENAI_MAX_TOOLS)
            .filter_map(|tool| {
                tool.get("function")
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect();

        tracing::warn!(
            total = definitions.len(),
            kept = OPENAI_MAX_TOOLS,
            omitted,
            omitted_tools = ?omitted_names,
            "provider tool limit exceeded; omitting trailing connector tools"
        );
        definitions.truncate(OPENAI_MAX_TOOLS);
    }

    definitions
}

#[derive(Clone)]
pub struct RunHandle {
    pub run_id: RunId,
    pub broadcast: broadcast::Sender<AgentEvent>,
    pub cancel: CancellationToken,
    sequence: Arc<Mutex<i64>>,
    db: Pool<Postgres>,
}

impl RunHandle {
    fn new(run_id: RunId, db: Pool<Postgres>) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            run_id,
            broadcast: tx,
            cancel: CancellationToken::new(),
            sequence: Arc::new(Mutex::new(0)),
            db,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AgentEvent> {
        self.broadcast.subscribe()
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn emit(&self, frame: &Value) -> Result<()> {
        let mut seq_guard = self.sequence.lock().await;
        *seq_guard += 1;
        let sequence = *seq_guard;
        drop(seq_guard);

        let event_type = frame
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        sqlx::query(
            "insert into run_events (id, run_id, sequence, event_type, payload) values ($1, $2, $3, $4, $5)",
        )
        .bind(Uuid::now_v7())
        .bind(self.run_id)
        .bind(sequence)
        .bind(event_type)
        .bind(frame)
        .execute(&self.db)
        .await
        .context("inserting run_event")?;

        let _ = self.broadcast.send(AgentEvent {
            sequence,
            frame: frame.clone(),
        });
        Ok(())
    }
}

pub struct RunnerSpec {
    pub run_id: RunId,
    pub user_id: Uuid,
    pub conversation_id: Uuid,
    pub provider: String,
    pub model: String,
    pub openai_api_key: String,
    pub base_url: String,
    pub workspace: Workspace,
    pub github_token: Option<String>,
    pub initial_user_message: String,
    /// Optional file/image attachments to include as structured content
    /// parts in the first user turn (vision-capable models will see actual
    /// image data; text-only models see the URLs as text).
    pub initial_user_attachments: Vec<AttachmentInput>,
    pub db: Pool<Postgres>,
    pub max_steps: usize,
    pub channel: String,
    /// Reasoning effort hint forwarded to the provider. One of
    /// "none" | "auto" | "low" | "medium" | "high".
    pub reasoning_level: Option<String>,
    /// In-memory registry of live runs. Used so that subagent child runs
    /// spawned from inside the agent loop are also discoverable for SSE
    /// tailing and cancellation by other endpoints.
    pub agents: super::registry::AgentRegistry,
}

pub fn spawn(spec: RunnerSpec) -> RunHandle {
    let handle = RunHandle::new(spec.run_id, spec.db.clone());
    let task_handle = handle.clone();
    let db = spec.db.clone();
    tokio::spawn(async move {
        let cancel = task_handle.cancel.clone();
        let result = tokio::select! {
            r = run(spec, task_handle.clone()) => r,
            _ = cancel.cancelled() => Err(anyhow::anyhow!("run cancelled by user")),
        };
        if let Err(err) = result {
            tracing::warn!(run_id = %task_handle.run_id, error = %err, "agent run errored");
            let _ = task_handle.emit(&events::error(&err.to_string())).await;
            let _ = task_handle.emit(&events::message_end()).await;
            let final_status = if cancel.is_cancelled() {
                RunStatus::Cancelled
            } else {
                RunStatus::Failed
            };
            let _ = set_status(&db, task_handle.run_id, final_status, Some(err.to_string())).await;
        }
    });
    handle
}

async fn run(spec: RunnerSpec, handle: RunHandle) -> Result<()> {
    set_status(&spec.db, spec.run_id, RunStatus::Running, None).await?;

    let client = Client::builder()
        // Long-running streams (many tool steps) can exceed 2 minutes.
        .timeout(std::time::Duration::from_secs(600))
        // TCP keepalives prevent Anthropic/OpenAI load balancers from silently
        // killing idle connections during model "thinking" time (Windows
        // WSAECONNRESET / os error 10054).
        .tcp_keepalive(std::time::Duration::from_secs(30))
        .build()
        .context("building reqwest client")?;

    let agent_ctx = AgentContext::new(
        spec.workspace.clone(),
        client.clone(),
        spec.github_token.clone(),
        spec.channel.clone(),
        spec.user_id,
        spec.db.clone(),
    );

    let mut messages: Vec<ChatMessage> = Vec::new();
    messages.push(ChatMessage {
        role: "system".to_owned(),
        content: Some(Value::String(build_system_message(
            &spec.workspace,
            &spec.channel,
        ))),
        name: None,
        tool_call_id: None,
        tool_calls: None,
    });
    let prior = load_conversation_history(&spec.db, &spec.conversation_id).await?;
    if prior.is_empty() {
        messages.push(ChatMessage {
            role: "user".to_owned(),
            content: Some(build_initial_user_content(
                &spec.initial_user_message,
                &spec.initial_user_attachments,
            )),
            name: None,
            tool_call_id: None,
            tool_calls: None,
        });
    } else {
        messages.extend(prior);
    }

    for _step in 0..spec.max_steps {
        if handle.cancel.is_cancelled() {
            anyhow::bail!("run cancelled");
        }

        // Per-step retry counter for transparent connection-reset recovery.
        // When the provider resets mid-stream before any content is emitted,
        // we re-issue the exact same request up to this many times.
        // Uses a labeled inner loop so retries don't consume step budget.
        const MAX_STEP_CONNECTION_RETRIES: u32 = 3;
        let mut step_connection_retries: u32 = 0;

        // Per-step state that persists across retry attempts but is reset each iteration.
        // The initial values are placeholders; they get overwritten at the start of each loop.
        #[allow(unused_assignments)]
        let mut text_started = false;
        #[allow(unused_assignments)]
        let mut accumulated_text = String::new();
        #[allow(unused_assignments)]
        let mut accumulated_reasoning = String::new();
        #[allow(unused_assignments)]
        let mut reasoning_id: Option<String> = None;
        #[allow(unused_assignments)]
        let mut tool_call_started: Vec<bool> = Vec::new();
        #[allow(unused_assignments)]
        let mut streamed_tool_info: Vec<(String, String)> = Vec::new();
        #[allow(unused_assignments)]
        let mut final_tool_calls: Vec<ToolCall> = Vec::new();
        #[allow(unused_assignments)]
        let mut finish_reason = String::new();
        #[allow(unused_assignments)]
        let mut usage: Option<(u64, u64, u64)> = None;
        #[allow(unused_assignments)]
        let mut provider_notices: Vec<Value> = Vec::new();
        #[allow(unused_assignments)]
        let mut provider_request_id: Option<String> = None;
        #[allow(unused_assignments)]
        let mut stream_failed: Option<String> = None;
        #[allow(unused_assignments)]
        let mut stream_interrupted_retriable = false;

        'retry_step: loop {
        // ---- inner retry loop starts here ----
        // Reset per-attempt state (keep retry counter)
        text_started = false;
        accumulated_text.clear();
        accumulated_reasoning.clear();
        reasoning_id = None;
        tool_call_started.clear();
        streamed_tool_info.clear();
        final_tool_calls.clear();
        finish_reason.clear();
        usage = None;
        provider_notices.clear();
        provider_request_id = None;
        stream_failed = None;
        stream_interrupted_retriable = false;

        let tool_definitions = provider_tool_definitions(&spec.provider, &spec.channel);

        // Apply automatic context windowing — transparent to user.
        // The UI shows all messages; the model receives a sliding window with
        // a heuristic summary of older turns if the budget is exceeded.
        let system_prompt = build_system_message(&spec.workspace, &spec.channel);
        let system_tokens = context::estimate_system_tokens(&system_prompt);
        let tool_tokens = context::estimate_tools_tokens(&tool_definitions);
        let windowed_messages = context::prepare_context(
            &messages,
            system_tokens,
            tool_tokens,
            &spec.provider,
            &spec.model,
        );

        tracing::info!(
            run_id = %spec.run_id,
            step = _step,
            provider = %spec.provider,
            model = %spec.model,
            messages_count = messages.len(),
            windowed_count = windowed_messages.len(),
            tools_count = tool_definitions.len(),
            retry_attempt = step_connection_retries,
            "llm_request_start"
        );

        let stream = if spec.provider == "anthropic" {
            let s = anthropic::stream_chat(
                &client,
                &spec.openai_api_key,
                &spec.model,
                &windowed_messages,
                &tool_definitions,
                spec.reasoning_level.as_deref(),
            )
            .await?;
            futures::future::Either::Left(futures::future::Either::Left(s))
        } else if spec.provider == "openai" && openai::requires_responses_api(&spec.model) {
            let s = openai::stream_responses(
                &client,
                &spec.openai_api_key,
                &spec.base_url,
                &spec.model,
                &windowed_messages,
                &tool_definitions,
                spec.reasoning_level.as_deref(),
            )
            .await?;
            futures::future::Either::Left(futures::future::Either::Right(s))
        } else {
            let s = openai::stream_chat(
                &client,
                &spec.openai_api_key,
                &spec.base_url,
                &spec.model,
                &messages,
                &tool_definitions,
                spec.reasoning_level.as_deref(),
            )
            .await?;
            futures::future::Either::Right(s)
        };
        tokio::pin!(stream);

        while let Some(event) = stream.next().await {
            if handle.cancel.is_cancelled() {
                anyhow::bail!("run cancelled");
            }
            let event = match event {
                Ok(ev) => ev,
                Err(err) => {
                    let msg = format!("{err:#}");
                    handle
                        .emit(&events::stream_error(
                            &msg,
                            provider_request_id.as_deref(),
                            Some(spec.provider.as_str()),
                        ))
                        .await?;
                    stream_failed = Some(msg);
                    break;
                }
            };
            match event {
                OpenAiEvent::ReasoningDelta(delta) => {
                    let id = match reasoning_id.as_ref() {
                        Some(id) => id.clone(),
                        None => {
                            let id = Uuid::now_v7().to_string();
                            handle.emit(&events::reasoning_start(&id)).await?;
                            reasoning_id = Some(id.clone());
                            id
                        }
                    };
                    accumulated_reasoning.push_str(&delta);
                    handle.emit(&events::reasoning_delta(&id, &delta)).await?;
                }
                OpenAiEvent::TextDelta(delta) => {
                    if let Some(id) = reasoning_id.take() {
                        handle.emit(&events::reasoning_end(&id)).await?;
                    }
                    text_started = true;
                    accumulated_text.push_str(&delta);
                    handle.emit(&events::text_delta(&delta)).await?;
                }
                OpenAiEvent::ToolCallBegin { index, id, name } => {
                    if let Some(rid) = reasoning_id.take() {
                        handle.emit(&events::reasoning_end(&rid)).await?;
                    }
                    while tool_call_started.len() <= index {
                        tool_call_started.push(false);
                    }
                    while streamed_tool_info.len() <= index {
                        streamed_tool_info.push((String::new(), String::new()));
                    }
                    if !tool_call_started[index] {
                        // Args aren't streamed yet — emit start with the
                        // tool name only; runner will follow up with
                        // tool-call-update once args parse.
                        let (invocation, _past) = tool_messages(&name, &serde_json::Value::Null);
                        handle
                            .emit(&events::tool_call_start(
                                &id,
                                &name,
                                Some(&invocation),
                                None,
                            ))
                            .await?;
                        tool_call_started[index] = true;
                        streamed_tool_info[index] = (id.clone(), name.clone());
                    }
                }
                OpenAiEvent::ToolCallInputDelta { id, arguments, .. } => {
                    let parsed = normalize_tool_arguments(&arguments);
                    handle
                        .emit(&events::tool_call_input_streaming(&id, &parsed))
                        .await?;
                }
                OpenAiEvent::Usage {
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                } => {
                    usage = Some((prompt_tokens, completion_tokens, total_tokens));
                    handle
                        .emit(&events::usage(
                            prompt_tokens,
                            completion_tokens,
                            total_tokens,
                        ))
                        .await?;
                }
                OpenAiEvent::ProviderRetry {
                    attempt,
                    max_attempts,
                    delay_ms,
                    message,
                } => {
                    let text = format!(
                        "{} (attempt {}/{}, waited {} ms)",
                        message, attempt, max_attempts, delay_ms
                    );
                    handle.emit(&events::warning(&text)).await?;
                    provider_notices.push(json!({ "type": "warning", "text": text }));
                }
                OpenAiEvent::ProviderRequestId(id) => {
                    handle
                        .emit(&events::provider_request_id(
                            &spec.provider,
                            &spec.model,
                            &id,
                        ))
                        .await?;
                    // Persist on the run row so dashboard log/sessions can
                    // display it after the stream ends. Keep only the latest
                    // id (multiple model calls per run = last one wins).
                    let _ = sqlx::query(
                        "update runs set provider_request_id = $1, updated_at = now() where id = $2",
                    )
                    .bind(&id)
                    .bind(spec.run_id)
                    .execute(&spec.db)
                    .await;
                    provider_request_id = Some(id);
                }
                OpenAiEvent::Finished {
                    finish_reason: r,
                    tool_calls,
                } => {
                    finish_reason = r;
                    final_tool_calls = tool_calls;
                }
                OpenAiEvent::StreamInterrupted { retriable, message } => {
                    if retriable && step_connection_retries < MAX_STEP_CONNECTION_RETRIES {
                        // Nothing was emitted to the client yet — retry silently.
                        stream_interrupted_retriable = true;
                    } else {
                        // Content was already streamed or retries exhausted.
                        // Surface a user-friendly error instead of the raw OS error.
                        handle
                            .emit(&events::stream_error(
                                &message,
                                provider_request_id.as_deref(),
                                Some(spec.provider.as_str()),
                            ))
                            .await?;
                        stream_failed = Some(message);
                    }
                    break;
                }
            }
        }

        if let Some(id) = reasoning_id.take() {
            handle.emit(&events::reasoning_end(&id)).await?;
        }
        if text_started {
            handle.emit(&events::text_end()).await?;
        }

        // Silent retry: connection was reset before any content was emitted.
        // Re-issue the same step without touching messages or the user-visible
        // stream. Backoff: 1s, 2s, 3s.
        if stream_interrupted_retriable {
            step_connection_retries += 1;
            let delay = std::time::Duration::from_secs(step_connection_retries as u64);
            tracing::warn!(
                run_id = %spec.run_id,
                attempt = step_connection_retries,
                max = MAX_STEP_CONNECTION_RETRIES,
                delay_ms = delay.as_millis() as u64,
                "stream connection reset — retrying step transparently"
            );
            tokio::time::sleep(delay).await;
            continue 'retry_step; // inner loop — retry same step without consuming budget
        }
        break 'retry_step; // success or non-retriable error — exit retry loop
        } // end 'retry_step loop

        // If the underlying provider stream errored mid-flight, persist the
        // partial assistant turn (so the user sees their inline error card on
        // reload) and bail out cleanly with a typed error.
        if let Some(err_msg) = stream_failed.clone() {
            // Cleanup: emit tool-call-output-error for any tool calls that started
            // during streaming but never completed (prevents UI stuck in loading)
            for (idx, started) in tool_call_started.iter().enumerate() {
                if *started {
                    if let Some((tc_id, tc_name)) = streamed_tool_info.get(idx) {
                        if !tc_id.is_empty() {
                            handle
                                .emit(&events::tool_call_output_error(
                                    tc_id,
                                    "Stream interrupted before tool execution",
                                ))
                                .await?;
                            handle.emit(&events::tool_call_end(tc_id)).await?;
                            tracing::debug!(
                                run_id = %spec.run_id,
                                tool_call_id = %tc_id,
                                tool_name = %tc_name,
                                "emitted cleanup tool-call-output-error for orphaned tool card"
                            );
                        }
                    }
                }
            }

            let mut assistant_parts: Vec<Value> = Vec::new();
            assistant_parts.extend(provider_notices.clone());
            if !accumulated_reasoning.is_empty() {
                assistant_parts.push(json!({
                    "type": "reasoning-delta",
                    "text": accumulated_reasoning,
                }));
                assistant_parts.push(json!({ "type": "reasoning-end", "text": "" }));
            }
            if !accumulated_text.is_empty() {
                assistant_parts.push(json!({ "type": "text-delta", "text": accumulated_text }));
                assistant_parts.push(json!({ "type": "text-end", "text": "" }));
            }
            assistant_parts.push(json!({
                "type": "stream-error",
                "message": err_msg,
                "requestId": provider_request_id,
                "provider": spec.provider,
            }));
            persist_message(
                &spec.db,
                &spec.conversation_id,
                Some(&spec.user_id),
                "assistant",
                &accumulated_text,
                &assistant_parts,
                Some(&spec.model),
            )
            .await?;
            handle.emit(&events::message_end()).await?;
            set_status(&spec.db, spec.run_id, RunStatus::Failed, Some(err_msg.clone())).await?;
            anyhow::bail!(err_msg);
        }

        // Build UI parts for this assistant turn (reasoning + text + per-tool-call parts).
        let mut assistant_parts: Vec<Value> = Vec::new();
        assistant_parts.extend(provider_notices);
        if !accumulated_reasoning.is_empty() {
            assistant_parts.push(json!({
                "type": "reasoning-delta",
                "text": accumulated_reasoning,
            }));
            assistant_parts.push(json!({
                "type": "reasoning-end",
                "text": "",
            }));
        }
        if !accumulated_text.is_empty() {
            assistant_parts.push(json!({ "type": "text-delta", "text": accumulated_text }));
            assistant_parts.push(json!({ "type": "text-end", "text": "" }));
        }
        if let Some((prompt_tokens, completion_tokens, total_tokens)) = usage {
            assistant_parts.push(json!({
                "type": "usage",
                "promptTokens": prompt_tokens,
                "completionTokens": completion_tokens,
                "totalTokens": total_tokens,
            }));
        }

        let final_tool_calls: Vec<ToolCall> =
            final_tool_calls.iter().map(canonical_tool_call).collect();

        let assistant_message = ChatMessage {
            role: "assistant".to_owned(),
            content: if accumulated_text.is_empty() {
                None
            } else {
                Some(Value::String(accumulated_text.clone()))
            },
            name: None,
            tool_call_id: None,
            tool_calls: if final_tool_calls.is_empty() {
                None
            } else {
                Some(final_tool_calls.clone())
            },
        };
        messages.push(assistant_message.clone());

        // Warn if the model hit its output token limit (response truncated).
        if finish_reason == "length" {
            tracing::warn!(run_id = %spec.run_id, "model hit max_tokens limit - response truncated");
            handle.emit(&events::warning("Response was truncated because the model reached its output token limit. Try a shorter prompt or enable a model with higher output limits.")).await?;
        }

        tracing::info!(
            run_id = %spec.run_id,
            step = _step,
            finish_reason = %finish_reason,
            text_len = accumulated_text.len(),
            tool_calls_count = final_tool_calls.len(),
            has_usage = usage.is_some(),
            "llm_stream_ended"
        );

        // No tool calls → final answer.
        if final_tool_calls.is_empty() {
            persist_message(
                &spec.db,
                &spec.conversation_id,
                Some(&spec.user_id),
                "assistant",
                accumulated_text.as_str(),
                &assistant_parts,
                Some(&spec.model),
            )
            .await?;

            // Auto-generate conversation title on the first turn
            if _step == 0 {
                auto_title_conversation(
                    &spec.db,
                    &spec.conversation_id,
                    &spec.initial_user_message,
                )
                .await
                .ok(); // non-fatal
            }

            handle.emit(&events::message_end()).await?;
            set_status(&spec.db, spec.run_id, RunStatus::Completed, None).await?;
            return Ok(());
        }

        // Execute every tool call inline; record the invocation + result as
        // a single rich part on the assistant message.
        for (tc_idx, tc) in final_tool_calls.iter().enumerate() {
            if handle.cancel.is_cancelled() {
                // Cleanup: emit tool-call-output-error for remaining tool calls
                // that had tool-call-start emitted during streaming.
                for remaining_tc in final_tool_calls.iter().skip(tc_idx) {
                    if !remaining_tc.id.is_empty() {
                        handle
                            .emit(&events::tool_call_output_error(
                                &remaining_tc.id,
                                "Run cancelled",
                            ))
                            .await?;
                        handle.emit(&events::tool_call_end(&remaining_tc.id)).await?;
                    }
                }
                anyhow::bail!("run cancelled");
            }
            let parsed_input: Value = normalize_tool_arguments(&tc.function.arguments);
            let (invocation, past_tense) = tool_messages(&tc.function.name, &parsed_input);
            let is_subagent = tc.function.name == "spawn_subagent"
                || tc.function.name == "run_subagent"
                || tc.function.name == "runSubagent";
            if is_subagent {
                let agent_name = parsed_input
                    .get("agent")
                    .or_else(|| parsed_input.get("agentName"))
                    .and_then(Value::as_str);
                let prompt = parsed_input.get("prompt").and_then(Value::as_str);
                handle
                    .emit(&events::subagent_start(&tc.id, agent_name, prompt))
                    .await?;
                handle
                    .emit(&events::subagent_progress(
                        &tc.id,
                        agent_name,
                        "Starting subagent run",
                        "active",
                    ))
                    .await?;
                assistant_parts.push(json!({
                    "type": "subagent-start",
                    "toolCallId": tc.id,
                    "agentName": agent_name,
                    "prompt": prompt,
                }));
                assistant_parts.push(json!({
                    "type": "subagent-progress",
                    "toolCallId": tc.id,
                    "agentName": agent_name,
                    "text": "Starting subagent run",
                    "status": "active",
                }));
            }
            // Update the card with a richer invocation message now that we
            // have the parsed args.
            handle
                .emit(&events::tool_call_update(&tc.id, Some(&invocation), None))
                .await?;
            handle
                .emit(&events::tool_call_input_available(&tc.id, &parsed_input))
                .await?;
            handle.emit(&events::tool_call_execute(&tc.id)).await?;

            assistant_parts.push(json!({
                "type": "tool-call-start",
                "toolCallId": tc.id,
                "toolName": tc.function.name,
                "invocationMessage": invocation,
                "state": "calling",
            }));
            assistant_parts.push(json!({
                "type": "tool-call-input-available",
                "toolCallId": tc.id,
                "toolName": tc.function.name,
                "args": parsed_input,
                "invocationMessage": invocation,
                "state": "input-available",
            }));
            assistant_parts.push(json!({
                "type": "tool-call-execute",
                "toolCallId": tc.id,
                "toolName": tc.function.name,
                "args": parsed_input,
                "invocationMessage": invocation,
                "state": "executing",
            }));

            let tool_start = std::time::Instant::now();
            let dispatch_result = if is_subagent {
                execute_subagent(
                    &spec,
                    &handle,
                    &agent_ctx,
                    &tc.id,
                    parsed_input
                        .get("agent")
                        .or_else(|| parsed_input.get("agentName"))
                        .and_then(Value::as_str),
                    parsed_input
                        .get("prompt")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                )
                .await
            } else {
                tools::dispatch(&agent_ctx, &tc.function.name, &parsed_input).await
            };
            let tool_elapsed = tool_start.elapsed();
            let (result_value, error_text) = match dispatch_result {
                Ok(value) => {
                    tracing::info!(
                        run_id = %spec.run_id,
                        tool = %tc.function.name,
                        tool_call_id = %tc.id,
                        duration_ms = tool_elapsed.as_millis() as u64,
                        success = true,
                        "tool_executed"
                    );
                    handle
                        .emit(&events::tool_call_output_available(
                            &tc.id,
                            &value,
                            Some(&past_tense),
                        ))
                        .await?;
                    (value, None)
                }
                Err(err) => {
                    let text = err.to_string();
                    tracing::warn!(
                        run_id = %spec.run_id,
                        tool = %tc.function.name,
                        tool_call_id = %tc.id,
                        duration_ms = tool_elapsed.as_millis() as u64,
                        error = %text,
                        "tool_execution_failed"
                    );
                    handle
                        .emit(&events::tool_call_output_error(&tc.id, &text))
                        .await?;
                    (json!({ "error": text }), Some(text))
                }
            };
            handle.emit(&events::tool_call_end(&tc.id)).await?;

            if is_subagent {
                let agent_name = parsed_input
                    .get("agent")
                    .or_else(|| parsed_input.get("agentName"))
                    .and_then(Value::as_str);
                let child_run_id = result_value
                    .get("runId")
                    .or_else(|| result_value.get("run_id"))
                    .or_else(|| result_value.get("childRunId"))
                    .and_then(Value::as_str);
                let child_log_url =
                    child_run_id.map(|run_id| format!("/dashboard/sessions?runId={run_id}"));
                handle
                    .emit(&events::subagent_progress(
                        &tc.id,
                        agent_name,
                        "Subagent run completed",
                        if error_text.is_some() {
                            "error"
                        } else {
                            "complete"
                        },
                    ))
                    .await?;
                handle
                    .emit(&events::subagent_result(
                        &tc.id,
                        agent_name,
                        &result_value,
                        child_run_id,
                        child_log_url.as_deref(),
                    ))
                    .await?;
                assistant_parts.push(json!({
                    "type": "subagent-progress",
                    "toolCallId": tc.id,
                    "agentName": agent_name,
                    "text": "Subagent run completed",
                    "status": if error_text.is_some() { "error" } else { "complete" },
                }));
                assistant_parts.push(json!({
                    "type": "subagent-result",
                    "toolCallId": tc.id,
                    "agentName": agent_name,
                    "runId": child_run_id,
                    "logUrl": child_log_url,
                    "result": result_value,
                }));
            }

            assistant_parts.push(json!({
                "type": "tool-call-output-available",
                "toolCallId": tc.id,
                "toolName": tc.function.name,
                "args": parsed_input,
                "result": result_value,
                "errorText": error_text,
                "invocationMessage": invocation,
                "pastTenseMessage": past_tense,
                "state": if error_text.is_some() { "output-error" } else { "output-available" },
            }));

            let tool_message = ChatMessage {
                role: "tool".to_owned(),
                content: Some(Value::String(result_value.to_string())),
                name: Some(tc.function.name.clone()),
                tool_call_id: Some(tc.id.clone()),
                tool_calls: None,
            };
            messages.push(tool_message);
        }

        persist_message(
            &spec.db,
            &spec.conversation_id,
            Some(&spec.user_id),
            "assistant",
            accumulated_text.as_str(),
            &assistant_parts,
            Some(&spec.model),
        )
        .await?;
    }

    let confirmation_id = format!("continue-{}", spec.run_id);
    let max_step_message = format!(
        "The agent reached the configured limit of {} tool step(s). Continue to let it keep working from the current state.",
        spec.max_steps
    );
    let assistant_parts = vec![
        json!({ "type": "warning", "text": max_step_message }),
        json!({
            "type": "confirmation",
            "confirmationId": confirmation_id,
            "title": "Continue working?",
            "message": "The model reached the current tool-iteration limit.",
            "data": { "reason": "max_steps", "runId": spec.run_id.to_string(), "maxSteps": spec.max_steps },
            "buttons": ["Continue", "Stop"],
        }),
    ];
    handle.emit(&events::warning(&max_step_message)).await?;
    handle
        .emit(&events::confirmation(
            &confirmation_id,
            "Continue working?",
            "The model reached the current tool-iteration limit.",
            &json!({ "reason": "max_steps", "runId": spec.run_id.to_string(), "maxSteps": spec.max_steps }),
            &["Continue", "Stop"],
        ))
        .await?;
    persist_message(
        &spec.db,
        &spec.conversation_id,
        Some(&spec.user_id),
        "assistant",
        &max_step_message,
        &assistant_parts,
        Some(&spec.model),
    )
    .await?;
    handle.emit(&events::message_end()).await?;
    set_status(&spec.db, spec.run_id, RunStatus::Completed, None).await?;
    Ok(())
}

/// Reconstruct the OpenAI ChatMessage history from the persisted UI parts.
async fn load_conversation_history(
    db: &Pool<Postgres>,
    conversation_id: &Uuid,
) -> Result<Vec<ChatMessage>> {
    let rows = sqlx::query(
        "select role, content, parts from messages where conversation_id = $1 order by created_at asc",
    )
    .bind(conversation_id)
    .fetch_all(db)
    .await
    .context("loading conversation history")?;

    let mut out = Vec::with_capacity(rows.len());
    // Persistent deduplication set: prevents the same tool_call_id from
    // producing multiple tool_result blocks across the full conversation
    // history, which would cause Anthropic to reject the request.
    let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in rows {
        let role: String = row.try_get("role")?;
        let content: String = row.try_get("content")?;
        let parts: Value = row.try_get("parts")?;

        match role.as_str() {
            "user" => out.push(ChatMessage {
                role: "user".to_owned(),
                content: Some(Value::String(content)),
                name: None,
                tool_call_id: None,
                tool_calls: None,
            }),
            "assistant" => {
                let mut tool_calls: Vec<ToolCall> = Vec::new();
                let mut text_buf = String::new();
                if let Some(arr) = parts.as_array() {
                    for part in arr {
                        let kind = part.get("type").and_then(|v| v.as_str()).unwrap_or("");
                        match kind {
                            "text-delta" => {
                                if let Some(t) = part.get("text").and_then(|v| v.as_str()) {
                                    text_buf.push_str(t);
                                }
                            }
                            k if k.starts_with("tool-call") => {
                                let tool_call_id = part
                                    .get("toolCallId")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let tool_name =
                                    part.get("toolName").and_then(|v| v.as_str()).unwrap_or("");
                                let args = part.get("args").cloned().unwrap_or(json!({}));
                                if !tool_call_id.is_empty()
                                    && tool_calls.iter().all(|tc| tc.id != tool_call_id)
                                {
                                    tool_calls.push(ToolCall {
                                        id: tool_call_id.to_owned(),
                                        kind: "function".to_owned(),
                                        function: ToolCallFunction {
                                            name: tool_name.to_owned(),
                                            arguments: args.to_string(),
                                        },
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                }
                let final_text = if text_buf.is_empty() {
                    content
                } else {
                    text_buf
                };
                out.push(ChatMessage {
                    role: "assistant".to_owned(),
                    content: if final_text.is_empty() {
                        None
                    } else {
                        Some(Value::String(final_text))
                    },
                    name: None,
                    tool_call_id: None,
                    tool_calls: if tool_calls.is_empty() {
                        None
                    } else {
                        // Each assistant turn must be followed by tool messages
                        // for every tool_call. Emit those next.
                        Some(tool_calls.clone())
                    },
                });
                // Emit a tool message per tool call recovered — only for output-available
                // (each tool has multiple lifecycle parts: start, input-available, execute,
                // output-available/end — we must emit exactly one tool_result per tool_call_id)
                if !tool_calls.is_empty() {
                    if let Some(arr) = parts.as_array() {
                        for part in arr {
                            let kind = part.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            // Only emit once per tool_call_id, prefer output-available
                            if kind != "tool-call-output-available" && kind != "tool-call-output-error" {
                                continue;
                            }
                            let tool_call_id = part
                                .get("toolCallId")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            if tool_call_id.is_empty() || seen_ids.contains(tool_call_id) {
                                continue;
                            }
                            seen_ids.insert(tool_call_id.to_owned());
                            let tool_name =
                                part.get("toolName").and_then(|v| v.as_str()).unwrap_or("");
                            let result = part.get("result").cloned().unwrap_or(json!({}));
                            out.push(ChatMessage {
                                role: "tool".to_owned(),
                                content: Some(Value::String(result.to_string())),
                                name: Some(tool_name.to_owned()),
                                tool_call_id: Some(tool_call_id.to_owned()),
                                tool_calls: None,
                            });
                        }
                    }
                }
            }
            "tool" => {
                // Skip — tool messages are derived from the preceding assistant
                // message's parts (legacy rows would be tolerated this way).
                continue;
            }
            _ => continue,
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
async fn persist_message(
    db: &Pool<Postgres>,
    conversation_id: &Uuid,
    user_id: Option<&Uuid>,
    role: &str,
    content: &str,
    parts: &[Value],
    model: Option<&str>,
) -> Result<()> {
    let parts_json = Value::Array(parts.to_vec());
    sqlx::query(
        "insert into messages (id, conversation_id, user_id, role, content, parts, model) values ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(Uuid::now_v7())
    .bind(conversation_id)
    .bind(user_id)
    .bind(role)
    .bind(content)
    .bind(parts_json)
    .bind(model)
    .execute(db)
    .await
    .context("inserting message")?;
    Ok(())
}

async fn set_status(
    db: &Pool<Postgres>,
    run_id: RunId,
    status: RunStatus,
    last_error: Option<String>,
) -> Result<()> {
    let started_at = if matches!(status, RunStatus::Running) {
        Some(Utc::now())
    } else {
        None
    };
    let completed_at = if matches!(
        status,
        RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
    ) {
        Some(Utc::now())
    } else {
        None
    };
    sqlx::query(
        "update runs set status = $2,
             started_at = coalesce(started_at, $3),
             completed_at = coalesce(completed_at, $4),
             last_error = coalesce($5, last_error),
             updated_at = now()
         where id = $1",
    )
    .bind(run_id)
    .bind(status.as_str())
    .bind(started_at)
    .bind(completed_at)
    .bind(last_error)
    .execute(db)
    .await
    .context("updating run status")?;
    Ok(())
}

pub fn default_max_steps() -> usize {
    std::env::var("OPERON_AGENT_MAX_STEPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_STEPS)
}

/// Spawn a child subagent run inheriting the parent's provider/model/keys
/// and forward its event stream live into the parent run as
/// `subagent-stream-delta` and `subagent-progress` events. Blocks until the
/// child emits `message-end` (or fails / is cancelled).
///
/// Returns a JSON value containing `runId`, `agent`, `prompt`, `finalText`
/// and `logUrl` for the parent runner to record as the tool result.
async fn execute_subagent(
    parent_spec: &RunnerSpec,
    parent_handle: &RunHandle,
    parent_ctx: &tools::AgentContext,
    parent_tool_call_id: &str,
    agent_name: Option<&str>,
    prompt: &str,
) -> Result<Value> {
    if prompt.trim().is_empty() {
        anyhow::bail!("subagent prompt is required");
    }

    let child_run_id = Uuid::now_v7();
    let metadata = json!({
        "kind": "subagent",
        "agent": agent_name,
        "parentRunId": parent_spec.run_id.to_string(),
        "parentToolCallId": parent_tool_call_id,
    });

    sqlx::query(
        "insert into runs (id, conversation_id, user_id, status, model, parent_run_id, parent_tool_call_id, metadata) values ($1, $2, $3, 'queued', $4, $5, $6, $7)",
    )
    .bind(child_run_id)
    .bind(parent_spec.conversation_id)
    .bind(parent_spec.user_id)
    .bind(format!("{}:{}", parent_spec.provider, parent_spec.model))
    .bind(parent_spec.run_id)
    .bind(parent_tool_call_id)
    .bind(&metadata)
    .execute(&parent_spec.db)
    .await
    .context("inserting subagent child run row")?;

    let child_spec = RunnerSpec {
        run_id: child_run_id,
        user_id: parent_spec.user_id,
        conversation_id: parent_spec.conversation_id,
        provider: parent_spec.provider.clone(),
        model: parent_spec.model.clone(),
        openai_api_key: parent_spec.openai_api_key.clone(),
        base_url: parent_spec.base_url.clone(),
        workspace: parent_ctx.workspace.clone(),
        github_token: parent_ctx.github_token.clone(),
        initial_user_message: prompt.to_string(),
        initial_user_attachments: Vec::new(),
        db: parent_spec.db.clone(),
        max_steps: default_subagent_max_steps(),
        channel: parent_spec.channel.clone(),
        reasoning_level: parent_spec.reasoning_level.clone(),
        agents: parent_spec.agents.clone(),
    };

    let child_handle = spawn(child_spec);
    parent_spec.agents.insert(child_handle.clone());

    // Cancel the child if the parent is cancelled.
    let child_cancel = child_handle.cancel.clone();
    let parent_cancel = parent_handle.cancel.clone();
    let cancel_link = tokio::spawn(async move {
        parent_cancel.cancelled().await;
        child_cancel.cancel();
    });

    let mut rx = child_handle.subscribe();
    let mut final_text = String::new();
    let mut had_error: Option<String> = None;

    loop {
        match rx.recv().await {
            Ok(event) => {
                let frame = &event.frame;
                let event_type = frame
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let data = frame.get("data");
                match event_type {
                    "text-delta" => {
                        if let Some(text) =
                            data.and_then(|d| d.get("text")).and_then(Value::as_str)
                        {
                            final_text.push_str(text);
                            let _ = parent_handle
                                .emit(&events::subagent_stream_delta(
                                    parent_tool_call_id,
                                    agent_name,
                                    "text",
                                    text,
                                ))
                                .await;
                        }
                    }
                    "tool-call-update" | "tool-call-input-available" => {
                        if let Some(msg) = data
                            .and_then(|d| d.get("invocationMessage"))
                            .and_then(Value::as_str)
                        {
                            let _ = parent_handle
                                .emit(&events::subagent_progress(
                                    parent_tool_call_id,
                                    agent_name,
                                    msg,
                                    "active",
                                ))
                                .await;
                        }
                    }
                    "error" => {
                        if let Some(text) = data
                            .and_then(|d| d.get("errorText"))
                            .and_then(Value::as_str)
                        {
                            had_error = Some(text.to_string());
                            let _ = parent_handle
                                .emit(&events::subagent_progress(
                                    parent_tool_call_id,
                                    agent_name,
                                    text,
                                    "error",
                                ))
                                .await;
                        }
                    }
                    "message-end" => break,
                    _ => {}
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // We dropped some frames; keep going from the latest position.
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }

    cancel_link.abort();
    parent_spec.agents.remove(&child_run_id);

    if let Some(err) = had_error {
        return Ok(json!({
            "runId": child_run_id.to_string(),
            "agent": agent_name,
            "prompt": prompt,
            "finalText": final_text,
            "error": err,
            "logUrl": format!("/dashboard/sessions?runId={child_run_id}"),
        }));
    }

    Ok(json!({
        "runId": child_run_id.to_string(),
        "agent": agent_name,
        "prompt": prompt,
        "finalText": final_text,
        "logUrl": format!("/dashboard/sessions?runId={child_run_id}"),
    }))
}

/// Tighter step budget for subagent runs (runs created with a `parent_run_id`).
/// Override via `OPERON_SUBAGENT_MAX_STEPS`; defaults to 40 so a single child
/// can't burn through the parent's quota or loop indefinitely.
pub fn default_subagent_max_steps() -> usize {
    std::env::var("OPERON_SUBAGENT_MAX_STEPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SUBAGENT_MAX_STEPS)
}

pub async fn load_events_since(
    db: &Pool<Postgres>,
    run_id: RunId,
    since_sequence: i64,
) -> Result<Vec<AgentEvent>> {
    let rows = sqlx::query(
        "select sequence, payload from run_events where run_id = $1 and sequence > $2 order by sequence asc",
    )
    .bind(run_id)
    .bind(since_sequence)
    .fetch_all(db)
    .await
    .context("loading run_events")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let sequence: i64 = row.try_get("sequence")?;
        let payload: Value = row.try_get("payload")?;
        out.push(AgentEvent {
            sequence,
            frame: payload,
        });
    }
    Ok(out)
}

/// Auto-generate a short conversation title from the user's first message.
/// Truncates to ~60 chars and strips markdown/noise.
async fn auto_title_conversation(
    db: &Pool<Postgres>,
    conversation_id: &Uuid,
    user_message: &str,
) -> Result<()> {
    // Only auto-title if the current title is still the default
    let row = sqlx::query("select title from conversations where id = $1")
        .bind(conversation_id)
        .fetch_optional(db)
        .await?;
    let current_title: String = row
        .as_ref()
        .and_then(|r| r.try_get("title").ok())
        .unwrap_or_default();
    if !current_title.is_empty() && current_title != "New Chat" {
        return Ok(()); // User or system already set a meaningful title
    }

    // Generate title: first meaningful line, truncated
    let title = generate_title_from_prompt(user_message);
    if title.is_empty() {
        return Ok(());
    }

    sqlx::query("update conversations set title = $2, updated_at = now() where id = $1")
        .bind(conversation_id)
        .bind(&title)
        .execute(db)
        .await
        .context("updating conversation title")?;
    Ok(())
}

/// Build the content payload for the first user turn. If no attachments are
/// supplied this returns a plain JSON string (which OpenAI/Anthropic both
/// accept as the simplest message form). With attachments it returns a
/// structured parts array using OpenAI Chat Completions syntax
/// (`{"type":"text",...}` and `{"type":"image_url","image_url":{"url":...}}`).
/// The Anthropic transformer in [`super::anthropic`] translates this
/// shape into Anthropic's native image content blocks.
///
/// For non-image files (text, code, CSV, PDF, etc.), the file content is
/// fetched and included inline so the model can actually work with it.
fn build_initial_user_content(text: &str, attachments: &[AttachmentInput]) -> Value {
    if attachments.is_empty() {
        return Value::String(text.to_owned());
    }
    let mut parts: Vec<Value> = Vec::with_capacity(attachments.len() + 1);
    if !text.is_empty() {
        parts.push(json!({ "type": "text", "text": text }));
    }
    for att in attachments {
        let mime = att.mime_type.as_deref().unwrap_or("");
        if mime.starts_with("image/") {
            parts.push(json!({
                "type": "image_url",
                "image_url": { "url": att.url },
            }));
        } else {
            // Non-image attachments: include file content inline when possible.
            // Try to fetch the content from the URL so the model can read it.
            let label = att.name.as_deref().unwrap_or("file");
            let content = fetch_file_content_sync(&att.url);
            match content {
                Some(text_content) if text_content.len() <= 200_000 => {
                    parts.push(json!({
                        "type": "text",
                        "text": format!(
                            "--- File: {label} ({mime}) ---\n{text_content}\n--- End of {label} ---"
                        ),
                    }));
                }
                _ => {
                    // Fallback: just mention the file
                    parts.push(json!({
                        "type": "text",
                        "text": format!("[Attached file '{label}' ({mime}): {url}]",
                            label = label, mime = mime, url = att.url),
                    }));
                }
            }
        }
    }
    Value::Array(parts)
}

/// Attempt to read file content from a URL (blocking, best-effort).
/// Used to inline text file contents into the LLM context.
fn fetch_file_content_sync(url: &str) -> Option<String> {
    // For local uploads, read directly from disk
    if url.contains("/local-uploads/") {
        let filename = url.rsplit('/').next()?;
        let path = std::path::Path::new("./local_uploads").join(filename);
        return std::fs::read_to_string(&path).ok();
    }
    // For remote URLs, use a quick blocking fetch
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .ok()?;
    let resp = client.get(url).send().ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.text().ok()
}

/// Simple heuristic title generation from user prompt (no LLM call needed).
fn generate_title_from_prompt(prompt: &str) -> String {
    let cleaned = prompt
        .lines()
        .find(|l| !l.trim().is_empty())
        .unwrap_or(prompt)
        .trim()
        .trim_start_matches('#')
        .trim_start_matches('>')
        .trim_start_matches('-')
        .trim_start_matches('*')
        .trim_matches(|c| matches!(c, '"' | '\'' | '`'))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    // Truncate to ~50 chars at a word boundary
    if cleaned.chars().count() <= 50 {
        return cleaned;
    }
    let truncated: String = cleaned.chars().take(50).collect();
    if let Some(last_space) = truncated.rfind(' ') {
        format!("{}...", truncated[..last_space].trim_end())
    } else {
        format!("{}...", truncated.trim_end())
    }
}
