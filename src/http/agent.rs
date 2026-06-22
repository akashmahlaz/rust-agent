//! HTTP routes for the coding agent.
//!
//!   POST /agent/runs          → create+spawn a run
//!   GET  /agent/runs/:id/sse  → SSE stream (replay-then-tail)
//!   POST /agent/runs/:id/cancel → request cancellation
//!
//! Auth: same JWT cookie/bearer as the rest of the API.

use std::convert::Infallible;
use std::path::PathBuf;
use std::time::Duration;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use chrono::Utc;
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;
use uuid::Uuid;

use crate::{
    agent::{
        runner::{self, RunnerSpec},
        tools::Workspace,
        types::{RunRequest, RunStatus},
    },
    http::error::{AppError, AppResult},
    state::AppState,
};

#[derive(Serialize)]
pub struct CreateRunResponse {
    pub run_id: Uuid,
    pub conversation_id: Uuid,
    pub status: &'static str,
    pub model: String,
}

pub async fn create_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<RunRequest>,
) -> AppResult<Json<CreateRunResponse>> {
    let user_id = require_user(&state, &headers)?;

    let model_full = payload
        .model
        .clone()
        .unwrap_or_else(|| state.config.default_agent_model.clone());

    // Split "provider/model-id" → (provider, model_id_only).
    // If there's no slash the entire string is treated as an OpenAI model.
    let (provider, model_id) = if let Some(slash) = model_full.find('/') {
        let p = model_full[..slash].to_owned();
        let m = model_full[slash + 1..].to_owned();
        (p, m)
    } else {
        ("openai".to_owned(), model_full.clone())
    };

    // Resolve API key: prefer stored profile key, fall back to env OPENAI_API_KEY.
    let api_key: String = {
        use sqlx::Row;
        let row = sqlx::query(
            "select api_key_ciphertext from provider_profiles where user_id = $1 and provider = $2",
        )
        .bind(user_id)
        .bind(&provider)
        .fetch_optional(&state.db)
        .await?;

        if let Some(r) = row {
            r.try_get::<Option<String>, _>("api_key_ciphertext")?
                .filter(|k| !k.is_empty())
                .or_else(|| state.config.openai_api_key.clone())
                .ok_or_else(|| {
                    AppError::ServiceUnavailable(format!("no API key for provider '{provider}'"))
                })?
        } else {
            state.config.openai_api_key.clone().ok_or_else(|| {
                AppError::ServiceUnavailable(format!("no API key for provider '{provider}'"))
            })?
        }
    };

    let base_url = provider_base_url(&provider).to_owned();

    let prompt = payload.prompt.trim();
    if prompt.is_empty() {
        return Err(AppError::BadRequest("prompt is required".into()));
    }
    let channel = payload
        .channel
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("coding");

    let conversation_id = match payload.conversation_id {
        Some(id) => {
            // verify the user owns it, and update the title to the first prompt
            // if it still has the default "New Chat" title (front-end creates
            // the conversation document before sending the first message).
            let row = sqlx::query("select user_id, title from conversations where id = $1")
                .bind(id)
                .fetch_optional(&state.db)
                .await?;
            match row {
                Some(r) => {
                    use sqlx::Row;
                    let owner: Uuid = r.try_get("user_id")?;
                    if owner != user_id {
                        return Err(AppError::Unauthorized);
                    }
                    let current_title: String = r.try_get("title")?;
                    if current_title == "New Chat" {
                        sqlx::query(
                            "update conversations set title = $2, updated_at = now() where id = $1",
                        )
                        .bind(id)
                        .bind(truncate_title(prompt))
                        .execute(&state.db)
                        .await?;
                    }
                    id
                }
                None => return Err(AppError::BadRequest("unknown conversation".into())),
            }
        }
        None => {
            let id = Uuid::now_v7();
            sqlx::query(
                "insert into conversations (id, user_id, title, channel) values ($1, $2, $3, $4)",
            )
            .bind(id)
            .bind(user_id)
            .bind(truncate_title(prompt))
            .bind(channel)
            .execute(&state.db)
            .await?;
            id
        }
    };

    let run_id = Uuid::now_v7();
    let run_metadata = build_run_metadata(&payload);
    sqlx::query(
        "insert into runs (id, conversation_id, user_id, status, model, parent_run_id, parent_request_id, parent_tool_call_id, metadata) values ($1, $2, $3, 'queued', $4, $5, $6, $7, $8)",
    )
    .bind(run_id)
    .bind(conversation_id)
    .bind(user_id)
    .bind(&model_full)
    .bind(payload.parent_run_id)
    .bind(payload.parent_request_id.as_deref())
    .bind(payload.parent_tool_call_id.as_deref())
    .bind(run_metadata)
    .execute(&state.db)
    .await?;

    // persist the initial user message
    sqlx::query(
        "insert into messages (id, conversation_id, user_id, role, content, parts) values ($1, $2, $3, 'user', $4, $5)",
    )
    .bind(Uuid::now_v7())
    .bind(conversation_id)
    .bind(user_id)
    .bind(prompt)
    .bind(json!([{ "type": "text-delta", "text": prompt }, { "type": "text-end", "text": "" }]))
    .execute(&state.db)
    .await?;

    let workspace_root = workspace_path_for(&state, &run_id, payload.workspace.as_deref())?;
    let workspace = Workspace::new(workspace_root)
        .map_err(|e| AppError::ServiceUnavailable(format!("workspace: {e}")))?;

    // Best-effort GitHub token lookup. If the user hasn't connected GitHub
    // we leave it None and the GitHub tools return a clear error to the model.
    let github_token: Option<String> = {
        use sqlx::Row;
        let row = sqlx::query(
            "select access_token_ciphertext from oauth_accounts where user_id = $1 and provider = 'github' order by updated_at desc limit 1",
        )
        .bind(user_id)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
        row.and_then(|r| {
            r.try_get::<Option<String>, _>("access_token_ciphertext")
                .ok()
                .flatten()
        })
        .filter(|t| !t.is_empty())
    };

    let handle = runner::spawn(RunnerSpec {
        run_id,
        user_id,
        conversation_id,
        provider: provider.clone(),
        model: model_id,
        openai_api_key: api_key,
        base_url,
        workspace,
        github_token,
        initial_user_message: prompt.to_owned(),
        initial_user_attachments: payload.attachments.clone().unwrap_or_default(),
        db: state.db.clone(),
        // Child runs (spawned via subagent tool) get an isolated, tighter
        // step budget so a runaway subagent can't exhaust the parent
        // agent's quota or loop indefinitely.
        max_steps: if payload.parent_run_id.is_some() {
            runner::default_subagent_max_steps()
        } else {
            runner::default_max_steps()
        },
        channel: channel.to_owned(),
        reasoning_level: payload.reasoning_level.clone(),
        agents: state.agents.clone(),
        providers: state.providers.clone(),
    });
    state.agents.insert(handle);

    Ok(Json(CreateRunResponse {
        run_id,
        conversation_id,
        status: RunStatus::Running.as_str(),
        model: model_full,
    }))
}

pub(crate) fn provider_base_url(provider: &str) -> &'static str {
    match provider {
        "openrouter" => "https://openrouter.ai/api/v1",
        "groq" => "https://api.groq.com/openai/v1",
        "deepseek" => "https://api.deepseek.com/v1",
        "xai" => "https://api.x.ai/v1",
        "mistral" => "https://api.mistral.ai/v1",
        "github" => "https://models.inference.ai.azure.com",
        "minimax" => "https://api.minimax.io/v1",
        "anthropic" => "https://api.anthropic.com/v1",
        "google" => "https://generativelanguage.googleapis.com/v1beta/openai",
        _ => "https://api.openai.com/v1",
    }
}

#[derive(Deserialize, Default)]
pub struct SseQuery {
    #[serde(default)]
    last_seq: Option<i64>,
}

pub async fn sse_run(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    Query(query): Query<SseQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, Response> {
    let user_id = match require_user_with_query(&state, &headers) {
        Ok(id) => id,
        Err(err) => return Err(err.into_response()),
    };

    let owner_check = sqlx::query("select user_id, status from runs where id = $1")
        .bind(run_id)
        .fetch_optional(&state.db)
        .await
        .map_err(|e| AppError::from(e).into_response())?;

    let (owner, status): (Uuid, String) = match owner_check {
        Some(row) => {
            use sqlx::Row;
            (
                row.try_get("user_id")
                    .map_err(|e| AppError::from(e).into_response())?,
                row.try_get("status")
                    .map_err(|e| AppError::from(e).into_response())?,
            )
        }
        None => return Err(AppError::BadRequest("unknown run".into()).into_response()),
    };
    if owner != user_id {
        return Err(AppError::Unauthorized.into_response());
    }

    let since = query.last_seq.unwrap_or(0);
    let replay = runner::load_events_since(&state.db, run_id, since)
        .await
        .map_err(|e| AppError::ServiceUnavailable(format!("load events: {e}")).into_response())?;

    let live = state.agents.get(&run_id);
    let is_terminal = matches!(status.as_str(), "completed" | "failed" | "cancelled");

    let replay_stream = stream::iter(replay.into_iter().map(|event| Ok(to_sse_event(event))));

    let combined: futures::stream::BoxStream<'static, Result<Event, Infallible>> =
        if let Some(handle) = live {
            let receiver = handle.subscribe();
            // Stop the SSE stream after a terminal frame (`message-end` or
            // `run-completed`). Without this the keep-alive pings keep the
            // body open forever and the client stays stuck in `streaming`.
            let live_stream = tokio_stream::wrappers::BroadcastStream::new(receiver).filter_map(
                |item| async move {
                    match item {
                        Ok(event) => Some(Ok::<_, Infallible>(event)),
                        Err(_) => None,
                    }
                },
            );
            let mut stopped = false;
            let live_terminating = live_stream.take_while(move |item| {
                if stopped {
                    return futures::future::ready(false);
                }
                if let Ok(ev) = item {
                    let kind = ev.frame.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if matches!(
                        kind,
                        "message-end" | "run-completed" | "run-failed" | "run-cancelled"
                    ) {
                        stopped = true;
                    }
                }
                futures::future::ready(true)
            });
            let mapped = live_terminating.map(|item| item.map(to_sse_event));
            // After the live stream ends, append a synthetic `done` so the
            // browser's reader receives an EOF promptly.
            let done = stream::iter(std::iter::once(Ok(Event::default()
                .event("done")
                .data("{\"type\":\"done\"}"))));
            replay_stream.chain(mapped).chain(done).boxed()
        } else if is_terminal {
            // emit a synthetic done so the client closes cleanly
            let done = stream::iter(std::iter::once(Ok(Event::default()
                .event("done")
                .data("{\"type\":\"done\"}"))));
            replay_stream.chain(done).boxed()
        } else {
            replay_stream.boxed()
        };

    Ok(Sse::new(combined).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    ))
}

pub async fn cancel_run(
    State(state): State<AppState>,
    Path(run_id): Path<Uuid>,
    headers: HeaderMap,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = require_user(&state, &headers)?;

    let row = sqlx::query("select user_id from runs where id = $1")
        .bind(run_id)
        .fetch_optional(&state.db)
        .await?
        .ok_or_else(|| AppError::BadRequest("unknown run".into()))?;
    use sqlx::Row;
    let owner: Uuid = row.try_get("user_id")?;
    if owner != user_id {
        return Err(AppError::Unauthorized);
    }

    if let Some(handle) = state.agents.get(&run_id) {
        handle.cancel();
    }
    state.agents.remove(&run_id);

    sqlx::query(
        "update runs set status = 'cancelled', completed_at = $2, updated_at = now()
         where id = $1 and status in ('queued','running','paused')",
    )
    .bind(run_id)
    .bind(Utc::now())
    .execute(&state.db)
    .await?;

    Ok(Json(json!({ "ok": true })))
}

fn to_sse_event(event: crate::agent::types::AgentEvent) -> Event {
    Event::default()
        .id(event.sequence.to_string())
        .data(event.frame.to_string())
}

fn truncate_title(text: &str) -> String {
    let title = title_from_prompt(text, 80);
    if title.is_empty() {
        "New Chat".to_owned()
    } else {
        title
    }
}

fn title_from_prompt(text: &str, max_chars: usize) -> String {
    let cleaned = text
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(text)
        .trim()
        .trim_start_matches('#')
        .trim_start_matches('>')
        .trim_start_matches('-')
        .trim_start_matches('*')
        .trim_matches(|c| matches!(c, '"' | '\'' | '`'))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if cleaned.chars().count() <= max_chars {
        cleaned
    } else {
        let prefix: String = cleaned.chars().take(max_chars).collect();
        match prefix.rfind(' ') {
            Some(idx) if idx > 0 => format!("{}...", prefix[..idx].trim_end()),
            _ => format!("{}...", prefix.trim_end()),
        }
    }
}

fn build_run_metadata(payload: &RunRequest) -> Value {
    let mut metadata = payload.metadata.clone().unwrap_or_else(|| json!({}));
    if !metadata.is_object() {
        metadata = json!({ "value": metadata });
    }
    if let Some(object) = metadata.as_object_mut() {
        if let Some(parent_run_id) = payload.parent_run_id {
            object.insert("parentRunId".to_owned(), json!(parent_run_id));
        }
        if let Some(parent_request_id) = payload.parent_request_id.as_deref() {
            object.insert("parentRequestId".to_owned(), json!(parent_request_id));
        }
        if let Some(parent_tool_call_id) = payload.parent_tool_call_id.as_deref() {
            object.insert("parentToolCallId".to_owned(), json!(parent_tool_call_id));
        }
    }
    metadata
}

fn workspace_path_for(
    state: &AppState,
    run_id: &Uuid,
    override_path: Option<&str>,
) -> AppResult<PathBuf> {
    if let Some(custom) = override_path {
        let p = PathBuf::from(custom);
        if !p.is_absolute() {
            return Err(AppError::BadRequest("workspace must be absolute".into()));
        }
        return Ok(p);
    }
    Ok(state.config.workspace_root.join(run_id.to_string()))
}

fn require_user(state: &AppState, headers: &HeaderMap) -> AppResult<Uuid> {
    require_user_with_query(state, headers)
}

fn require_user_with_query(
    state: &AppState,
    headers: &HeaderMap,
) -> AppResult<Uuid> {
    let token = super::token_from_request(headers).ok_or(AppError::Unauthorized)?;
    super::decode_claims_public(state, token)
}
