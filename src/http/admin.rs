use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

#[derive(Deserialize)]
pub struct AgentUpdate {
    #[serde(default)]
    enabled: Option<bool>,
}

pub async fn usage_summary() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({
        "summary": {
            "totalRequests": 0,
            "totalTokens": 0,
            "totalPromptTokens": 0,
            "totalCompletionTokens": 0,
            "totalCost": 0,
            "avgDuration": 0,
            "totalToolCalls": 0,
            "errorCount": 0
        },
        "daily": []
    })))
}

/// GET /admin/logs — most-recent 100 runs for the calling user, with the
/// captured provider request id and last error (if any). The frontend Logs
/// page renders this as a live tail; correlation with provider dashboards
/// is the main use case.
pub async fn logs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<serde_json::Value>> {
    let token = super::token_from_request(&headers).ok_or(AppError::Unauthorized)?;
    let user_id = super::decode_claims_public(&state, token)?;

    let rows = sqlx::query(
        "select id, conversation_id, status, model, provider_request_id, last_error, \
                started_at, completed_at, created_at, updated_at \
         from runs where user_id = $1 order by created_at desc limit 100",
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let logs: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            let id: uuid::Uuid = r.try_get("id").unwrap_or_default();
            let conv: uuid::Uuid = r.try_get("conversation_id").unwrap_or_default();
            let status: String = r.try_get("status").unwrap_or_default();
            let model: String = r.try_get("model").unwrap_or_default();
            let req_id: Option<String> = r.try_get("provider_request_id").ok().flatten();
            let last_error: Option<String> = r.try_get("last_error").ok().flatten();
            let started: Option<chrono::DateTime<chrono::Utc>> =
                r.try_get("started_at").ok().flatten();
            let completed: Option<chrono::DateTime<chrono::Utc>> =
                r.try_get("completed_at").ok().flatten();
            let created: chrono::DateTime<chrono::Utc> = r
                .try_get("created_at")
                .unwrap_or_else(|_| chrono::Utc::now());
            json!({
                "runId": id,
                "conversationId": conv,
                "status": status,
                "model": model,
                "requestId": req_id,
                "lastError": last_error,
                "startedAt": started,
                "completedAt": completed,
                "createdAt": created,
            })
        })
        .collect();

    Ok(Json(json!({ "logs": logs })))
}

pub async fn agents() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({
        "agents": [{
            "id": "operon-rust-agent",
            "name": "Operon Rust Agent",
            "description": "Long-running coding agent served by operonx",
            "tools": ["shell", "files", "web"],
            "enabled": true
        }]
    })))
}

pub async fn update_agent(Path(_id): Path<String>, Json(payload): Json<AgentUpdate>) -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "ok": true, "enabled": payload.enabled.unwrap_or(true) })))
}

pub async fn delete_agent(Path(_id): Path<String>) -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "ok": true })))
}
