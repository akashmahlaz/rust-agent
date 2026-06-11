use axum::{
    extract::{Query, State},
    http::HeaderMap,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct AppendLogRequest {
    #[serde(default, rename = "userId")]
    user_id: Option<Uuid>,
    level: String,
    source: String,
    message: String,
    #[serde(default)]
    metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct ListLogsQuery {
    #[serde(default, rename = "userId")]
    user_id: Option<Uuid>,
    #[serde(default = "default_limit")]
    limit: i64,
}

#[derive(Debug, Serialize)]
pub struct LogResponse {
    id: Uuid,
    #[serde(rename = "userId", skip_serializing_if = "Option::is_none")]
    user_id: Option<Uuid>,
    level: String,
    source: String,
    message: String,
    metadata: Value,
    #[serde(rename = "createdAt")]
    created_at: DateTime<Utc>,
}

fn default_limit() -> i64 {
    100
}

fn is_internal_request(state: &AppState, headers: &HeaderMap) -> bool {
    let Some(secret) = state.config.internal_secret.as_deref() else {
        return false;
    };
    headers
        .get("x-operon-internal-secret")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == secret)
}

fn require_user(state: &AppState, headers: &HeaderMap) -> AppResult<Uuid> {
    let token = super::token_from_request(headers).ok_or(AppError::Unauthorized)?;
    super::decode_claims_public(state, token)
}

fn validate_level(level: &str) -> AppResult<()> {
    match level {
        "info" | "warn" | "error" | "debug" => Ok(()),
        other => Err(AppError::BadRequest(format!("invalid log level: {other}"))),
    }
}

fn row_to_log(row: &sqlx::postgres::PgRow) -> AppResult<LogResponse> {
    Ok(LogResponse {
        id: row.try_get("id")?,
        user_id: row.try_get("user_id")?,
        level: row.try_get("level")?,
        source: row.try_get("source")?,
        message: row.try_get("message")?,
        metadata: row.try_get("metadata")?,
        created_at: row.try_get("created_at")?,
    })
}

pub async fn append_log(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<AppendLogRequest>,
) -> AppResult<Json<LogResponse>> {
    if !is_internal_request(&state, &headers) {
        return Err(AppError::Unauthorized);
    }

    validate_level(&payload.level)?;

    let row = sqlx::query(
        r#"
        insert into logs (id, user_id, level, source, message, metadata)
        values ($1, $2, $3, $4, $5, $6)
        returning id, user_id, level, source, message, metadata, created_at
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(payload.user_id)
    .bind(payload.level)
    .bind(payload.source)
    .bind(payload.message)
    .bind(if payload.metadata.is_null() { json!({}) } else { payload.metadata })
    .fetch_one(&state.db)
    .await?;

    Ok(Json(row_to_log(&row)?))
}

pub async fn list_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListLogsQuery>,
) -> AppResult<Json<Vec<LogResponse>>> {
    let is_internal = is_internal_request(&state, &headers);
    let authenticated_user = if is_internal {
        None
    } else {
        Some(require_user(&state, &headers)?)
    };
    let limit = query.limit.clamp(1, 500);
    let requested_user = query.user_id.or(authenticated_user);

    let rows = if let Some(user_id) = requested_user {
        sqlx::query(
            r#"
            select id, user_id, level, source, message, metadata, created_at
            from logs
            where user_id = $1
            order by created_at desc
            limit $2
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&state.db)
        .await?
    } else if is_internal {
        sqlx::query(
            r#"
            select id, user_id, level, source, message, metadata, created_at
            from logs
            order by created_at desc
            limit $1
            "#,
        )
        .bind(limit)
        .fetch_all(&state.db)
        .await?
    } else {
        return Err(AppError::Unauthorized);
    };

    let logs = rows
        .iter()
        .map(row_to_log)
        .collect::<AppResult<Vec<_>>>()?;
    Ok(Json(logs))
}