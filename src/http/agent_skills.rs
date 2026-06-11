use axum::{extract::State, http::HeaderMap, Json};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

fn require_user(state: &AppState, headers: &HeaderMap) -> AppResult<Uuid> {
    let token = super::token_from_request(headers).ok_or(AppError::Unauthorized)?;
    super::decode_claims_public(state, token)
}

pub async fn list_agent_skills(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = require_user(&state, &headers)?;
    tracing::debug!(user_id = %user_id, "agent_skills_list_start");

    let rows = sqlx::query(
        r#"
        select id, name, description, trigger, tags, steps,
               invocation_count, success_count, failure_count,
               last_used_at, created_at, updated_at
        from agent_skills
        where user_id = $1
        order by updated_at desc
        limit 200
        "#,
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let skills = rows
        .into_iter()
        .map(|row| {
            let id: Uuid = row.try_get("id")?;
            let name: String = row.try_get("name")?;
            let description: Option<String> = row.try_get("description")?;
            let trigger: Option<String> = row.try_get("trigger")?;
            let tags: Option<Vec<String>> = row.try_get("tags")?;
            let steps: serde_json::Value = row.try_get("steps")?;
            let invocation_count: i32 = row.try_get("invocation_count")?;
            let success_count: i32 = row.try_get("success_count")?;
            let failure_count: i32 = row.try_get("failure_count")?;
            let last_used_at: Option<chrono::DateTime<chrono::Utc>> = row.try_get("last_used_at")?;
            let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
            let updated_at: chrono::DateTime<chrono::Utc> = row.try_get("updated_at")?;

            Ok::<_, sqlx::Error>(json!({
                "id": id,
                "name": name,
                "description": description.unwrap_or_default(),
                "trigger": trigger.unwrap_or_default(),
                "tags": tags.unwrap_or_default(),
                "steps": steps.as_array().cloned().unwrap_or_default(),
                "invocationCount": invocation_count,
                "successCount": success_count,
                "failureCount": failure_count,
                "lastUsedAt": last_used_at.map(|dt| dt.to_rfc3339()),
                "createdAt": created_at.to_rfc3339(),
                "updatedAt": updated_at.to_rfc3339(),
            }))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;

    tracing::debug!(user_id = %user_id, count = skills.len(), "agent_skills_list_done");
    Ok(Json(json!({ "skills": skills })))
}
