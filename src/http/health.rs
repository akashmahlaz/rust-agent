use axum::{Json, extract::State};
use serde::Serialize;
use sqlx::Row;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
}

pub async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn readyz(State(state): State<AppState>) -> AppResult<Json<HealthResponse>> {
    let row = sqlx::query("select 1 as ok").fetch_one(&state.db).await?;
    let ok: i32 = row.try_get("ok")?;

    if ok == 1 {
        Ok(Json(HealthResponse { status: "ready" }))
    } else {
        Err(AppError::ServiceUnavailable(
            "database readiness check failed".to_owned(),
        ))
    }
}
