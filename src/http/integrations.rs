use axum::{Json, extract::State, http::HeaderMap};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

#[derive(Deserialize)]
pub struct IntegrationAction {
    #[serde(default)]
    action: Option<String>,
    #[serde(default, rename = "phoneType")]
    phone_type: Option<String>,
    #[serde(default, rename = "dmPolicy")]
    dm_policy: Option<String>,
    #[serde(default, rename = "allowFrom")]
    allow_from: Vec<String>,
}

pub async fn whatsapp_status() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "connected": false })))
}

pub async fn telegram_status() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "connected": false })))
}

pub async fn whatsapp_onboarding() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({
        "phoneType": null,
        "dmPolicy": "pairing",
        "allowFrom": []
    })))
}

pub async fn whatsapp_action(Json(payload): Json<IntegrationAction>) -> AppResult<Json<serde_json::Value>> {
    let action = payload.action.as_deref().unwrap_or("status");
    let response = match action {
        "qr" => json!({
            "sessionId": "local-rust-session",
            "qrDataUrl": null,
            "message": "WhatsApp pairing is handled by the Rust backend. Configure the adapter process to enable QR pairing."
        }),
        "disconnect" => json!({ "ok": true, "connected": false }),
        "onboarding" => json!({
            "ok": true,
            "phoneType": payload.phone_type,
            "dmPolicy": payload.dm_policy.unwrap_or_else(|| "pairing".to_owned()),
            "allowFrom": payload.allow_from
        }),
        _ => json!({ "ok": true, "connected": false }),
    };
    Ok(Json(response))
}

pub async fn github_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<serde_json::Value>> {
    let token = super::token_from_request(&headers).ok_or(AppError::Unauthorized)?;
    let user_id = super::decode_claims_public(&state, token)?;
    let row = sqlx::query(
        "select provider_account_id, scopes, updated_at from oauth_accounts
             where user_id = $1 and provider = 'github'
             order by updated_at desc limit 1",
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?;
    match row {
        None => Ok(Json(json!({ "connected": false }))),
        Some(row) => {
            let account_id: String = row.try_get("provider_account_id")?;
            // Best-effort fetch of login from GitHub. Cheap call, cached on
            // their CDN. We swallow errors and just return the id.
            let login = fetch_github_login(&state, user_id).await.ok().flatten();
            Ok(Json(json!({
                "connected": true,
                "accountId": account_id,
                "login": login,
            })))
        }
    }
}

async fn fetch_github_login(state: &AppState, user_id: uuid::Uuid) -> AppResult<Option<String>> {
    let row = sqlx::query(
        "select access_token_ciphertext from oauth_accounts
             where user_id = $1 and provider = 'github'
             order by updated_at desc limit 1",
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?;
    let token = match row.and_then(|r| r.try_get::<Option<String>, _>("access_token_ciphertext").ok().flatten()) {
        Some(t) => t,
        None => return Ok(None),
    };
    let resp = reqwest::Client::new()
        .get("https://api.github.com/user")
        .header(reqwest::header::USER_AGENT, "operonx")
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .bearer_auth(token)
        .send()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("github user: {err}")))?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("github user: {err}")))?;
    Ok(body
        .get("login")
        .and_then(|v| v.as_str())
        .map(str::to_owned))
}
