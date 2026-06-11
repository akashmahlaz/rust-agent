use axum::{
    extract::{Query, State},
    http::HeaderMap,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

const GRAPH_API: &str = "https://graph.facebook.com/v21.0";

fn require_user(state: &AppState, headers: &HeaderMap) -> AppResult<Uuid> {
    let token = super::token_from_request(headers).ok_or(AppError::Unauthorized)?;
    super::decode_claims_public(state, token)
}

async fn meta_fetch(client: &reqwest::Client, token: &str, path: &str) -> AppResult<Value> {
    let url = if path.starts_with("http") {
        path.to_owned()
    } else {
        format!("{GRAPH_API}{path}")
    };
    let sep = if url.contains('?') { '&' } else { '?' };
    let full_url = format!("{url}{sep}access_token={}", urlencoding::encode(token));
    tracing::debug!(path = %path, "meta_fetch_start");
    let res = client
        .get(full_url)
        .send()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("meta request: {err}")))?;
    let status = res.status();
    let body = res
        .text()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("meta body: {err}")))?;
    if !status.is_success() {
        tracing::warn!(status = %status, body = %body, path = %path, "meta_fetch_failed");
        return Err(AppError::ServiceUnavailable(format!("Meta Graph API {status}: {body}")));
    }
    serde_json::from_str(&body)
        .map_err(|err| AppError::ServiceUnavailable(format!("meta json parse: {err}")))
}

async fn resolve_meta_token(state: &AppState, user_id: Uuid) -> AppResult<Option<String>> {
    let row = sqlx::query(
        "select encrypted_oauth_token from auth_profiles where user_id = $1 and provider = 'meta' order by updated_at desc limit 1",
    )
    .bind(user_id)
    .fetch_optional(&state.db)
    .await?;

    let Some(row) = row else { return Ok(None); };
    let encrypted: Option<String> = row.try_get("encrypted_oauth_token")?;
    let Some(encrypted) = encrypted.filter(|v| !v.is_empty()) else { return Ok(None); };
    let token = crate::tools::decrypt_token(&encrypted)
        .map_err(|err| AppError::ServiceUnavailable(format!("decrypt meta token: {err}")))?;
    Ok(Some(token))
}

fn token_ref(token: &str) -> String {
    if token.len() <= 10 {
        format!("{}...{}", &token[..token.len().min(2)], &token[token.len().saturating_sub(2)..])
    } else {
        format!("{}...{}", &token[..6], &token[token.len() - 4..])
    }
}

#[derive(Deserialize)]
pub struct ConnectRequest {
    token: String,
}

pub async fn connect(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConnectRequest>,
) -> AppResult<Json<Value>> {
    let user_id = require_user(&state, &headers)?;
    let token = payload.token.trim();
    if token.is_empty() {
        return Err(AppError::BadRequest("token is required".into()));
    }
    let client = reqwest::Client::new();
    let me = meta_fetch(&client, token, "/me?fields=id,name").await?;

    tracing::info!(user_id = %user_id, meta_user = ?me.get("id"), "meta_connect_success");
    // Store as legacy plaintext-compatible value for now; decrypt_token returns
    // non-v1 strings unchanged. Provider profile keys elsewhere in this Rust
    // backend are currently stored the same way despite the *_ciphertext name.
    sqlx::query(
        r#"
        insert into auth_profiles (
            id, user_id, provider, type,
            encrypted_oauth_token, token_ref, metadata
        ) values ($1, $2, 'meta', 'oauth', $3, $4, $5)
        on conflict (user_id, provider, type) do update set
            encrypted_oauth_token = excluded.encrypted_oauth_token,
            token_ref = excluded.token_ref,
            metadata = excluded.metadata,
            updated_at = now()
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(user_id)
    .bind(token)
    .bind(token_ref(token))
    .bind(json!({ "metaUser": me }))
    .execute(&state.db)
    .await?;

    Ok(Json(json!({ "ok": true, "user": me })))
}

pub async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<Value>> {
    let user_id = require_user(&state, &headers)?;
    let Some(token) = resolve_meta_token(&state, user_id).await? else {
        return Ok(Json(json!({ "connected": false })));
    };
    let client = reqwest::Client::new();
    let me = match meta_fetch(&client, &token, "/me?fields=id,name").await {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(user_id = %user_id, error = %err, "meta_status_validation_failed");
            return Ok(Json(json!({ "connected": false, "error": err.to_string() })));
        }
    };
    let accounts = meta_fetch(
        &client,
        &token,
        "/me/adaccounts?fields=id,account_id,name,currency,account_status&limit=50",
    )
    .await
    .unwrap_or_else(|_| json!({ "data": [] }));
    let ad_accounts = accounts.get("data").cloned().unwrap_or_else(|| json!([]));
    let count = ad_accounts.as_array().map(|items| items.len()).unwrap_or(0);
    Ok(Json(json!({
        "connected": true,
        "user": me,
        "adAccountsCount": count,
        "adAccounts": ad_accounts,
    })))
}

#[derive(Deserialize)]
pub struct CampaignsQuery {
    #[serde(rename = "adAccountId")]
    ad_account_id: String,
    #[serde(default)]
    limit: Option<u32>,
}

pub async fn campaigns(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<CampaignsQuery>,
) -> AppResult<Json<Value>> {
    let user_id = require_user(&state, &headers)?;
    let token = resolve_meta_token(&state, user_id)
        .await?
        .ok_or_else(|| AppError::Unauthorized)?;
    let limit = query.limit.unwrap_or(25).clamp(1, 100);
    let path = format!(
        "/{}/campaigns?fields=id,name,objective,status,daily_budget,lifetime_budget,created_time,updated_time&limit={}",
        query.ad_account_id, limit
    );
    let value = meta_fetch(&reqwest::Client::new(), &token, &path).await?;
    Ok(Json(json!({ "campaigns": value.get("data").cloned().unwrap_or_else(|| json!([])) })))
}

#[derive(Deserialize)]
pub struct InsightsQuery {
    #[serde(rename = "campaignId")]
    campaign_id: String,
    #[serde(default, rename = "datePreset")]
    date_preset: Option<String>,
}

pub async fn insights(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InsightsQuery>,
) -> AppResult<Json<Value>> {
    let user_id = require_user(&state, &headers)?;
    let token = resolve_meta_token(&state, user_id)
        .await?
        .ok_or_else(|| AppError::Unauthorized)?;
    let preset = query.date_preset.unwrap_or_else(|| "last_7d".to_owned());
    let path = format!(
        "/{}/insights?fields=impressions,clicks,spend,cpc,cpm,ctr,reach&date_preset={}",
        query.campaign_id, preset
    );
    let value = meta_fetch(&reqwest::Client::new(), &token, &path).await?;
    Ok(Json(json!({ "insights": value.get("data").cloned().unwrap_or_else(|| json!([])) })))
}

#[derive(Deserialize)]
pub struct CampaignActionRequest {
    #[serde(rename = "campaignId")]
    campaign_id: String,
    action: String,
}

pub async fn campaign_action(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<CampaignActionRequest>,
) -> AppResult<Json<Value>> {
    let user_id = require_user(&state, &headers)?;
    let token = resolve_meta_token(&state, user_id)
        .await?
        .ok_or_else(|| AppError::Unauthorized)?;
    let status = match payload.action.as_str() {
        "pause" => "PAUSED",
        "resume" => "ACTIVE",
        _ => return Err(AppError::BadRequest("action must be pause or resume".into())),
    };
    let url = format!("{GRAPH_API}/{}", payload.campaign_id);
    let res = reqwest::Client::new()
        .post(url)
        .form(&[("status", status), ("access_token", token.as_str())])
        .send()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("meta campaign action: {err}")))?;
    let http_status = res.status();
    let body = res.text().await.unwrap_or_default();
    if !http_status.is_success() {
        tracing::warn!(status = %http_status, body = %body, "meta_campaign_action_failed");
        return Err(AppError::ServiceUnavailable(format!("Meta campaign action {http_status}: {body}")));
    }
    tracing::info!(user_id = %user_id, campaign_id = %payload.campaign_id, action = %payload.action, "meta_campaign_action_done");
    Ok(Json(json!({ "ok": true })))
}
