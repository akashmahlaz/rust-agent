use axum::{Json, extract::{Path, State}, http::HeaderMap};
use serde::Deserialize;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::{http::error::{AppError, AppResult}, state::AppState};

#[derive(Deserialize)]
pub struct ProviderRequest {
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default, rename = "baseUrl")]
    base_url: Option<String>,
    #[serde(default, rename = "apiKey")]
    api_key: Option<String>,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
pub struct WorkspaceFileRequest {
    kind: String,
    content: String,
}

fn require_user(state: &AppState, headers: &HeaderMap) -> AppResult<Uuid> {
    let token = super::token_from_request(headers).ok_or(AppError::Unauthorized)?;
    super::decode_claims_public(state, token)
}

pub async fn providers(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = require_user(&state, &headers)?;
    let rows = sqlx::query(
        "select id, provider, models, default_model, updated_at from provider_profiles where user_id = $1 order by updated_at desc",
    )
    .bind(user_id)
    .fetch_all(&state.db)
    .await?;

    let profiles: Vec<_> = rows
        .iter()
        .map(|row| {
            let provider: String = row.try_get("provider")?;
            let profile_id: Uuid = row.try_get("id")?;
            let models: serde_json::Value = row.try_get("models")?;
            let default_model: Option<String> = row.try_get("default_model")?;
            let updated_at: chrono::DateTime<chrono::Utc> = row.try_get("updated_at")?;
            Ok(json!({
                "profileId": profile_id,
                "provider": provider,
                "tokenRef": format!("rust:{}", provider),
                "models": models.as_array().cloned().unwrap_or_default(),
                "defaultModel": default_model,
                "updatedAt": updated_at,
            }))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;

    let default_model = profiles
        .iter()
        .find_map(|profile| profile.get("defaultModel").and_then(|v| v.as_str()).map(str::to_owned))
        .or_else(|| {
            profiles.first().and_then(|profile| {
                let provider = profile.get("provider")?.as_str()?;
                let model = profile.get("models")?.as_array()?.first()?.as_str()?;
                Some(format!("{provider}/{model}"))
            })
        })
        .unwrap_or_else(|| "openai/gpt-4o-mini".to_owned());

    let recent_provider_id = profiles
        .first()
        .and_then(|profile| profile.get("provider"))
        .and_then(|provider| provider.as_str())
        .unwrap_or("openai");

    Ok(Json(json!({
        "providers": [],
        "profiles": profiles,
        "defaultModel": default_model,
        "recentProviderId": recent_provider_id
    })))
}

pub async fn update_provider(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ProviderRequest>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = require_user(&state, &headers)?;
    let provider = payload.provider.unwrap_or_else(|| "openai".to_owned());
    let action = payload.action.unwrap_or_else(|| "connect".to_owned());

    if action == "set-default" {
        let model = payload.model.ok_or_else(|| AppError::BadRequest("model is required".into()))?;
        let provider_for_model = model.split('/').next().unwrap_or(&provider).to_owned();
        sqlx::query("update provider_profiles set default_model = null where user_id = $1")
            .bind(user_id)
            .execute(&state.db)
            .await?;
        sqlx::query(
            "update provider_profiles set default_model = $1, updated_at = now() where user_id = $2 and provider = $3",
        )
        .bind(&model)
        .bind(user_id)
        .bind(&provider_for_model)
        .execute(&state.db)
        .await?;
        return Ok(Json(json!({ "ok": true, "defaultModel": model })));
    }

    let api_key = if let Some(key) = payload.api_key.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        key.to_owned()
    } else {
        let row = sqlx::query("select api_key_ciphertext from provider_profiles where user_id = $1 and provider = $2")
            .bind(user_id)
            .bind(&provider)
            .fetch_optional(&state.db)
            .await?
            .ok_or_else(|| AppError::BadRequest("provider is not connected".into()))?;
        row.try_get::<Option<String>, _>("api_key_ciphertext")?
            .ok_or_else(|| AppError::BadRequest("provider has no saved key".into()))?
    };

    let model_ids = fetch_models(&provider, &api_key, payload.base_url.as_deref()).await?;
    let default_model = payload
        .model
        .unwrap_or_else(|| model_ids.first().map(|model| format!("{provider}/{model}")).unwrap_or_else(|| format!("{provider}/manual")));
    let models_json = json!(model_ids);

    let row = sqlx::query(
        "insert into provider_profiles (id, user_id, provider, api_key_ciphertext, models, default_model)
             values ($1, $2, $3, $4, $5, $6)
             on conflict (user_id, provider) do update
                 set api_key_ciphertext = coalesce(excluded.api_key_ciphertext, provider_profiles.api_key_ciphertext),
                     models = excluded.models,
                     default_model = coalesce(provider_profiles.default_model, excluded.default_model),
                     updated_at = now()
             returning id, provider, models, default_model, updated_at",
    )
    .bind(Uuid::now_v7())
    .bind(user_id)
    .bind(&provider)
    .bind(Some(api_key))
    .bind(&models_json)
    .bind(&default_model)
    .fetch_one(&state.db)
    .await?;

    let profile_id: Uuid = row.try_get("id")?;
    let persisted_models: serde_json::Value = row.try_get("models")?;
    let persisted_default: Option<String> = row.try_get("default_model")?;
    let updated_at: chrono::DateTime<chrono::Utc> = row.try_get("updated_at")?;
    let models = persisted_models.as_array().cloned().unwrap_or_default();
    Ok(Json(json!({
        "ok": true,
        "models": models.iter().map(|id| json!({ "id": id })).collect::<Vec<_>>(),
        "source": "api",
        "defaultModel": persisted_default.clone().unwrap_or(default_model),
        "profile": {
            "profileId": profile_id,
            "provider": provider,
            "tokenRef": format!("rust:{}", provider),
            "baseUrl": payload.base_url,
            "models": persisted_models,
            "defaultModel": persisted_default,
            "updatedAt": updated_at
        }
    })))
}

pub async fn delete_provider_profile(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((provider, _profile_id)): Path<(String, String)>,
) -> AppResult<Json<serde_json::Value>> {
    let user_id = require_user(&state, &headers)?;
    sqlx::query("delete from provider_profiles where user_id = $1 and provider = $2")
        .bind(user_id)
        .bind(provider)
        .execute(&state.db)
        .await?;
    Ok(Json(json!({ "ok": true })))
}

async fn fetch_models(provider: &str, api_key: &str, base_url: Option<&str>) -> AppResult<Vec<String>> {
    let client = reqwest::Client::new();
    let models = match provider {
        "github-code" => {
            client
                .get("https://api.github.com/user")
                .header("User-Agent", "operon")
                .bearer_auth(api_key)
                .send()
                .await
                .map_err(|err| AppError::ServiceUnavailable(format!("github token validation: {err}")))?
                .error_for_status()
                .map_err(|err| AppError::ServiceUnavailable(format!("github token validation: {err}")))?;
            Vec::new()
        }
        "google" => {
            let url = format!("https://generativelanguage.googleapis.com/v1beta/models?key={api_key}");
            let value: serde_json::Value = client.get(url).send().await.map_err(|err| AppError::ServiceUnavailable(format!("model fetch: {err}")))?.error_for_status().map_err(|err| AppError::ServiceUnavailable(format!("model fetch: {err}")))?.json().await.map_err(|err| AppError::ServiceUnavailable(format!("model parse: {err}")))?;
            value.get("models").and_then(|v| v.as_array()).into_iter().flatten().filter_map(|m| m.get("name").and_then(|v| v.as_str()).map(|name| name.trim_start_matches("models/").to_owned())).collect()
        }
        "anthropic" => {
            let value: serde_json::Value = client.get("https://api.anthropic.com/v1/models").header("x-api-key", api_key).header("anthropic-version", "2023-06-01").send().await.map_err(|err| AppError::ServiceUnavailable(format!("model fetch: {err}")))?.error_for_status().map_err(|err| AppError::ServiceUnavailable(format!("model fetch: {err}")))?.json().await.map_err(|err| AppError::ServiceUnavailable(format!("model parse: {err}")))?;
            value.get("data").and_then(|v| v.as_array()).into_iter().flatten().filter_map(|m| m.get("id").and_then(|v| v.as_str()).map(str::to_owned)).collect()
        }
        _ => {
            let root = base_url.map(str::trim).filter(|v| !v.is_empty()).unwrap_or_else(|| default_base_url(provider));
            let url = format!("{}/models", root.trim_end_matches('/'));
            let value: serde_json::Value = client.get(url).bearer_auth(api_key).send().await.map_err(|err| AppError::ServiceUnavailable(format!("model fetch: {err}")))?.error_for_status().map_err(|err| AppError::ServiceUnavailable(format!("model fetch: {err}")))?.json().await.map_err(|err| AppError::ServiceUnavailable(format!("model parse: {err}")))?;
            value.get("data").and_then(|v| v.as_array()).into_iter().flatten().filter_map(|m| m.get("id").and_then(|v| v.as_str()).map(str::to_owned)).collect()
        }
    };

    Ok(models)
}

fn default_base_url(provider: &str) -> &'static str {
    match provider {
        "openrouter" => "https://openrouter.ai/api/v1",
        "groq" => "https://api.groq.com/openai/v1",
        "deepseek" => "https://api.deepseek.com/v1",
        "xai" => "https://api.x.ai/v1",
        "mistral" => "https://api.mistral.ai/v1",
        "github" => "https://models.inference.ai.azure.com",
        "minimax" => "https://api.minimax.io/v1",
        _ => "https://api.openai.com/v1",
    }
}

pub async fn persona() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "persona": null })))
}

pub async fn save_persona(Json(payload): Json<serde_json::Value>) -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "persona": payload })))
}

pub async fn memory() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "results": [] })))
}

pub async fn delete_memory(Path(_id): Path<String>) -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "ok": true })))
}

pub async fn workspace_files() -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({
        "files": {
            "bootstrap": "",
            "soul": "",
            "user": ""
        }
    })))
}

pub async fn save_workspace_file(Json(payload): Json<WorkspaceFileRequest>) -> AppResult<Json<serde_json::Value>> {
    Ok(Json(json!({ "ok": true, "kind": payload.kind, "size": payload.content.len() })))
}
