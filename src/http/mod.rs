mod admin;
mod agent;
mod agent_skills;
mod auth;
mod codex;
mod conversations;
mod error;
mod health;
mod integrations;
mod logs;
mod meta;
mod settings;
mod uploads;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderValue, Method, header},
    routing::{delete, get, patch, post},
};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use crate::state::AppState;

pub use auth::{decode_claims_public, token_from_request};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .route("/codex/healthz", get(codex::healthz))
        .route("/codex/capabilities", get(codex::capabilities))
        .route("/auth/signup", post(auth::signup))
        .route("/auth/login", post(auth::login))
        .route("/auth/logout", post(auth::logout))
        .route("/auth/me", get(auth::me))
        .route("/auth/oauth/google", get(auth::google_oauth_start))
        .route("/auth/oauth/google/callback", get(auth::google_oauth_callback))
        .route("/auth/oauth/github", get(auth::github_oauth_start))
        .route("/auth/oauth/github/callback", get(auth::github_oauth_callback))
        .route("/auth/internal/exchange", post(auth::internal_exchange))
        .route("/agent/runs", post(agent::create_run))
        .route("/agent/runs/{id}/sse", get(agent::sse_run))
        .route("/agent/runs/{id}/cancel", post(agent::cancel_run))
        .route("/agent/conversations", get(conversations::list_conversations).post(conversations::create_conversation))
        .route("/agent/conversations/{id}", get(conversations::get_conversation).patch(conversations::update_conversation).delete(conversations::delete_conversation))
        .route("/agent/conversations/{id}/messages", post(conversations::append_message))
        .route("/agent/conversations/{id}/compact", post(conversations::compact_conversation))
        .route("/agent/conversations/{id}/confirm", post(conversations::confirm_action))
        .route("/agent-skills", get(agent_skills::list_agent_skills))
        .route("/meta/status", get(meta::status))
        .route("/meta/connect", post(meta::connect))
        .route("/meta/campaigns", get(meta::campaigns))
        .route("/meta/insights", get(meta::insights))
        .route("/meta/campaign-action", post(meta::campaign_action))
        .route("/integrations/whatsapp/status", get(integrations::whatsapp_status))
        .route("/integrations/telegram/status", get(integrations::telegram_status))
        .route("/integrations/github/status", get(integrations::github_status))
        .route("/integrations/whatsapp/onboarding", get(integrations::whatsapp_onboarding))
        .route("/integrations/whatsapp", post(integrations::whatsapp_action))
        .route("/logs", get(logs::list_logs).post(logs::append_log))
        .route(
            "/uploads",
            post(uploads::create_upload)
                // Axum's default request body limit is 2 MiB which silently
                // 413's larger files before our own size check runs. Bump it
                // above the handler's 100 MiB file cap for multipart overhead.
                .layer(DefaultBodyLimit::max(125 * 1024 * 1024)),
        )
        // Serve locally-stored uploads (used as S3 fallback in dev when
        // AWS credentials are not configured).
        .nest_service("/local-uploads", ServeDir::new(uploads::LOCAL_UPLOADS_DIR))
        .route("/admin/usage", get(admin::usage_summary))
        .route("/admin/logs", get(admin::logs))
        .route("/admin/agents", get(admin::agents))
        .route("/admin/agents/{id}", patch(admin::update_agent).delete(admin::delete_agent))
        .route("/providers", get(settings::providers).post(settings::update_provider))
        .route("/providers/{provider}/profiles/{profile_id}", delete(settings::delete_provider_profile))
        .route("/persona", get(settings::persona).put(settings::save_persona))
        .route("/memory", get(settings::memory))
        .route("/memory/{id}", delete(settings::delete_memory))
        .route("/workspace-files", get(settings::workspace_files).post(settings::save_workspace_file))
        .with_state(state.clone())
        .layer(cors(state))
}

fn cors(state: AppState) -> CorsLayer {
    let origin: HeaderValue = state
        .config
        .web_origin
        .parse()
        .expect("OPERON_WEB_ORIGIN must be a valid header value");

    CorsLayer::new()
        .allow_origin(origin)
        .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE, header::ACCEPT])
        .allow_credentials(true)
}
