use axum::{Json, extract::State};
use serde::Serialize;

use crate::{codex::CodexHealth, http::error::AppResult, state::AppState};

#[derive(Serialize)]
pub struct CodexCapabilitiesResponse {
    status: &'static str,
    transport: &'static str,
    client_methods: &'static [&'static str],
    server_notifications: &'static [&'static str],
    server_requests: &'static [&'static str],
}

pub async fn healthz(State(state): State<AppState>) -> AppResult<Json<CodexHealth>> {
    Ok(Json(state.codex.health().await))
}

pub async fn capabilities() -> Json<CodexCapabilitiesResponse> {
    Json(CodexCapabilitiesResponse {
        status: "protocol_bindings_generated",
        transport: "codex app-server over stdio or websocket",
        client_methods: &[
            "initialize",
            "thread/start",
            "thread/resume",
            "thread/fork",
            "thread/list",
            "thread/read",
            "turn/start",
            "turn/steer",
            "turn/interrupt",
            "review/start",
            "model/list",
            "config/read",
            "skills/list",
            "plugin/list",
            "app/list",
            "mcpServerStatus/list",
            "command/exec",
        ],
        server_notifications: &[
            "thread/started",
            "thread/status/changed",
            "turn/started",
            "turn/completed",
            "item/agentMessage/delta",
            "item/reasoning/textDelta",
            "item/reasoning/summaryTextDelta",
            "item/commandExecution/outputDelta",
            "item/fileChange/outputDelta",
            "item/fileChange/patchUpdated",
            "turn/diff/updated",
            "turn/plan/updated",
            "serverRequest/resolved",
            "error",
        ],
        server_requests: &[
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
            "item/tool/requestUserInput",
            "mcpServer/elicitation/request",
            "applyPatchApproval",
            "execCommandApproval",
        ],
    })
}
