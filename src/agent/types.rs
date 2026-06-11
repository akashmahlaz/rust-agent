use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub type RunId = Uuid;

#[derive(Debug, Clone, Deserialize)]
pub struct RunRequest {
    /// Initial user prompt that kicks off the run.
    pub prompt: String,
    /// Provider-qualified model id. Defaults to "openai:gpt-4o-mini" if absent.
    #[serde(default)]
    pub model: Option<String>,
    /// Optional pre-existing conversation id; new one is created if absent.
    #[serde(default)]
    pub conversation_id: Option<Uuid>,
    /// Conversation channel. Defaults to coding for /api/coding and web for /api/chat proxies.
    #[serde(default)]
    pub channel: Option<String>,
    /// Optional workspace path override (must already exist & be inside the
    /// configured workspace root).
    #[serde(default)]
    pub workspace: Option<String>,
    /// Optional parent run id for child-agent/subagent tracing.
    #[serde(default)]
    pub parent_run_id: Option<Uuid>,
    /// Optional parent request id for Copilot-style request lineage.
    #[serde(default)]
    pub parent_request_id: Option<String>,
    /// Optional parent tool call id when this run was spawned by a tool.
    #[serde(default)]
    pub parent_tool_call_id: Option<String>,
    /// Optional provider/tool metadata stored with the run.
    #[serde(default)]
    pub metadata: Option<Value>,
    /// Reasoning effort hint forwarded to the provider. One of
    /// "none" | "auto" | "low" | "medium" | "high".
    /// Maps to OpenAI `reasoning_effort` and Anthropic `thinking.budget_tokens`.
    #[serde(default, alias = "reasoningLevel")]
    pub reasoning_level: Option<String>,
    /// Optional file attachments (typically images for vision models) that
    /// should be sent to the provider as structured content parts alongside
    /// the user prompt. Each entry must include a publicly fetchable URL.
    #[serde(default)]
    pub attachments: Option<Vec<AttachmentInput>>,
}

/// One attachment from the chat UI (uploaded via /uploads, stored in S3, etc.).
/// The runner converts these into provider-native content blocks
/// (OpenAI `image_url`, Anthropic `image` with URL source) for the first
/// user turn so vision-capable models can actually see the image.
#[derive(Debug, Clone, Deserialize)]
pub struct AttachmentInput {
    pub url: String,
    #[serde(default, alias = "mimeType", alias = "mime_type")]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Queued,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Internal event broadcast from the runner to subscribers (SSE clients).
///
/// `sequence` matches the `run_events.sequence` column so reconnecting clients
/// can replay-then-tail.
#[derive(Debug, Clone, Serialize)]
pub struct AgentEvent {
    pub sequence: i64,
    /// Raw payload as serialized for the AI SDK UI Message Stream Protocol.
    /// Each value is one `data: ...` SSE frame body.
    pub frame: serde_json::Value,
}
