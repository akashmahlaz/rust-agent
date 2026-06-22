use std::sync::Arc;

use sqlx::{Pool, Postgres};

use crate::agent::{AgentRegistry, ProviderRegistry};
use crate::codex::CodexBridge;
use crate::config::Config;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Pool<Postgres>,
    pub codex: CodexBridge,
    pub agents: AgentRegistry,
    /// Provider-agnostic adapter registry. The agent runner asks this
    /// registry how to translate file attachments into the active model's
    /// native content blocks (OpenAI `input_file`, Anthropic `document`,
    /// Gemini `file_data`, ...). Cheap to clone — backed by `Arc` inside.
    pub providers: Arc<ProviderRegistry>,
}
