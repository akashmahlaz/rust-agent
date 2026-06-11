use sqlx::{Pool, Postgres};

use crate::agent::AgentRegistry;
use crate::codex::CodexBridge;
use crate::config::Config;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Pool<Postgres>,
    pub codex: CodexBridge,
    pub agents: AgentRegistry,
}
