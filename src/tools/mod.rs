//! Integration tools for the agent.
//!
//! This module is the home for all tools that call external APIs (Google, Stripe, etc.).
//! The local workspace tools (read/write/exec) live in `agent::tools` instead.
//!
//! Each tool module exports two functions:
//!   - `definitions()` -> Vec<Value>  (OpenAI function-calling schema)
//!   - `execute(ctx, args) -> Result<Value>` (runtime implementation)

pub mod crypto;

// Re-export so other modules can use it
pub use crypto::decrypt_token;

/// Build a tool definition for the LLM function-calling interface.
#[macro_export]
macro_rules! tool_def {
    ($name:expr, $description:expr, $schema:expr) => {
        serde_json::json!({
            "type": "function",
            "function": {
                "name": $name,
                "description": $description,
                "parameters": $schema
            }
        })
    };
    ($name:expr, $description:expr) => {
        serde_json::json!({
            "type": "function",
            "function": {
                "name": $name,
                "description": $description,
                "parameters": serde_json::json!({
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false
                })
            }
        })
    };
}
