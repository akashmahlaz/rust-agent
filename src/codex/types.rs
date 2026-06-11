#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CodexStreamEvent {
    RawProtocol {
        method: String,
        payload: Value,
    },
    AgentMessageDelta {
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
    },
    ReasoningDelta {
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
    },
    CommandOutputDelta {
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
    },
    FileChangeDelta {
        thread_id: String,
        turn_id: String,
        item_id: String,
        delta: String,
    },
    ApprovalRequested {
        approval_id: String,
        approval_type: ApprovalType,
        payload: Value,
    },
    TurnStatus {
        thread_id: String,
        turn_id: String,
        status: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalType {
    CommandExecution,
    FileChange,
    Permissions,
    ToolUserInput,
    McpElicitation,
    LegacyExecCommand,
    LegacyApplyPatch,
}
