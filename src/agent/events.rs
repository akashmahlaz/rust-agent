//! SSE event envelopes emitted by the agent runner and consumed by the
//! `useStreamEvents` hook. Modelled after Copilot's `ChatResponseStream`
//! (see vscode-copilot-chat/src/util/common/chatResponseStreamImpl.ts).
//!
//! Wire shape: `{ "type": "<kind>", "data": { ... } }`. Keep keys camelCase
//! so the TS frontend can consume them without renaming.

#![allow(dead_code)]

use serde_json::{Value, json};

// ── Text / markdown ────────────────────────────────────────────────────────

/// Streaming text chunk.
pub fn text_delta(text: &str) -> Value {
    json!({ "type": "text-delta", "data": { "text": text } })
}

/// Marks the end of a contiguous text run (the model produced a tool call
/// or finished). Following text-deltas start a new logical block.
pub fn text_end() -> Value {
    json!({ "type": "text-end", "data": {} })
}

/// Associate the next ``` code block with a file URI. Mirrors `codeblockUri`.
pub fn codeblock_uri(uri: &str, is_edit: bool) -> Value {
    json!({
        "type": "codeblock-uri",
        "data": { "uri": uri, "isEdit": is_edit }
    })
}

// ── Reasoning / thinking ───────────────────────────────────────────────────

/// Marks the beginning of a reasoning block. `id` allows chunks to merge.
pub fn reasoning_start(id: &str) -> Value {
    json!({
        "type": "reasoning-start",
        "data": { "id": id }
    })
}

/// Streaming reasoning chunk. `id` allows chunks to merge into one block.
/// Mirrors `thinkingProgress`.
pub fn reasoning_delta(id: &str, text: &str) -> Value {
    json!({
        "type": "reasoning-delta",
        "data": { "id": id, "text": text }
    })
}

pub fn reasoning_end(id: &str) -> Value {
    json!({ "type": "reasoning-end", "data": { "id": id } })
}

// ── Status / progress ──────────────────────────────────────────────────────

/// Lightweight status line shown above the next assistant text. Mirrors `progress`.
pub fn progress(text: &str) -> Value {
    json!({ "type": "progress", "data": { "text": text, "status": "active" } })
}

pub fn progress_done(text: &str) -> Value {
    json!({ "type": "progress", "data": { "text": text, "status": "complete" } })
}

/// Inline anchor — clickable file or symbol reference. Mirrors `anchor`.
pub fn anchor(uri: &str, title: Option<&str>, line: Option<u32>) -> Value {
    json!({
        "type": "anchor",
        "data": { "uri": uri, "title": title, "line": line }
    })
}

/// Sidebar reference chip with optional status. Mirrors `reference2`.
/// status: loading | success | error | omitted | partial
pub fn reference(uri: &str, title: Option<&str>, status: Option<&str>) -> Value {
    json!({
        "type": "reference",
        "data": { "uri": uri, "title": title, "status": status }
    })
}

// ── Edits ──────────────────────────────────────────────────────────────────

/// Streaming text edit chunk for a file. Mirrors `textEdit(target, edits|true)`.
pub fn text_edit(target: &str, edits: &Value, is_done: bool) -> Value {
    json!({
        "type": "text-edit",
        "data": { "target": target, "edits": edits, "isDone": is_done }
    })
}

// ── Confirmation / interactive ─────────────────────────────────────────────

/// Confirmation card. Frontend posts the chosen button (and `data`) back to
/// `POST /agent/runs/{id}/respond`. Mirrors `confirmation`.
pub fn confirmation(id: &str, title: &str, message: &str, data: &Value, buttons: &[&str]) -> Value {
    json!({
        "type": "confirmation",
        "data": {
            "id": id,
            "title": title,
            "message": message,
            "data": data,
            "buttons": buttons,
        }
    })
}

/// Inline command button. Mirrors `button`.
pub fn command_button(command: &str, title: &str, args: &Value) -> Value {
    json!({
        "type": "command-button",
        "data": { "command": command, "title": title, "args": args }
    })
}

/// Warning banner. Mirrors `warning`.
pub fn warning(text: &str) -> Value {
    json!({ "type": "warning", "data": { "text": text } })
}

// ── Tool invocation lifecycle ──────────────────────────────────────────────
//
// Mirrors Copilot's separate tool channel. UI states:
//   pending → input-streaming → input-available → executing →
//   output-available | output-error
//
// `invocationMessage` = present-tense ("Reading file `foo.ts`…")
// `pastTenseMessage`  = past-tense   ("Read file `foo.ts`")

pub fn tool_call_start(
    tool_call_id: &str,
    tool_name: &str,
    invocation_message: Option<&str>,
    origin_message: Option<&str>,
) -> Value {
    json!({
        "type": "tool-call-start",
        "data": {
            "toolCallId": tool_call_id,
            "toolName": tool_name,
            "invocationMessage": invocation_message,
            "originMessage": origin_message,
        }
    })
}

pub fn tool_call_input_available(tool_call_id: &str, args: &Value) -> Value {
    json!({
        "type": "tool-call-input-available",
        "data": { "toolCallId": tool_call_id, "args": args }
    })
}

pub fn tool_call_input_streaming(tool_call_id: &str, args: &Value) -> Value {
    json!({
        "type": "tool-call-input-streaming",
        "data": { "toolCallId": tool_call_id, "args": args }
    })
}

pub fn tool_call_execute(tool_call_id: &str) -> Value {
    json!({
        "type": "tool-call-execute",
        "data": { "toolCallId": tool_call_id }
    })
}

pub fn tool_call_update(
    tool_call_id: &str,
    invocation_message: Option<&str>,
    past_tense_message: Option<&str>,
) -> Value {
    json!({
        "type": "tool-call-update",
        "data": {
            "toolCallId": tool_call_id,
            "invocationMessage": invocation_message,
            "pastTenseMessage": past_tense_message,
        }
    })
}

pub fn tool_call_output_available(
    tool_call_id: &str,
    result: &Value,
    past_tense_message: Option<&str>,
) -> Value {
    json!({
        "type": "tool-call-output-available",
        "data": {
            "toolCallId": tool_call_id,
            "result": result,
            "pastTenseMessage": past_tense_message,
        }
    })
}

pub fn tool_call_output_error(tool_call_id: &str, error_text: &str) -> Value {
    json!({
        "type": "tool-call-output-error",
        "data": { "toolCallId": tool_call_id, "errorText": error_text }
    })
}

pub fn tool_call_end(tool_call_id: &str) -> Value {
    json!({
        "type": "tool-call-end",
        "data": { "toolCallId": tool_call_id }
    })
}

// ── Lifecycle / metadata ───────────────────────────────────────────────────

pub fn message_end() -> Value {
    json!({ "type": "message-end", "data": {} })
}

/// Provider-side request id (for log correlation). Emitted once per LLM
/// request, before streaming text begins.
pub fn provider_request_id(provider: &str, model: &str, request_id: &str) -> Value {
    json!({
        "type": "provider-request-id",
        "data": {
            "provider": provider,
            "model": model,
            "requestId": request_id,
        }
    })
}

/// Stream-level error (HTTP failure mid-stream, parser failure, etc.).
/// Renders as an inline error card on the frontend without aborting the run.
pub fn stream_error(message: &str, request_id: Option<&str>, provider: Option<&str>) -> Value {
    json!({
        "type": "stream-error",
        "data": {
            "message": message,
            "requestId": request_id,
            "provider": provider,
        }
    })
}

/// Token usage emitted on completion. Mirrors `usage`.
pub fn usage(prompt_tokens: u64, completion_tokens: u64, total_tokens: u64) -> Value {
    json!({
        "type": "usage",
        "data": {
            "promptTokens": prompt_tokens,
            "completionTokens": completion_tokens,
            "totalTokens": total_tokens,
        }
    })
}

pub fn subagent_start(tool_call_id: &str, agent_name: Option<&str>, prompt: Option<&str>) -> Value {
    json!({
        "type": "subagent-start",
        "data": { "toolCallId": tool_call_id, "agentName": agent_name, "prompt": prompt }
    })
}

pub fn subagent_progress(
    tool_call_id: &str,
    agent_name: Option<&str>,
    text: &str,
    status: &str,
) -> Value {
    json!({
        "type": "subagent-progress",
        "data": {
            "toolCallId": tool_call_id,
            "agentName": agent_name,
            "text": text,
            "status": status,
        }
    })
}

pub fn subagent_stream_delta(
    tool_call_id: &str,
    agent_name: Option<&str>,
    kind: &str,
    text: &str,
) -> Value {
    json!({
        "type": "subagent-stream-delta",
        "data": {
            "toolCallId": tool_call_id,
            "agentName": agent_name,
            "kind": kind,
            "text": text,
        }
    })
}

pub fn subagent_result(
    tool_call_id: &str,
    agent_name: Option<&str>,
    result: &Value,
    run_id: Option<&str>,
    log_url: Option<&str>,
) -> Value {
    json!({
        "type": "subagent-result",
        "data": {
            "toolCallId": tool_call_id,
            "agentName": agent_name,
            "result": result,
            "runId": run_id,
            "logUrl": log_url,
        }
    })
}

pub fn error(error_text: &str) -> Value {
    json!({ "type": "error", "data": { "errorText": error_text } })
}
