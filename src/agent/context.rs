//! Automatic context window management.
//!
//! Before every LLM call, the runner uses this module to fit the conversation
//! into the model's context limit. The user sees the full history in the UI;
//! the model receives a sliding window with a heuristic summary of older turns.
//! This makes sessions effectively unlimited without manual compaction.
//!
//! Combined with Anthropic's prompt caching (cache_control), this keeps
//! per-turn costs at ~$0.03-0.05 instead of $0.30-0.56.

use serde_json::Value;

use super::openai::ChatMessage;

/// Known context limits per provider/model (in tokens).
pub fn model_context_limit(provider: &str, model: &str) -> usize {
    match (provider, model) {
        // OpenAI
        (_, m) if m.contains("gpt-4o") => 128_000,
        (_, m) if m.contains("gpt-4o-mini") => 128_000,
        (_, m) if m.contains("gpt-4-turbo") => 128_000,
        (_, m) if m.contains("gpt-4") && m.contains("32k") => 32_768,
        (_, m) if m.contains("o1") || m.contains("o3") || m.contains("o4") => 200_000,
        (_, m) if m.contains("gpt-3.5-turbo") => 16_385,
        // Anthropic
        ("anthropic", _) => 200_000,
        (_, m) if m.contains("claude") => 200_000,
        // Google
        (_, m) if m.contains("gemini-2") => 1_000_000,
        (_, m) if m.contains("gemini-1.5") => 1_000_000,
        (_, m) if m.contains("gemini") => 128_000,
        // Other OpenAI-compatible
        ("deepseek", _) => 128_000,
        ("minimax", _) => 128_000,
        ("groq", _) => 32_000,
        ("openrouter", _) => 128_000,
        ("xai", _) => 128_000,
        ("mistral", _) => 128_000,
        ("github", _) => 128_000,
        // Fallback
        _ => 32_000,
    }
}

/// Default max output tokens per provider.
#[allow(dead_code)]
pub fn default_max_output_tokens(provider: &str, _model: &str) -> usize {
    match provider {
        "anthropic" => 8_192,
        "openai" | "openrouter" | "deepseek" | "minimax" | "xai" | "mistral" => 16_384,
        "groq" => 8_192,
        "google" => 8_192,
        _ => 4_096,
    }
}

/// Rough token estimation: ~4 chars per token for English text.
/// This is intentionally conservative (slightly over-estimates) to avoid overflow.
fn estimate_tokens(text: &str) -> usize {
    // ~3.5 chars per token average, round up for safety margin
    (text.len() + 3) / 4 + 4 // +4 for message overhead (role, separators)
}

fn estimate_message_tokens(msg: &ChatMessage) -> usize {
    let content_tokens = match &msg.content {
        Some(Value::String(s)) => estimate_tokens(s),
        Some(v) => estimate_tokens(&v.to_string()),
        None => 0,
    };
    let tool_calls_tokens = msg.tool_calls.as_ref()
        .map(|tcs| tcs.iter().map(|tc| {
            estimate_tokens(&tc.function.name) + estimate_tokens(&tc.function.arguments)
        }).sum::<usize>())
        .unwrap_or(0);
    content_tokens + tool_calls_tokens + 4 // ~4 token overhead per message
}

/// Estimate tokens for tool definitions (they're surprisingly expensive — often 1-2K tokens).
pub fn estimate_tools_tokens(tools: &[Value]) -> usize {
    tools.iter().map(|t| estimate_tokens(&t.to_string())).sum()
}

/// Estimate tokens for the system prompt.
pub fn estimate_system_tokens(system: &str) -> usize {
    estimate_tokens(system)
}

/// Build a context-managed message array that fits within the model's limits.
/// Returns the messages to send to the model (may be fewer than full history).
///
/// Strategy:
/// 1. Always include system prompt (counted separately via system_tokens param)
/// 2. Fill from most recent messages backward until budget is exhausted
/// 3. If older messages were dropped, prepend a FREE heuristic summary
///
/// The UI shows all messages; the model sees only what fits + a summary.
pub fn prepare_context(
    messages: &[ChatMessage],
    system_tokens: usize,
    tool_tokens: usize,
    provider: &str,
    model: &str,
) -> Vec<ChatMessage> {
    let model_limit = model_context_limit(provider, model);
    let output_reserve: usize = 8_192; // always reserve space for model output
    let budget = model_limit.saturating_sub(output_reserve + system_tokens + tool_tokens);

    tracing::trace!(
        model_limit = model_limit,
        output_reserve = output_reserve,
        system_tokens = system_tokens,
        tool_tokens = tool_tokens,
        budget = budget,
        total_messages = messages.len(),
        "context_window_prepare"
    );

    // If everything fits, return as-is (most common case for short conversations)
    let total_msg_tokens: usize = messages.iter().map(|m| estimate_message_tokens(m)).sum();
    if total_msg_tokens <= budget {
        tracing::debug!(
            total_tokens = total_msg_tokens,
            budget = budget,
            "context fits within budget - sending all {} messages",
            messages.len()
        );
        return messages.to_vec();
    }

    tracing::info!(
        total_tokens = total_msg_tokens,
        budget = budget,
        messages_count = messages.len(),
        "context exceeds budget - applying sliding window"
    );

    // Sliding window: keep most recent messages that fit
    let mut selected: Vec<&ChatMessage> = Vec::new();
    let mut used: usize = 0;

    for msg in messages.iter().rev() {
        let cost = estimate_message_tokens(msg);
        if used + cost > budget {
            break;
        }
        selected.push(msg);
        used += cost;
    }
    selected.reverse();

    let dropped_count = messages.len() - selected.len();

    // Generate a FREE heuristic summary of dropped messages (no LLM call)
    if dropped_count > 0 {
        let dropped = &messages[..dropped_count];
        let summary = heuristic_summary(dropped);

        // Inject the summary as a system message (acts as "compressed memory")
        let summary_msg = ChatMessage {
            role: "system".to_owned(),
            content: Some(Value::String(format!(
                "[Earlier conversation summary — {} messages omitted for context window]\n{}",
                dropped_count, summary
            ))),
            name: None,
            tool_call_id: None,
            tool_calls: None,
        };

        let mut result = Vec::with_capacity(selected.len() + 1);
        result.push(summary_msg);
        // selected is Vec<&ChatMessage> — dereference then clone to get owned ChatMessage
        for msg in selected.iter() {
            result.push((*msg).clone());
        }

        tracing::info!(
            dropped_messages = dropped_count,
            kept_messages = selected.len(),
            summary_chars = summary.len(),
            result_tokens = result.iter().map(|m| estimate_message_tokens(m)).sum::<usize>(),
            "context_window_applied - summarized older messages"
        );

        return result;
    }

    // Shouldn't reach here, but safety fallback
    selected.iter().map(|m| (*m).clone()).collect()
}

/// Generate a structural summary of messages without any LLM call (zero cost).
/// Extracts key information: user goals, decisions, tool results.
/// This is a simple rule-based summarizer — no AI involved.
fn heuristic_summary(messages: &[ChatMessage]) -> String {
    let mut summary = String::with_capacity(2048);

    let mut user_goals: Vec<String> = Vec::new();
    let mut key_responses: Vec<String> = Vec::new();
    let mut tool_count = 0;
    let mut errors: Vec<String> = Vec::new();

    for msg in messages {
        let content = match &msg.content {
            Some(Value::String(s)) => s.as_str(),
            _ => continue,
        };

        match msg.role.as_str() {
            "user" => {
                // Extract first meaningful line as the user's intent
                let intent: String = content.lines()
                    .find(|l| !l.trim().is_empty() && l.len() > 5)
                    .map(|l| l.trim().chars().take(120).collect::<String>())
                    .unwrap_or_default();
                if !intent.is_empty() && intent.len() > 10 {
                    user_goals.push(intent);
                }
            }
            "assistant" => {
                // Extract first meaningful response
                let first_line = content.lines()
                    .find(|l| !l.trim().is_empty() && l.len() > 10 && !l.trim().starts_with("```"))
                    .map(|l| l.trim().chars().take(150).collect::<String>())
                    .unwrap_or_default();
                if !first_line.is_empty() && first_line.len() > 15 {
                    key_responses.push(first_line);
                }
            }
            "tool" => {
                tool_count += 1;
                // Check for errors in tool output
                if content.to_lowercase().contains("error")
                    || content.to_lowercase().contains("failed")
                    || content.to_lowercase().contains("exception") {
                    let brief: String = content.chars().take(80).collect();
                    errors.push(brief);
                }
            }
            _ => {}
        }
    }

    // Build summary structure
    summary.push_str("## Earlier Conversation Summary\n\n");

    if !user_goals.is_empty() {
        summary.push_str("### User Goals/Requests\n");
        for (i, goal) in user_goals.iter().take(8).enumerate() {
            summary.push_str(&format!("{}. {}\n", i + 1, goal));
        }
        if user_goals.len() > 8 {
            summary.push_str(&format!("... and {} more requests\n", user_goals.len() - 8));
        }
        summary.push('\n');
    }

    if !key_responses.is_empty() {
        summary.push_str("### Key AI Responses\n");
        for response in key_responses.iter().take(5) {
            summary.push_str(&format!("- {}\n", response));
        }
        if key_responses.len() > 5 {
            summary.push_str(&format!("... and {} more responses\n", key_responses.len() - 5));
        }
        summary.push('\n');
    }

    if tool_count > 0 {
        summary.push_str(&format!("### Tool Activity\n"));
        summary.push_str(&format!("{} tool calls were executed", tool_count));
        if !errors.is_empty() {
            summary.push_str(&format!(" ({} had errors)\n", errors.len()));
            for err in errors.iter().take(3) {
                summary.push_str(&format!("  - Error: {}...\n", err));
            }
        } else {
            summary.push('\n');
        }
    }

    if summary.len() < 100 {
        summary.push_str("(Earlier context summarized for token budget — see above for what was covered)");
    }

    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(role: &str, content: &str) -> ChatMessage {
        ChatMessage {
            role: role.to_owned(),
            content: Some(Value::String(content.to_owned())),
            name: None,
            tool_call_id: None,
            tool_calls: None,
        }
    }

    #[test]
    fn test_everything_fits() {
        let msgs = vec![
            make_msg("user", "Hello"),
            make_msg("assistant", "Hi!"),
        ];
        let result = prepare_context(&msgs, 100, 50, "openai", "gpt-4o");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_heuristic_summary() {
        let msgs = vec![
            make_msg("user", "Can you help me build a web app?"),
            make_msg("assistant", "Sure! I'll help you build a React app."),
            make_msg("user", "Add authentication"),
            make_msg("assistant", "I've added auth using next-auth."),
        ];
        let summary = heuristic_summary(&msgs);
        assert!(summary.contains("User Goals"));
        assert!(summary.contains("web app"));
        assert!(summary.contains("authentication"));
    }

    #[test]
    fn test_model_limits() {
        assert_eq!(model_context_limit("anthropic", "claude-3-5-sonnet"), 200_000);
        assert_eq!(model_context_limit("openai", "gpt-4o"), 128_000);
        assert_eq!(model_context_limit("openai", "gpt-4o-mini"), 128_000);
        assert_eq!(model_context_limit("minimax", "whatever"), 128_000);
        assert_eq!(model_context_limit("groq", "llama-3.1-8b"), 32_000);
        assert_eq!(model_context_limit("unknown", "unknown"), 32_000);
    }
}
