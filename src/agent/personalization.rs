//! Personalization engine — learns from user interactions, builds profiles,
//! and injects relevant context into the system prompt.
//!
//! Inspired by OpenClaw's BOOTSTRAP.md + USER.md + MEMORY.md system,
//! but backed by PostgreSQL + pgvector instead of files.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

/// User profile data loaded from the database.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UserProfile {
    pub display_name: Option<String>,
    pub role: Option<String>,
    pub organization: Option<String>,
    pub timezone: Option<String>,
    pub language: String,
    pub communication_style: Option<String>,
    pub expertise_level: String,
    pub known_domains: Vec<String>,
    pub known_exchanges: Vec<String>,
    pub known_publishers: Vec<String>,
    pub preferred_tools: Vec<String>,
    pub preferred_checks: Vec<String>,
    pub bootstrap_completed: bool,
    pub interaction_count: i32,
}

/// Relevant memories recalled for a conversation.
#[derive(Debug, Clone, Serialize)]
pub struct PersonalizationContext {
    pub profile: UserProfile,
    pub relevant_memories: Vec<MemoryEntry>,
    pub recent_patterns: Vec<String>,
    pub is_first_session: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemoryEntry {
    pub content: String,
    pub category: String,
    pub created_at: String,
}

/// Load a user's profile from the database. Creates one if it doesn't exist.
pub async fn load_or_create_profile(db: &Pool<Postgres>, user_id: Uuid) -> Result<UserProfile> {
    // Try to load existing profile
    let row = sqlx::query(
        r#"SELECT display_name, role, organization, timezone, language,
                  communication_style, expertise_level, known_domains,
                  known_exchanges, known_publishers, preferred_tools,
                  preferred_checks, bootstrap_completed, interaction_count
           FROM user_profiles WHERE user_id = $1"#,
    )
    .bind(user_id)
    .fetch_optional(db)
    .await
    .context("loading user profile")?;

    match row {
        Some(row) => Ok(UserProfile {
            display_name: row.try_get("display_name").ok().flatten(),
            role: row.try_get("role").ok().flatten(),
            organization: row.try_get("organization").ok().flatten(),
            timezone: row.try_get("timezone").ok().flatten(),
            language: row.try_get::<Option<String>, _>("language").ok().flatten().unwrap_or_else(|| "en".to_string()),
            communication_style: row.try_get("communication_style").ok().flatten(),
            expertise_level: row.try_get::<Option<String>, _>("expertise_level").ok().flatten().unwrap_or_else(|| "intermediate".to_string()),
            known_domains: row.try_get::<Vec<String>, _>("known_domains").unwrap_or_default(),
            known_exchanges: row.try_get::<Vec<String>, _>("known_exchanges").unwrap_or_default(),
            known_publishers: row.try_get::<Vec<String>, _>("known_publishers").unwrap_or_default(),
            preferred_tools: row.try_get::<Vec<String>, _>("preferred_tools").unwrap_or_default(),
            preferred_checks: row.try_get::<Vec<String>, _>("preferred_checks").unwrap_or_default(),
            bootstrap_completed: row.try_get("bootstrap_completed").unwrap_or(false),
            interaction_count: row.try_get("interaction_count").unwrap_or(0),
        }),
        None => {
            // Create empty profile
            sqlx::query(
                "INSERT INTO user_profiles (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            )
            .bind(user_id)
            .execute(db)
            .await
            .context("creating user profile")?;

            Ok(UserProfile::default())
        }
    }
}

/// Recall relevant memories for personalization.
/// Uses text search (falls back gracefully when pgvector is unavailable).
pub async fn recall_relevant_memories(
    db: &Pool<Postgres>,
    user_id: Uuid,
    query: Option<&str>,
    limit: i64,
) -> Result<Vec<MemoryEntry>> {
    let rows = if let Some(q) = query {
        sqlx::query(
            r#"SELECT content, category, created_at FROM memories
               WHERE user_id = $1 AND content ILIKE '%' || $2 || '%'
               ORDER BY updated_at DESC LIMIT $3"#,
        )
        .bind(user_id)
        .bind(q)
        .bind(limit)
        .fetch_all(db)
        .await
    } else {
        // Get most recent memories across all categories
        sqlx::query(
            r#"SELECT content, category, created_at FROM memories
               WHERE user_id = $1
               ORDER BY updated_at DESC LIMIT $2"#,
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(db)
        .await
    }
    .context("recalling memories")?;

    Ok(rows
        .iter()
        .map(|row| MemoryEntry {
            content: row.try_get("content").unwrap_or_default(),
            category: row.try_get::<Option<String>, _>("category")
                .ok()
                .flatten()
                .unwrap_or_else(|| "general".to_string()),
            created_at: row
                .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_default(),
        })
        .collect())
}

/// Get recent investigation patterns for the user.
pub async fn get_recent_patterns(db: &Pool<Postgres>, user_id: Uuid) -> Result<Vec<String>> {
    let rows = sqlx::query(
        r#"SELECT pattern_type, domain, parameters FROM investigation_patterns
           WHERE user_id = $1
           ORDER BY frequency DESC, last_used_at DESC
           LIMIT 5"#,
    )
    .bind(user_id)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    Ok(rows
        .iter()
        .map(|row| {
            let pattern_type: String = row.try_get("pattern_type").unwrap_or_default();
            let domain: Option<String> = row.try_get("domain").ok().flatten();
            match domain {
                Some(d) => format!("{} on {}", pattern_type, d),
                None => pattern_type,
            }
        })
        .collect())
}

/// Build the full personalization context for a conversation.
pub async fn build_context(
    db: &Pool<Postgres>,
    user_id: Uuid,
    user_message: Option<&str>,
) -> Result<PersonalizationContext> {
    let profile = load_or_create_profile(db, user_id).await?;
    let is_first = !profile.bootstrap_completed && profile.interaction_count == 0;

    // Recall memories relevant to current query (if available)
    let relevant_memories = recall_relevant_memories(db, user_id, user_message, 10).await?;
    let recent_patterns = get_recent_patterns(db, user_id).await?;

    Ok(PersonalizationContext {
        profile,
        relevant_memories,
        recent_patterns,
        is_first_session: is_first,
    })
}

/// Generate the personalization block to inject into the system prompt.
pub fn generate_prompt_injection(ctx: &PersonalizationContext) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Bootstrap prompt for first session
    if ctx.is_first_session {
        parts.push(r#"<bootstrap>
This is the user's FIRST session. Welcome them warmly and learn about them:
1. Ask their name and role (AdOps, Publisher, Analyst, Developer, etc.)
2. Ask what organization they work with
3. Ask what they typically investigate (domains, sellers, fraud, traffic)
4. Ask their preferred level of detail (brief vs detailed)
5. Note their timezone if mentioned

Store everything you learn using memory_store with scope="user" and appropriate tags.
After the conversation, the system will automatically build their profile.
Be natural — don't ask all questions at once. Weave them into the conversation.
</bootstrap>"#.to_string());
        return parts.join("\n\n");
    }

    // User profile injection
    let p = &ctx.profile;
    let mut profile_parts: Vec<String> = Vec::new();

    if let Some(ref name) = p.display_name {
        profile_parts.push(format!("User: {}", name));
    }
    if let Some(ref role) = p.role {
        profile_parts.push(format!("Role: {}", role));
    }
    if let Some(ref org) = p.organization {
        profile_parts.push(format!("Organization: {}", org));
    }
    if let Some(ref style) = p.communication_style {
        profile_parts.push(format!("Communication: {}", style));
    }
    if !p.known_domains.is_empty() {
        profile_parts.push(format!("Domains they work with: {}", p.known_domains.join(", ")));
    }
    if !p.known_exchanges.is_empty() {
        profile_parts.push(format!("Exchanges: {}", p.known_exchanges.join(", ")));
    }
    if !p.preferred_tools.is_empty() {
        profile_parts.push(format!("Preferred tools: {}", p.preferred_tools.join(", ")));
    }

    if !profile_parts.is_empty() {
        parts.push(format!(
            "<userProfile>\n{}\nExpertise: {}\nSessions: {}\n</userProfile>",
            profile_parts.join("\n"),
            p.expertise_level,
            p.interaction_count
        ));
    }

    // Relevant memories
    if !ctx.relevant_memories.is_empty() {
        let memory_lines: Vec<String> = ctx
            .relevant_memories
            .iter()
            .take(5)
            .map(|m| format!("- [{}] {}", m.category, m.content))
            .collect();
        parts.push(format!(
            "<relevantMemories>\n{}\n</relevantMemories>",
            memory_lines.join("\n")
        ));
    }

    // Recent patterns
    if !ctx.recent_patterns.is_empty() {
        parts.push(format!(
            "<recentPatterns>\nThis user frequently: {}\n</recentPatterns>",
            ctx.recent_patterns.join(", ")
        ));
    }

    parts.join("\n\n")
}

/// After a conversation ends, extract insights and update the profile.
/// Called from the runner after an agent run completes.
pub async fn post_conversation_learn(
    db: &Pool<Postgres>,
    user_id: Uuid,
    _conversation_id: Uuid,
    messages: &[Value],
) -> Result<()> {
    // Increment interaction count
    sqlx::query(
        r#"UPDATE user_profiles
           SET interaction_count = interaction_count + 1,
               last_interaction_at = now(),
               updated_at = now()
           WHERE user_id = $1"#,
    )
    .bind(user_id)
    .execute(db)
    .await
    .ok();

    // Extract domains mentioned in assistant tool calls
    let mut domains_used: Vec<String> = Vec::new();
    let mut tools_used: Vec<String> = Vec::new();

    for msg in messages {
        // Check tool calls in assistant messages
        if let Some(parts) = msg.get("parts").and_then(Value::as_array) {
            for part in parts {
                if part.get("type").and_then(Value::as_str) == Some("tool-invocation")
                    || part.get("type").and_then(Value::as_str) == Some("tool-result")
                {
                    if let Some(name) = part.get("toolName").and_then(Value::as_str) {
                        if !tools_used.contains(&name.to_string()) {
                            tools_used.push(name.to_string());
                        }
                    }
                    // Extract domain from tool args
                    if let Some(args) = part.get("args") {
                        if let Some(domain) = args.get("domain").and_then(Value::as_str) {
                            let d = domain.to_string();
                            if !domains_used.contains(&d) {
                                domains_used.push(d);
                            }
                        }
                    }
                }
            }
        }
    }

    // Update known_domains (append new ones)
    if !domains_used.is_empty() {
        for domain in &domains_used {
            sqlx::query(
                r#"UPDATE user_profiles
                   SET known_domains = array_append(
                       CASE WHEN NOT ($2 = ANY(known_domains)) THEN known_domains ELSE known_domains END,
                       CASE WHEN NOT ($2 = ANY(known_domains)) THEN $2 ELSE NULL END
                   ),
                   updated_at = now()
                   WHERE user_id = $1 AND NOT ($2 = ANY(known_domains))"#,
            )
            .bind(user_id)
            .bind(domain)
            .execute(db)
            .await
            .ok();
        }
    }

    // Update preferred_tools (track frequency)
    if !tools_used.is_empty() {
        for tool in &tools_used {
            sqlx::query(
                r#"UPDATE user_profiles
                   SET preferred_tools = array_append(
                       CASE WHEN NOT ($2 = ANY(preferred_tools)) THEN preferred_tools ELSE preferred_tools END,
                       CASE WHEN NOT ($2 = ANY(preferred_tools)) THEN $2 ELSE NULL END
                   ),
                   updated_at = now()
                   WHERE user_id = $1 AND NOT ($2 = ANY(preferred_tools))"#,
            )
            .bind(user_id)
            .bind(tool)
            .execute(db)
            .await
            .ok();
        }
    }

    // Record investigation pattern if domain tools were used
    let adtech_tools = [
        "fetch_ads_txt", "fetch_sellers_json", "verify_domain",
        "detect_fraud", "crawl_domain", "dns_lookup", "ssl_inspect",
    ];
    let used_adtech = tools_used.iter().any(|t| adtech_tools.contains(&t.as_str()));

    if used_adtech && !domains_used.is_empty() {
        let pattern_type = if tools_used.contains(&"detect_fraud".to_string()) {
            "fraud_scan"
        } else if tools_used.contains(&"verify_domain".to_string()) {
            "domain_check"
        } else if tools_used.contains(&"fetch_sellers_json".to_string()) {
            "supply_chain_audit"
        } else {
            "general_investigation"
        };

        for domain in &domains_used {
            sqlx::query(
                r#"INSERT INTO investigation_patterns (user_id, pattern_type, domain, parameters)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT DO NOTHING"#,
            )
            .bind(user_id)
            .bind(pattern_type)
            .bind(domain)
            .bind(json!({"tools": tools_used}))
            .execute(db)
            .await
            .ok();
        }
    }

    Ok(())
}

/// Mark bootstrap as completed for a user.
#[allow(dead_code)]
pub async fn complete_bootstrap(db: &Pool<Postgres>, user_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"UPDATE user_profiles
           SET bootstrap_completed = true,
               bootstrap_completed_at = now(),
               updated_at = now()
           WHERE user_id = $1"#,
    )
    .bind(user_id)
    .execute(db)
    .await
    .context("completing bootstrap")?;
    Ok(())
}

/// Update user profile from bootstrap conversation data.
#[allow(dead_code)]
pub async fn update_profile_from_bootstrap(
    db: &Pool<Postgres>,
    user_id: Uuid,
    data: &Value,
) -> Result<()> {
    let name = data.get("name").and_then(Value::as_str);
    let role = data.get("role").and_then(Value::as_str);
    let org = data.get("organization").and_then(Value::as_str);
    let style = data.get("communication_style").and_then(Value::as_str);
    let expertise = data.get("expertise_level").and_then(Value::as_str);

    sqlx::query(
        r#"UPDATE user_profiles SET
            display_name = COALESCE($2, display_name),
            role = COALESCE($3, role),
            organization = COALESCE($4, organization),
            communication_style = COALESCE($5, communication_style),
            expertise_level = COALESCE($6, expertise_level),
            updated_at = now()
           WHERE user_id = $1"#,
    )
    .bind(user_id)
    .bind(name)
    .bind(role)
    .bind(org)
    .bind(style)
    .bind(expertise)
    .execute(db)
    .await
    .context("updating profile from bootstrap")?;

    Ok(())
}
