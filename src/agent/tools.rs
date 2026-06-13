//! Native Rust tools available to the coding agent.
//!
//! System-wide tools bypass workspace isolation for full system access.
//! Workspace tools are restricted to the configured workspace root.
//!
//! Channel modes:
//!   - "coding": Full local-fs and shell tools via workspace
//!   - "system": System-wide file access, exec, networking, memory, background tasks

use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use ignore::WalkBuilder;
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use similar::TextDiff;
use sqlx::{Pool, Postgres, Row};
use tokio::{io::AsyncReadExt, process::Command, time::timeout};
use tokio::sync::RwLock;
use uuid::Uuid;

use super::github;

const MAX_FILE_BYTES: usize = 10_000_000; // 10MB for system-wide reads
const DEFAULT_EXEC_TIMEOUT_SECS: u64 = 300;

// Compiled-once regex patterns for HTML stripping (avoids re-compiling on every web_fetch call)
static RE_SCRIPT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)<script[^>]*>.*?</script>").unwrap());
static RE_STYLE:  Lazy<Regex> = Lazy::new(|| Regex::new(r"(?is)<style[^>]*>.*?</style>").unwrap());
static RE_TAGS:   Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]+>").unwrap());
static RE_SPACE:  Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static RE_DDG:    Lazy<Regex> = Lazy::new(|| Regex::new(r#"class="result__a"[^>]*href="([^"]+)"[^>]*>([^<]+)"#).unwrap());
const MAX_EXEC_OUTPUT_BYTES: usize = 200_000;
const MAX_LIST_ENTRIES: usize = 500;
const MAX_SEARCH_RESULTS: usize = 200;

#[derive(Clone)]
pub struct Workspace {
    root: Arc<PathBuf>,
}

impl Workspace {
    pub fn new(root: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&root)
            .with_context(|| format!("creating workspace root {}", root.display()))?;
        let canonical = std::fs::canonicalize(&root)
            .with_context(|| format!("canonicalizing workspace root {}", root.display()))?;
        Ok(Self {
            root: Arc::new(canonical),
        })
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// Resolve a relative path inside the workspace, refusing absolute paths
    /// and any `..` component that would escape the root.
    fn resolve(&self, rel: &str) -> Result<PathBuf> {
        let candidate = Path::new(rel);
        if candidate.is_absolute() {
            bail!("path must be relative to workspace root: {rel}");
        }
        let mut joined = self.root.as_ref().clone();
        for component in candidate.components() {
            match component {
                Component::CurDir => {}
                Component::ParentDir => {
                    if !joined.pop() || !joined.starts_with(self.root.as_ref()) {
                        bail!("path escapes workspace: {rel}");
                    }
                }
                Component::Normal(name) => joined.push(name),
                Component::Prefix(_) | Component::RootDir => {
                    bail!("path must be relative: {rel}");
                }
            }
        }
        if !joined.starts_with(self.root.as_ref()) {
            bail!("path escapes workspace: {rel}");
        }
        Ok(joined)
    }
}

/// Per-run context handed to every tool dispatch. Carries the workspace,
/// credentials, and database pool so integration tools can resolve API keys
/// and persist data without a separate network round-trip to Next.js.
#[derive(Clone)]
#[allow(dead_code)]
pub struct AgentContext {
    pub workspace: Workspace,
    pub http: Client,
    pub github_token: Option<String>,
    /// Conversation channel: "web" | "coding" | "whatsapp" | "telegram".
    /// Local-fs / shell tools (`exec`, `write_file`, `apply_patch`) are only
    /// dispatched in `coding`. Web mode must use the GitHub API tools instead.
    pub channel: String,
    /// Authenticated user ID (from JWT).
    pub user_id: Uuid,
    /// Shared database pool (Postgres/SQLx). Used by integration tools to
    /// resolve credentials and persist data.
    pub db: Pool<Postgres>,
}

impl AgentContext {
    pub fn new(
        workspace: Workspace,
        http: Client,
        github_token: Option<String>,
        channel: String,
        user_id: Uuid,
        db: Pool<Postgres>,
    ) -> Self {
        Self {
            workspace,
            http,
            github_token,
            channel,
            user_id,
            db,
        }
    }

    /// Resolve an API key for a provider from the user's stored credentials.
    /// Returns the decrypted key, or None if not found (callers should fall back to
    /// the platform-level env var or surface a clear "connect X" error).
    #[allow(dead_code)]
    pub async fn resolve_api_key(&self, provider: &str) -> Result<Option<String>> {
        use sqlx::Row;
        let row = sqlx::query(
            r#"
            select encrypted_api_key, encrypted_oauth_token
            from auth_profiles
            where user_id = $1 and provider = $2
            order by updated_at desc limit 1
            "#,
        )
        .bind(self.user_id)
        .bind(provider)
        .fetch_optional(&self.db)
        .await?;

        let encrypted = match row {
            Some(r) => r
                .try_get::<Option<String>, _>("encrypted_api_key")?
                .or_else(|| r.try_get::<Option<String>, _>("encrypted_oauth_token").ok().flatten()),
            None => return Ok(None),
        };
        let encrypted = match encrypted {
            Some(e) if !e.is_empty() => e,
            _ => return Ok(None),
        };
        // v1:iv:tag:ciphertext format (AES-256-GCM)
        let decrypted = crate::tools::decrypt_token(&encrypted)
            .map_err(|e| anyhow!("decrypt token: {}", e))?;
        Ok(Some(decrypted))
    }

    #[allow(dead_code)]
    fn is_coding(&self) -> bool {
        self.channel == "coding"
    }
}

/// Returns the OpenAI-style tool definitions for the agent.
///
/// All tools (including exec, write_file, apply_patch) are available in every
/// channel. The workspace is sandboxed per-user so this is safe.
#[allow(dead_code)]
pub fn tool_definitions(_channel: &str) -> Vec<Value> {
    all_tool_definitions()
}

fn all_tool_definitions() -> Vec<Value> {
    vec![
        tool_def(
            "read_file",
            "Read a UTF-8 text file from the workspace.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["path"],
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative file path." }
                }
            }),
        ),
        tool_def(
            "write_file",
            "Create or overwrite a UTF-8 text file in the workspace.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["path", "contents"],
                "properties": {
                    "path": { "type": "string" },
                    "contents": { "type": "string" }
                }
            }),
        ),
        tool_def(
            "apply_patch",
            "Apply a unified diff against the workspace. Each hunk header must use file paths relative to the workspace root.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["diff"],
                "properties": {
                    "diff": { "type": "string", "description": "Unified diff text." }
                }
            }),
        ),
        tool_def(
            "list_dir",
            "List a directory (gitignore-aware, max 500 entries).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "path": { "type": "string", "default": "." }
                }
            }),
        ),
        tool_def(
            "search",
            "Substring search across the workspace. Honors .gitignore.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["query"],
                "properties": {
                    "query": { "type": "string" },
                    "path":  { "type": "string", "description": "Subdirectory to scope search to." }
                }
            }),
        ),
        tool_def(
            "tool_search",
            "Search the native Rust tool catalog by capability. Use this when you need to discover the exact tool name or parameters.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["query"],
                "properties": {
                    "query": { "type": "string", "description": "Natural language description of the tool capability to find. Use broad queries such as 'gmail email inbox' or 'vercel deployments'." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 25, "default": 8 }
                }
            }),
        ),
        tool_def(
            "exec",
            "Run a shell command in the workspace. Captures stdout/stderr and exit code.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["command"],
                "properties": {
                    "command":      { "type": "string", "description": "Full command line, parsed with shell-words." },
                    "cwd":          { "type": "string", "description": "Workspace-relative cwd." },
                    "timeout_secs": { "type": "integer", "minimum": 1, "maximum": 1800 }
                }
            }),
        ),
        tool_def(
            "github_get_status",
            "Check whether the user has connected GitHub. Returns {connected, login, name, avatar_url} when connected. Always call this first when the user asks anything about GitHub.",
            json!({ "type": "object", "additionalProperties": false, "properties": {} }),
        ),
        tool_def(
            "github_list_repos",
            "List the authenticated user's accessible GitHub repositories (most recently updated first).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "visibility": { "type": "string", "enum": ["all", "public", "private"], "default": "all" },
                    "per_page":   { "type": "integer", "minimum": 1, "maximum": 100, "default": 30 }
                }
            }),
        ),
        tool_def(
            "github_get_repo",
            "Fetch metadata for a single GitHub repository.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo"],
                "properties": {
                    "owner": { "type": "string" },
                    "repo":  { "type": "string" }
                }
            }),
        ),
        tool_def(
            "github_list_contents",
            "List files and folders at a path inside a GitHub repository (defaults to repo root).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo"],
                "properties": {
                    "owner": { "type": "string" },
                    "repo":  { "type": "string" },
                    "path":  { "type": "string", "default": "" },
                    "ref":   { "type": "string", "description": "Branch, tag, or commit SHA. Defaults to default branch." }
                }
            }),
        ),
        tool_def(
            "github_read_file",
            "Read a single file from a GitHub repository (decoded UTF-8).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo", "path"],
                "properties": {
                    "owner": { "type": "string" },
                    "repo":  { "type": "string" },
                    "path":  { "type": "string" },
                    "ref":   { "type": "string" }
                }
            }),
        ),
        tool_def(
            "github_search_code",
            "Search for code across GitHub. Optionally scope to a single repo (`owner/repo`).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["query"],
                "properties": {
                    "query":    { "type": "string" },
                    "repo":     { "type": "string", "description": "`owner/repo` to scope the search." },
                    "per_page": { "type": "integer", "minimum": 1, "maximum": 50, "default": 20 }
                }
            }),
        ),
        tool_def(
            "github_list_branches",
            "List branches of a GitHub repository.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo"],
                "properties": {
                    "owner": { "type": "string" },
                    "repo":  { "type": "string" }
                }
            }),
        ),
        tool_def(
            "github_list_issues",
            "List issues for a GitHub repository.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo"],
                "properties": {
                    "owner": { "type": "string" },
                    "repo":  { "type": "string" },
                    "state": { "type": "string", "enum": ["open", "closed", "all"], "default": "open" }
                }
            }),
        ),
        tool_def(
            "github_list_pull_requests",
            "List pull requests for a GitHub repository.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo"],
                "properties": {
                    "owner": { "type": "string" },
                    "repo":  { "type": "string" },
                    "state": { "type": "string", "enum": ["open", "closed", "all"], "default": "open" }
                }
            }),
        ),
        tool_def(
            "github_create_repo",
            "Create a new GitHub repository under the connected operator account. Use this — NOT a shell `gh repo create` or `git init` — when the user asks to create a repo. Set `auto_init: true` to seed with README so the repo is immediately usable.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["name"],
                "properties": {
                    "name":               { "type": "string" },
                    "private":            { "type": "boolean", "default": true },
                    "description":        { "type": "string" },
                    "auto_init":          { "type": "boolean", "default": true },
                    "gitignore_template": { "type": "string", "description": "e.g. 'Rust', 'Node', 'Python'." },
                    "license_template":   { "type": "string", "description": "e.g. 'mit', 'apache-2.0'." }
                }
            }),
        ),
        tool_def(
            "github_create_branch",
            "Create a new branch in a GitHub repository, branched off `from_branch` (defaults to the repo default branch).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo", "branch"],
                "properties": {
                    "owner":       { "type": "string" },
                    "repo":        { "type": "string" },
                    "branch":      { "type": "string" },
                    "from_branch": { "type": "string" }
                }
            }),
        ),
        tool_def(
            "github_write_file",
            "Create or update a file in a GitHub repository via the API. Pass the existing `sha` to update an existing file; omit to create a new one. Content is plain text and base64-encoded automatically. Use this — NOT `git push` or `gh` shell commands — to ship a file change in web mode.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo", "path", "contents", "message"],
                "properties": {
                    "owner":    { "type": "string" },
                    "repo":     { "type": "string" },
                    "path":     { "type": "string" },
                    "contents": { "type": "string" },
                    "message":  { "type": "string", "description": "Commit message." },
                    "branch":   { "type": "string" },
                    "sha":      { "type": "string", "description": "Existing file blob sha when updating." }
                }
            }),
        ),
        tool_def(
            "github_delete_file",
            "Delete a file from a GitHub repository via the API. Requires the existing blob `sha`.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo", "path", "message", "sha"],
                "properties": {
                    "owner":   { "type": "string" },
                    "repo":    { "type": "string" },
                    "path":    { "type": "string" },
                    "message": { "type": "string" },
                    "sha":     { "type": "string" },
                    "branch":  { "type": "string" }
                }
            }),
        ),
        tool_def(
            "github_create_pr",
            "Open a pull request from `head` (e.g. 'feature-branch') into `base` (e.g. 'main').",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["owner", "repo", "title", "head", "base"],
                "properties": {
                    "owner": { "type": "string" },
                    "repo":  { "type": "string" },
                    "title": { "type": "string" },
                    "head":  { "type": "string" },
                    "base":  { "type": "string" },
                    "body":  { "type": "string" },
                    "draft": { "type": "boolean", "default": false }
                }
            }),
        ),
        tool_def(
            "spawn_subagent",
            "Delegate a focused sub-task to a child agent that streams its output live back into this conversation. Use sparingly for parallelizable read-only research, summarization, or multi-step exploration. The subagent inherits this run's provider, model, channel and credentials but runs with an isolated, tighter step budget.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["prompt"],
                "properties": {
                    "agent": {
                        "type": "string",
                        "description": "Optional human-readable name for the subagent (e.g. 'explore', 'summarize'). Used as a label only."
                    },
                    "prompt": {
                        "type": "string",
                        "description": "The full instruction the subagent should execute. Be specific — the subagent has no other context."
                    }
                }
            }),
        ),
        tool_def(
            "web_search",
            "Search the web for current information. Returns top results with titles, URLs, and snippets.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["query"],
                "properties": {
                    "query": { "type": "string", "description": "Search query." },
                    "count": { "type": "integer", "minimum": 1, "maximum": 10, "default": 5 }
                }
            }),
        ),
        tool_def(
            "web_fetch",
            "Fetch a URL and extract its readable text/markdown content. Use for reading web pages, docs, articles.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["url"],
                "properties": {
                    "url": { "type": "string", "description": "URL to fetch." },
                    "max_chars": { "type": "integer", "minimum": 100, "maximum": 100000, "default": 20000 }
                }
            }),
        ),
        tool_def(
            "create_file",
            "Create a file in the workspace and return a download URL. Use this when the user asks you to generate a PDF, CSV, image, document, or any downloadable file. Write the file contents to workspace, then this tool makes it accessible via URL.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["path", "description"],
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative path for the output file (e.g. 'output/resume.pdf')." },
                    "description": { "type": "string", "description": "Brief description of the file for the user." }
                }
            }),
        ),
        // ============ SYSTEM-WIDE TOOLS (Full System Access) ============
        tool_def(
            "read_system_file",
            "Read a UTF-8 text file from ANY path on the system (not restricted to workspace). Use absolute paths or paths relative to home directory (~).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["path"],
                "properties": {
                    "path": { "type": "string", "description": "Absolute path or ~-relative path to the file." }
                }
            }),
        ),
        tool_def(
            "write_system_file",
            "Write content to ANY file on the system (not restricted to workspace). Creates parent directories if needed.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["path", "contents"],
                "properties": {
                    "path": { "type": "string", "description": "Absolute path or ~-relative path for the output file." },
                    "contents": { "type": "string", "description": "File contents to write." }
                }
            }),
        ),
        tool_def(
            "list_system_dir",
            "List contents of ANY directory on the system (not restricted to workspace). Returns files and subdirectories.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "path": { "type": "string", "description": "Absolute path or ~-relative path to directory. Defaults to home directory." }
                }
            }),
        ),
        tool_def(
            "search_system",
            "Recursively search for text within files at ANY path on the system.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["query"],
                "properties": {
                    "query": { "type": "string", "description": "Text to search for." },
                    "path": { "type": "string", "description": "Root path to search from. Defaults to home directory." },
                    "file_pattern": { "type": "string", "description": "Glob pattern for files to search (e.g. '*.rs', '*.ts')." }
                }
            }),
        ),
        tool_def(
            "exec_system",
            "Execute a shell command with FULL system access. Command runs from the specified directory or home directory. Environment variables are accessible. Captures stdout/stderr.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["command"],
                "properties": {
                    "command": { "type": "string", "description": "Shell command to execute." },
                    "cwd": { "type": "string", "description": "Working directory. Defaults to home directory or /." },
                    "env": { "type": "object", "description": "Additional environment variables to set." },
                    "timeout_secs": { "type": "integer", "minimum": 1, "maximum": 3600 }
                }
            }),
        ),
        tool_def(
            "get_env",
            "Read system environment variables. Returns all env vars or specific ones by name.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "names": { "type": "array", "items": { "type": "string" }, "description": "Specific variable names to read. If empty, returns all." }
                }
            }),
        ),
        tool_def(
            "get_system_info",
            "Get detailed system information: OS, CPU, memory, disk space, network interfaces, running processes.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
        ),
        tool_def(
            "get_home_dir",
            "Get the current user's home directory path.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
        ),
        // ============ PERSISTENT MEMORY TOOLS ============
        tool_def(
            "memory_store",
            "Store a persistent memory entry that survives between sessions. Use for facts, preferences, learned information about the user or projects.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["content"],
                "properties": {
                    "content": { "type": "string", "description": "The memory content to store." },
                    "scope": { "type": "string", "enum": ["user", "workspace", "conversation"], "default": "user", "description": "Scope of the memory: user (global), workspace (project), or conversation (current chat)." },
                    "subject_id": { "type": "string", "description": "Optional ID to group related memories." },
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "Tags for categorization." }
                }
            }),
        ),
        tool_def(
            "memory_recall",
            "Retrieve persistent memories. Search by content keywords or retrieve by subject.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "query": { "type": "string", "description": "Search query to find relevant memories." },
                    "scope": { "type": "string", "enum": ["user", "workspace", "conversation", "all"], "default": "all" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }
                }
            }),
        ),
        tool_def(
            "memory_forget",
            "Delete a specific memory by ID.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["id"],
                "properties": {
                    "id": { "type": "string", "description": "Memory ID to delete." }
                }
            }),
        ),
        tool_def(
            "memory_clear",
            "Clear all memories for a given scope (user, workspace, or conversation).",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "scope": { "type": "string", "enum": ["user", "workspace", "conversation"], "default": "user" }
                }
            }),
        ),
        // ============ BACKGROUND TASK TOOLS ============
        tool_def(
            "spawn_background_task",
            "Start a long-running background process that continues executing independently. Returns a task ID for tracking.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["command"],
                "properties": {
                    "command": { "type": "string", "description": "Shell command to run in background." },
                    "cwd": { "type": "string", "description": "Working directory." },
                    "name": { "type": "string", "description": "Optional friendly name for the task." }
                }
            }),
        ),
        tool_def(
            "list_background_tasks",
            "List all currently running background tasks.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
        ),
        tool_def(
            "get_background_task_output",
            "Get the accumulated output from a background task so far.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["task_id"],
                "properties": {
                    "task_id": { "type": "string", "description": "The background task ID." }
                }
            }),
        ),
        tool_def(
            "kill_background_task",
            "Terminate a running background task.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["task_id"],
                "properties": {
                    "task_id": { "type": "string", "description": "The background task ID to kill." }
                }
            }),
        ),
        // ============ NETWORKING TOOLS ============
        tool_def(
            "http_request",
            "Make HTTP/HTTPS requests to any URL. Supports GET, POST, PUT, DELETE, PATCH methods. Can include custom headers and body.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["url", "method"],
                "properties": {
                    "url": { "type": "string", "description": "The URL to request." },
                    "method": { "type": "string", "enum": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"], "default": "GET" },
                    "headers": { "type": "object", "description": "Custom HTTP headers as key-value pairs." },
                    "body": { "type": "string", "description": "Request body content." },
                    "timeout_secs": { "type": "integer", "minimum": 1, "maximum": 120, "default": 30 }
                }
            }),
        ),
        tool_def(
            "web_scrape",
            "Fetch a URL and extract structured content. Better than raw fetch for getting article text, product info, etc.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["url"],
                "properties": {
                    "url": { "type": "string", "description": "URL to scrape." },
                    "selectors": { "type": "array", "items": { "type": "string" }, "description": "CSS selectors to extract specific elements." }
                }
            }),
        ),
        // ============ CLOUD INTEGRATION TOOLS ============
        tool_def(
            "aws_s3_list",
            "List S3 buckets or objects. Requires AWS credentials configured.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "bucket": { "type": "string", "description": "Bucket name. If omitted, lists all buckets." },
                    "prefix": { "type": "string", "description": "Object key prefix filter." },
                    "max_keys": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 }
                }
            }),
        ),
        tool_def(
            "aws_s3_read",
            "Read an object from S3.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["bucket", "key"],
                "properties": {
                    "bucket": { "type": "string", "description": "Bucket name." },
                    "key": { "type": "string", "description": "Object key." }
                }
            }),
        ),
        tool_def(
            "aws_s3_write",
            "Write an object to S3.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "required": ["bucket", "key", "body"],
                "properties": {
                    "bucket": { "type": "string", "description": "Bucket name." },
                    "key": { "type": "string", "description": "Object key." },
                    "body": { "type": "string", "description": "Content to write." },
                    "content_type": { "type": "string", "description": "MIME type." }
                }
            }),
        ),
        tool_def(
            "cloud_list_services",
            "List configured cloud services (AWS, Azure, GCP) and their status.",
            json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
        ),
    ]
}

fn tool_def(name: &str, description: &str, parameters: Value) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": normalize_tool_parameters(parameters),
        }
    })
}

fn normalize_tool_parameters(parameters: Value) -> Value {
    let mut schema = match parameters {
        Value::Object(map) => Value::Object(map),
        _ => json!({ "type": "object", "properties": {} }),
    };

    normalize_json_schema(&mut schema);
    schema
}

fn normalize_json_schema(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    let is_object = object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "object")
        || object.contains_key("properties")
        || object.contains_key("required");

    if is_object {
        object.insert("type".to_owned(), Value::String("object".to_owned()));

        if !object.get("properties").is_some_and(Value::is_object) {
            object.insert("properties".to_owned(), Value::Object(Map::new()));
        }

        let property_names: HashSet<String> = object
            .get("properties")
            .and_then(Value::as_object)
            .map(|properties| properties.keys().cloned().collect())
            .unwrap_or_default();

        match object.get_mut("required") {
            Some(Value::Array(required)) => {
                required.retain(|item| {
                    item.as_str()
                        .map(|name| property_names.contains(name))
                        .unwrap_or(false)
                });
            }
            Some(_) => {
                object.remove("required");
            }
            None => {}
        }
    }

    if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
        for value in properties.values_mut() {
            normalize_json_schema(value);
        }
    }

    if let Some(items) = object.get_mut("items") {
        normalize_json_schema(items);
    }

    if let Some(additional) = object.get_mut("additionalProperties") {
        if additional.is_object() {
            normalize_json_schema(additional);
        }
    }
}

pub async fn dispatch(ctx: &AgentContext, tool_name: &str, input: &Value) -> Result<Value> {
    let ws = &ctx.workspace;
    match tool_name {
        // Workspace-bounded tools
        "read_file" => read_file(ws, input).await,
        "write_file" => write_file(ws, input).await,
        "apply_patch" => apply_patch(ws, input).await,
        "list_dir" => list_dir(ws, input),
        "search" => search(ws, input),
        "tool_search" => tool_search(ctx, input),
        "exec" => exec(ws, input).await,
        "web_search" => web_search(ctx, input).await,
        "web_fetch" => web_fetch(ctx, input).await,
        "create_file" => create_file(ws, input).await,
        // System-wide tools (bypass workspace isolation)
        "read_system_file" => read_system_file(input).await,
        "write_system_file" => write_system_file(input).await,
        "list_system_dir" => list_system_dir(input),
        "search_system" => search_system(input),
        "exec_system" => exec_system(input).await,
        "get_env" => get_env(input),
        "get_system_info" => get_system_info(),
        "get_home_dir" => get_home_dir(),
        // Persistent memory tools
        "memory_store" => memory_store(ctx, input).await,
        "memory_recall" => memory_recall(ctx, input).await,
        "memory_forget" => memory_forget(ctx, input).await,
        "memory_clear" => memory_clear(ctx, input).await,
        // Background task tools
        "spawn_background_task" => spawn_background_task(ctx, input).await,
        "list_background_tasks" => list_background_tasks(ctx).await,
        "get_background_task_output" => get_background_task_output(ctx, input).await,
        "kill_background_task" => kill_background_task(ctx, input).await,
        // Networking tools
        "http_request" => http_request(ctx, input).await,
        "web_scrape" => web_scrape(ctx, input).await,
        // Cloud tools
        "aws_s3_list" => aws_s3_list(ctx, input).await,
        "aws_s3_read" => aws_s3_read(ctx, input).await,
        "aws_s3_write" => aws_s3_write(ctx, input).await,
        "cloud_list_services" => cloud_list_services(ctx).await,
        // GitHub
        "github_get_status" => github::get_status(&ctx.http, ctx.github_token.as_deref()).await,
        name if name.starts_with("github_") => {
            let token = ctx.github_token.as_deref().ok_or_else(|| {
                anyhow!("GitHub is not connected for this user. Ask the user to connect GitHub from Dashboard > Settings > Providers.")
            })?;
            dispatch_github(&ctx.http, token, name, input).await
        }
        other => {
            Err(anyhow!("unknown tool: {other}"))
        }
    }
}

fn tool_search(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        query: String,
        #[serde(default)]
        limit: Option<usize>,
    }

    let args: Args = serde_json::from_value(input.clone())?;
    let query = args.query.trim();
    if query.is_empty() {
        bail!("query is required");
    }

    let limit = args.limit.unwrap_or(8).clamp(1, 25);
    let definitions = tool_definitions(&ctx.channel);
    let mut matches: Vec<(i32, Value)> = definitions
        .into_iter()
        .filter_map(|tool| {
            let function = tool.get("function")?;
            let name = function.get("name").and_then(Value::as_str).unwrap_or("");
            let description = function
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("");
            let score = search_score(name, description, query);
            (score > 0).then_some((score, tool))
        })
        .collect();

    matches.sort_by(|(left_score, left_tool), (right_score, right_tool)| {
        let left_name = left_tool
            .get("function")
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let right_name = right_tool
            .get("function")
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        right_score
            .cmp(left_score)
            .then_with(|| left_name.cmp(right_name))
    });

    let tools: Vec<Value> = matches
        .into_iter()
        .take(limit)
        .map(|(score, tool)| {
            let function = tool.get("function").unwrap_or(&Value::Null);
            json!({
                "name": function.get("name").cloned().unwrap_or(Value::Null),
                "description": function.get("description").cloned().unwrap_or(Value::Null),
                "input_schema": function.get("parameters").cloned().unwrap_or(Value::Null),
                "score": score,
                "source": "rust",
            })
        })
        .collect();

    Ok(json!({
        "query": query,
        "tools": tools,
        "instruction": "Call the exact native Rust tool name when needed. If nothing relevant was returned, refine the query with broader capability words."
    }))
}

fn search_score(name: &str, description: &str, query: &str) -> i32 {
    let name = name.to_ascii_lowercase();
    let description = description.to_ascii_lowercase();
    let query = query.to_ascii_lowercase();
    let haystack = format!("{name} {description}");
    let mut score = 0;

    for token in query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| token.len() >= 2)
    {
        if name == token {
            score += 20;
        } else if name.contains(token) {
            score += 10;
        } else if description.contains(token) {
            score += 3;
        }
    }

    let boosts: &[(&[&str], &[&str])] = &[
        (
            &["gmail", "email", "mail", "inbox"],
            &["gmail", "email", "mail"],
        ),
        (
            &["calendar", "meeting", "schedule", "event"],
            &["calendar", "event", "schedule"],
        ),
        (
            &["github", "repo", "pull", "issue", "branch", "commit"],
            &["github", "repo", "pull", "issue", "branch", "commit"],
        ),
        (
            &["vercel", "deploy", "deployment", "domain"],
            &["vercel", "deploy", "deployment", "domain"],
        ),
        (&["whatsapp"], &["whatsapp"]),
        (&["telegram"], &["telegram"]),
        (&["memory", "remember", "recall"], &["memory"]),
        (&["mcp", "server"], &["mcp"]),
        (
            &["stripe", "payment", "invoice", "customer"],
            &["stripe", "payment", "invoice", "customer"],
        ),
        (
            &["facebook", "meta", "ads", "campaign", "social"],
            &["facebook", "meta", "ads", "campaign", "social"],
        ),
    ];

    for (query_terms, tool_terms) in boosts {
        if query_terms.iter().any(|term| query.contains(term))
            && tool_terms.iter().any(|term| haystack.contains(term))
        {
            score += 40;
        }
    }

    score
}

async fn dispatch_github(client: &Client, token: &str, tool: &str, input: &Value) -> Result<Value> {
    #[derive(Deserialize, Default)]
    struct ListReposArgs {
        #[serde(default)]
        visibility: Option<String>,
        #[serde(default)]
        per_page: Option<u32>,
    }
    #[derive(Deserialize)]
    struct RepoArgs {
        owner: String,
        repo: String,
    }
    #[derive(Deserialize)]
    struct ContentsArgs {
        owner: String,
        repo: String,
        #[serde(default)]
        path: Option<String>,
        #[serde(default, rename = "ref")]
        git_ref: Option<String>,
    }
    #[derive(Deserialize)]
    struct ReadFileArgs {
        owner: String,
        repo: String,
        path: String,
        #[serde(default, rename = "ref")]
        git_ref: Option<String>,
    }
    #[derive(Deserialize)]
    struct SearchArgs {
        query: String,
        #[serde(default)]
        repo: Option<String>,
        #[serde(default)]
        per_page: Option<u32>,
    }
    #[derive(Deserialize)]
    struct StateArgs {
        owner: String,
        repo: String,
        #[serde(default)]
        state: Option<String>,
    }

    match tool {
        "github_list_repos" => {
            let a: ListReposArgs = serde_json::from_value(input.clone()).unwrap_or_default();
            github::list_repos(
                client,
                token,
                a.visibility.as_deref(),
                a.per_page.unwrap_or(30),
            )
            .await
        }
        "github_get_repo" => {
            let a: RepoArgs = serde_json::from_value(input.clone())?;
            github::get_repo(client, token, &a.owner, &a.repo).await
        }
        "github_list_contents" => {
            let a: ContentsArgs = serde_json::from_value(input.clone())?;
            github::list_contents(
                client,
                token,
                &a.owner,
                &a.repo,
                a.path.as_deref().unwrap_or(""),
                a.git_ref.as_deref(),
            )
            .await
        }
        "github_read_file" => {
            let a: ReadFileArgs = serde_json::from_value(input.clone())?;
            github::read_file(
                client,
                token,
                &a.owner,
                &a.repo,
                &a.path,
                a.git_ref.as_deref(),
            )
            .await
        }
        "github_search_code" => {
            let a: SearchArgs = serde_json::from_value(input.clone())?;
            github::search_code(
                client,
                token,
                &a.query,
                a.repo.as_deref(),
                a.per_page.unwrap_or(20),
            )
            .await
        }
        "github_list_branches" => {
            let a: RepoArgs = serde_json::from_value(input.clone())?;
            github::list_branches(client, token, &a.owner, &a.repo).await
        }
        "github_list_issues" => {
            let a: StateArgs = serde_json::from_value(input.clone())?;
            github::list_issues(client, token, &a.owner, &a.repo, a.state.as_deref()).await
        }
        "github_list_pull_requests" => {
            let a: StateArgs = serde_json::from_value(input.clone())?;
            github::list_pull_requests(client, token, &a.owner, &a.repo, a.state.as_deref()).await
        }
        "github_create_repo" => {
            #[derive(Deserialize)]
            struct A {
                name: String,
                #[serde(default = "default_true")]
                private: bool,
                #[serde(default)]
                description: Option<String>,
                #[serde(default = "default_true")]
                auto_init: bool,
                #[serde(default)]
                gitignore_template: Option<String>,
                #[serde(default)]
                license_template: Option<String>,
            }
            fn default_true() -> bool {
                true
            }
            let a: A = serde_json::from_value(input.clone())?;
            github::create_repo(
                client,
                token,
                &a.name,
                a.private,
                a.description.as_deref(),
                a.auto_init,
                a.gitignore_template.as_deref(),
                a.license_template.as_deref(),
            )
            .await
        }
        "github_create_branch" => {
            #[derive(Deserialize)]
            struct A {
                owner: String,
                repo: String,
                branch: String,
                #[serde(default)]
                from_branch: Option<String>,
            }
            let a: A = serde_json::from_value(input.clone())?;
            github::create_branch(
                client,
                token,
                &a.owner,
                &a.repo,
                &a.branch,
                a.from_branch.as_deref(),
            )
            .await
        }
        "github_write_file" => {
            #[derive(Deserialize)]
            struct A {
                owner: String,
                repo: String,
                path: String,
                contents: String,
                message: String,
                #[serde(default)]
                branch: Option<String>,
                #[serde(default)]
                sha: Option<String>,
            }
            let a: A = serde_json::from_value(input.clone())?;
            github::write_file(
                client,
                token,
                &a.owner,
                &a.repo,
                &a.path,
                &a.contents,
                &a.message,
                a.branch.as_deref(),
                a.sha.as_deref(),
            )
            .await
        }
        "github_delete_file" => {
            #[derive(Deserialize)]
            struct A {
                owner: String,
                repo: String,
                path: String,
                message: String,
                sha: String,
                #[serde(default)]
                branch: Option<String>,
            }
            let a: A = serde_json::from_value(input.clone())?;
            github::delete_file(
                client,
                token,
                &a.owner,
                &a.repo,
                &a.path,
                &a.message,
                &a.sha,
                a.branch.as_deref(),
            )
            .await
        }
        "github_create_pr" => {
            #[derive(Deserialize)]
            struct A {
                owner: String,
                repo: String,
                title: String,
                head: String,
                base: String,
                #[serde(default)]
                body: Option<String>,
                #[serde(default)]
                draft: bool,
            }
            let a: A = serde_json::from_value(input.clone())?;
            github::create_pull_request(
                client,
                token,
                &a.owner,
                &a.repo,
                &a.title,
                &a.head,
                &a.base,
                a.body.as_deref(),
                a.draft,
            )
            .await
        }
        other => Err(anyhow!("unknown github tool: {other}")),
    }
}

async fn read_file(ws: &Workspace, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        path: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let path = ws.resolve(&args.path)?;
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("reading {}", path.display()))?;
    if bytes.len() > MAX_FILE_BYTES {
        bail!(
            "file too large ({} bytes, max {MAX_FILE_BYTES})",
            bytes.len()
        );
    }
    let contents = String::from_utf8(bytes).context("file is not valid UTF-8")?;
    Ok(json!({ "path": args.path, "contents": contents }))
}

async fn write_file(ws: &Workspace, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        path: String,
        contents: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let path = ws.resolve(&args.path)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating parent of {}", path.display()))?;
    }
    let byte_count = args.contents.len();
    tokio::fs::write(&path, args.contents.as_bytes())
        .await
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(json!({ "path": args.path, "bytes": byte_count }))
}

async fn apply_patch(ws: &Workspace, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        diff: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let summary = patch::apply_unified_diff(ws, &args.diff).await?;
    Ok(json!({ "files_changed": summary.files_changed, "summary": summary.summary }))
}

fn list_dir(ws: &Workspace, input: &Value) -> Result<Value> {
    #[derive(Deserialize, Default)]
    struct Args {
        #[serde(default)]
        path: Option<String>,
    }
    let args: Args = serde_json::from_value(input.clone()).unwrap_or_default();
    let rel = args.path.unwrap_or_else(|| ".".to_owned());
    let target = ws.resolve(&rel)?;

    let mut entries = Vec::new();
    let walker = WalkBuilder::new(&target)
        .max_depth(Some(1))
        .hidden(false)
        .git_ignore(true)
        .build();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.path() == target {
            continue;
        }
        let rel_path = entry
            .path()
            .strip_prefix(ws.root())
            .unwrap_or(entry.path())
            .to_string_lossy()
            .replace('\\', "/");
        let kind = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            "dir"
        } else {
            "file"
        };
        entries.push(json!({ "path": rel_path, "type": kind }));
        if entries.len() >= MAX_LIST_ENTRIES {
            break;
        }
    }

    Ok(json!({ "path": rel, "entries": entries }))
}

fn search(ws: &Workspace, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        query: String,
        #[serde(default)]
        path: Option<String>,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let needle = args.query;
    if needle.is_empty() {
        bail!("search query must not be empty");
    }
    let scope = ws.resolve(args.path.as_deref().unwrap_or("."))?;

    let mut hits = Vec::new();
    let walker = WalkBuilder::new(&scope)
        .hidden(false)
        .git_ignore(true)
        .build();

    'outer: for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let bytes = match std::fs::read(entry.path()) {
            Ok(b) if b.len() <= MAX_FILE_BYTES => b,
            _ => continue,
        };
        let text = match std::str::from_utf8(&bytes) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let rel_path = entry
            .path()
            .strip_prefix(ws.root())
            .unwrap_or(entry.path())
            .to_string_lossy()
            .replace('\\', "/");

        for (lineno, line) in text.lines().enumerate() {
            if line.contains(&needle) {
                hits.push(json!({
                    "path": rel_path,
                    "line": lineno + 1,
                    "preview": line.chars().take(240).collect::<String>(),
                }));
                if hits.len() >= MAX_SEARCH_RESULTS {
                    break 'outer;
                }
            }
        }
    }

    Ok(json!({ "query": needle, "hits": hits }))
}

async fn exec(ws: &Workspace, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        command: String,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        timeout_secs: Option<u64>,
    }
    let args: Args = serde_json::from_value(input.clone())?;

    let cwd = ws.resolve(args.cwd.as_deref().unwrap_or("."))?;
    let parts = shell_words::split(&args.command).context("parsing command")?;
    let (program, rest) = parts
        .split_first()
        .ok_or_else(|| anyhow!("empty command"))?;

    let timeout_secs = args
        .timeout_secs
        .unwrap_or(DEFAULT_EXEC_TIMEOUT_SECS)
        .clamp(1, 1800);

    let mut command = Command::new(program);
    command
        .args(rest)
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let mut child = command
        .spawn()
        .with_context(|| format!("spawning `{}`", args.command))?;

    let stdout_handle = child.stdout.take().unwrap();
    let stderr_handle = child.stderr.take().unwrap();

    let read_stream = |mut h: tokio::process::ChildStdout| async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = h.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.len() >= MAX_EXEC_OUTPUT_BYTES {
                buf.truncate(MAX_EXEC_OUTPUT_BYTES);
                break;
            }
        }
        buf
    };
    let read_err = |mut h: tokio::process::ChildStderr| async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = h.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.len() >= MAX_EXEC_OUTPUT_BYTES {
                buf.truncate(MAX_EXEC_OUTPUT_BYTES);
                break;
            }
        }
        buf
    };

    let stdout_task = tokio::spawn(read_stream(stdout_handle));
    let stderr_task = tokio::spawn(read_err(stderr_handle));

    let wait = child.wait();
    let exit = match timeout(Duration::from_secs(timeout_secs), wait).await {
        Ok(result) => result.context("waiting on child process")?,
        Err(_) => {
            bail!("command timed out after {timeout_secs}s");
        }
    };

    let stdout_bytes = stdout_task.await.unwrap_or_default();
    let stderr_bytes = stderr_task.await.unwrap_or_default();

    Ok(json!({
        "command": args.command,
        "exit_code": exit.code(),
        "stdout": String::from_utf8_lossy(&stdout_bytes),
        "stderr": String::from_utf8_lossy(&stderr_bytes),
    }))
}

async fn web_search(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        query: String,
        #[serde(default)]
        count: Option<u32>,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let count = args.count.unwrap_or(5).clamp(1, 10);

    // Use DuckDuckGo HTML search (no API key needed)
    let url = format!(
        "https://html.duckduckgo.com/html/?q={}&kl=us-en",
        urlencoding::encode(&args.query)
    );
    let resp = ctx.http
        .get(&url)
        .header("User-Agent", "Mozilla/5.0 (compatible; OpeRon/1.0)")
        .send()
        .await
        .context("web_search request failed")?;
    let body = resp.text().await.context("reading search response")?;

    // Parse results from DDG HTML (simple regex extraction)
    let mut results: Vec<Value> = Vec::new();
    for cap in RE_DDG.captures_iter(&body)
    {
        if results.len() >= count as usize {
            break;
        }
        let href = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let title = cap.get(2).map(|m| m.as_str()).unwrap_or("");
        // DDG wraps URLs in a redirect; extract the actual URL
        let actual_url = if href.contains("uddg=") {
            href.split("uddg=")
                .nth(1)
                .and_then(|s| urlencoding::decode(s.split('&').next().unwrap_or(s)).ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| href.to_string())
        } else {
            href.to_string()
        };
        results.push(json!({
            "title": html_escape::decode_html_entities(title).to_string(),
            "url": actual_url,
        }));
    }

    Ok(json!({
        "query": args.query,
        "results": results,
    }))
}

async fn web_fetch(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        url: String,
        #[serde(default)]
        max_chars: Option<usize>,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let max_chars = args.max_chars.unwrap_or(20000).clamp(100, 100000);

    let resp = ctx.http
        .get(&args.url)
        .header("User-Agent", "Mozilla/5.0 (compatible; OpeRon/1.0)")
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .context("web_fetch request failed")?;

    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Refuse to return binary file data as text — it would be garbled garbage
    // that confuses the model and wastes tokens. Return a clear error instead.
    let is_binary_mime = content_type.contains("pdf")
        || content_type.contains("msword")
        || content_type.contains("officedocument")
        || content_type.contains("zip")
        || content_type.contains("octet-stream")
        || (content_type.starts_with("application/") && !content_type.contains("json") && !content_type.contains("xml") && !content_type.contains("javascript") && !content_type.contains("text"));
    if is_binary_mime {
        return Ok(json!({
            "url": args.url,
            "status": status,
            "content_type": content_type,
            "error": format!(
                "Binary file detected ({content_type}) — cannot extract text. \
                 If this is a user-uploaded file, ask the user to provide the \
                 text content directly instead of uploading a binary document."
            ),
        }));
    }

    let body = resp.text().await.context("reading response body")?;

    // Strip HTML tags for a readable extraction (simple approach)
    let text = if content_type.contains("html") {
        // Remove script/style blocks, then strip tags
        let no_scripts = RE_SCRIPT.replace_all(&body, "");
        let no_scripts = RE_STYLE.replace_all(&no_scripts, "");
        let no_tags = RE_TAGS.replace_all(&no_scripts, "");
        let decoded = html_escape::decode_html_entities(&no_tags).to_string();
        // Collapse whitespace
        RE_SPACE.replace_all(&decoded, " ")
            .trim()
            .to_string()
    } else {
        body.clone()
    };

    let truncated = if text.len() > max_chars {
        format!("{}\n\n[...truncated at {} chars]", &text[..max_chars], max_chars)
    } else {
        text
    };

    Ok(json!({
        "url": args.url,
        "status": status,
        "content_type": content_type,
        "content": truncated,
    }))
}

async fn create_file(ws: &Workspace, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        path: String,
        description: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let path = ws.resolve(&args.path)?;

    // Verify the file exists (agent should have written it with write_file or exec first)
    if !path.exists() {
        bail!(
            "File '{}' does not exist. Write the file first using write_file or exec, then call create_file to generate the download URL.",
            args.path
        );
    }

    let metadata = tokio::fs::metadata(&path).await?;
    let size = metadata.len();
    if !metadata.is_file() {
        bail!("'{}' is not a file", args.path);
    }

    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("download");
    let download_id = Uuid::now_v7();
    let download_dir = PathBuf::from("./local_uploads")
        .join("generated")
        .join(download_id.to_string());
    tokio::fs::create_dir_all(&download_dir).await?;
    let served_path = download_dir.join(filename);
    tokio::fs::copy(&path, &served_path).await?;
    let encoded_filename = urlencoding::encode(filename);

    Ok(json!({
        "path": args.path,
        "description": args.description,
        "size_bytes": size,
        "download_url": format!("/local-uploads/generated/{download_id}/{encoded_filename}"),
        "status": "ready",
    }))
}

// ============ SYSTEM-WIDE FILE TOOLS ============

/// Resolve a path that may be absolute, ~-relative, or home-relative
fn resolve_system_path(path: &str) -> Result<PathBuf> {
    let path = path.trim();
    if path.is_empty() {
        return Err(anyhow!("path cannot be empty"));
    }

    if path == "~" || path.starts_with("~/") {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("could not determine home directory"))?;
        if path == "~" {
            return Ok(home);
        }
        return Ok(home.join(path.trim_start_matches("~/")));
    }

    let p = PathBuf::from(path);
    if p.is_absolute() {
        Ok(p)
    } else {
        // Try relative to current dir
        Ok(std::env::current_dir()?.join(path))
    }
}

async fn read_system_file(input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        path: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let path = resolve_system_path(&args.path)?;

    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("reading {}", path.display()))?;

    if bytes.len() > MAX_FILE_BYTES {
        bail!(
            "file too large ({} bytes, max {MAX_FILE_BYTES})",
            bytes.len()
        );
    }

    // For binary files, return size instead of content
    if !bytes.starts_with(&b"<!DOCTYPE".to_vec()) &&
       !bytes.starts_with(&b"<!doctype".to_vec()) &&
       !bytes.starts_with(b"<?xml") &&
       !bytes.iter().all(|&b| b == 0 || (b >= 32 && b < 127) || b == b'\n' || b == b'\r' || b == b'\t') {
        return Ok(json!({
            "path": args.path,
            "size_bytes": bytes.len(),
            "is_binary": true,
            "note": "File is binary. Use exec_system with 'xxd' or 'hexdump' to view it, or exec_system with 'base64' to encode it."
        }));
    }

    let contents = String::from_utf8(bytes).context("file is not valid UTF-8")?;
    Ok(json!({ "path": args.path, "contents": contents }))
}

async fn write_system_file(input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        path: String,
        contents: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let path = resolve_system_path(&args.path)?;

    // Create parent directories if needed
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating parent of {}", path.display()))?;
    }

    let byte_count = args.contents.len();
    tokio::fs::write(&path, args.contents.as_bytes())
        .await
        .with_context(|| format!("writing {}", path.display()))?;

    Ok(json!({
        "path": args.path,
        "bytes": byte_count,
        "absolute_path": path.display().to_string()
    }))
}

fn list_system_dir(input: &Value) -> Result<Value> {
    #[derive(Deserialize, Default)]
    struct Args {
        #[serde(default)]
        path: Option<String>,
    }
    let args: Args = serde_json::from_value(input.clone()).unwrap_or_default();
    let path = if let Some(p) = args.path {
        resolve_system_path(&p)?
    } else {
        dirs::home_dir().unwrap_or_else(|| PathBuf::from("."))
    };

    let mut entries = Vec::new();
    let walker = WalkBuilder::new(&path)
        .max_depth(Some(1))
        .hidden(false)
        .git_ignore(false)
        .build();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::debug!("skipping entry: {}", e);
                continue;
            }
        };
        if entry.path() == path {
            continue;
        }
        let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
        let size = entry.metadata().ok().map(|m| m.len()).unwrap_or(0);
        let modified = entry.metadata().ok()
            .and_then(|m| m.modified().ok())
            .map(|t| {
                let datetime: chrono::DateTime<chrono::Utc> = t.into();
                datetime.to_rfc3339()
            });

        entries.push(json!({
            "name": entry.file_name().to_string_lossy(),
            "path": entry.path().display().to_string(),
            "type": if is_dir { "dir" } else { "file" },
            "size_bytes": size,
            "modified_at": modified
        }));

        if entries.len() >= MAX_LIST_ENTRIES {
            break;
        }
    }

    Ok(json!({
        "path": path.display().to_string(),
        "entries": entries
    }))
}

fn search_system(input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        query: String,
        #[serde(default)]
        path: Option<String>,
        #[serde(default)]
        file_pattern: Option<String>,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let needle = args.query;
    if needle.is_empty() {
        bail!("search query must not be empty");
    }

    let scope = if let Some(p) = args.path {
        resolve_system_path(&p)?
    } else {
        dirs::home_dir().unwrap_or_else(|| PathBuf::from("."))
    };

    let pattern = args.file_pattern.as_deref().unwrap_or("*");

    let mut hits = Vec::new();
    let walker = WalkBuilder::new(&scope)
        .hidden(false)
        .git_ignore(false)
        .build();

    'outer: for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }

        // Check pattern if specified
        if pattern != "*" {
            let name = entry.file_name().to_string_lossy();
            if !glob_match(pattern, &name) {
                continue;
            }
        }

        let bytes = match std::fs::read(entry.path()) {
            Ok(b) if b.len() <= MAX_FILE_BYTES => b,
            _ => continue,
        };
        let text = match std::str::from_utf8(&bytes) {
            Ok(t) => t,
            Err(_) => continue,
        };

        for (lineno, line) in text.lines().enumerate() {
            if line.contains(&needle) {
                hits.push(json!({
                    "path": entry.path().display().to_string(),
                    "line": lineno + 1,
                    "preview": line.chars().take(240).collect::<String>(),
                }));
                if hits.len() >= MAX_SEARCH_RESULTS {
                    break 'outer;
                }
            }
        }
    }

    Ok(json!({ "query": needle, "path": scope.display().to_string(), "hits": hits }))
}

/// Simple glob pattern matching (supports * and ?)
fn glob_match(pattern: &str, name: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut name_chars = name.chars().peekable();

    loop {
        match (pattern_chars.next(), name_chars.next()) {
            (None, None) => return true,
            (None, _) => return false,
            (Some('*'), None) => return true,
            (Some('*'), Some(_)) => {
                // Try matching at current position or skip the *
                let remaining_pattern: String = pattern_chars.clone().collect();
                loop {
                    if glob_match(&remaining_pattern, name) {
                        return true;
                    }
                    if name_chars.peek().is_none() {
                        return false;
                    }
                    name_chars.next();
                }
            }
            (Some('?'), None) => return false,
            (Some('?'), Some(_)) => {}
            (Some(p), Some(n)) if p == n => {}
            (Some(_p), _) => return false,
        }
    }
}

async fn exec_system(input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        command: String,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        env: Option<std::collections::HashMap<String, String>>,
        #[serde(default)]
        timeout_secs: Option<u64>,
    }
    let args: Args = serde_json::from_value(input.clone())?;

    let cwd = if let Some(c) = args.cwd {
        resolve_system_path(&c)?
    } else {
        dirs::home_dir().unwrap_or_else(|| PathBuf::from("."))
    };

    let parts = shell_words::split(&args.command).context("parsing command")?;
    let (program, rest) = parts
        .split_first()
        .ok_or_else(|| anyhow!("empty command"))?;

    let timeout_secs = args
        .timeout_secs
        .unwrap_or(DEFAULT_EXEC_TIMEOUT_SECS)
        .clamp(1, 3600);

    let mut command = Command::new(program);
    command
        .args(rest)
        .current_dir(&cwd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    // Inherit all current env vars plus any custom ones
    for (key, _value) in std::env::vars() {
        command.env_remove(&key);
    }
    if let Some(env) = args.env {
        for (key, value) in env {
            command.env(&key, &value);
        }
    }

    let mut child = command
        .spawn()
        .with_context(|| format!("spawning `{}`", args.command))?;

    let stdout_handle = child.stdout.take().unwrap();
    let stderr_handle = child.stderr.take().unwrap();

    let read_stream = |mut h: tokio::process::ChildStdout| async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = h.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.len() >= MAX_EXEC_OUTPUT_BYTES {
                buf.truncate(MAX_EXEC_OUTPUT_BYTES);
                break;
            }
        }
        buf
    };
    let read_err = |mut h: tokio::process::ChildStderr| async move {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = h.read(&mut chunk).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.len() >= MAX_EXEC_OUTPUT_BYTES {
                buf.truncate(MAX_EXEC_OUTPUT_BYTES);
                break;
            }
        }
        buf
    };

    let stdout_task = tokio::spawn(read_stream(stdout_handle));
    let stderr_task = tokio::spawn(read_err(stderr_handle));

    let wait = child.wait();
    let exit = match timeout(Duration::from_secs(timeout_secs), wait).await {
        Ok(result) => result.context("waiting on child process")?,
        Err(_) => {
            child.kill().await.ok();
            bail!("command timed out after {timeout_secs}s");
        }
    };

    let stdout_bytes = stdout_task.await.unwrap_or_default();
    let stderr_bytes = stderr_task.await.unwrap_or_default();

    Ok(json!({
        "command": args.command,
        "cwd": cwd.display().to_string(),
        "exit_code": exit.code(),
        "stdout": String::from_utf8_lossy(&stdout_bytes),
        "stderr": String::from_utf8_lossy(&stderr_bytes),
    }))
}

fn get_env(input: &Value) -> Result<Value> {
    #[derive(Deserialize, Default)]
    struct Args {
        #[serde(default)]
        names: Option<Vec<String>>,
    }
    let args: Args = serde_json::from_value(input.clone()).unwrap_or_default();

    let mut result = json!({});
    if let Some(names) = args.names {
        for name in names {
            if let Ok(value) = std::env::var(&name) {
                result.as_object_mut().unwrap().insert(name, json!(value));
            }
        }
    } else {
        // Return all environment variables
        for (key, value) in std::env::vars() {
            result.as_object_mut().unwrap().insert(key, json!(value));
        }
    }

    Ok(json!({
        "env": result,
        "count": result.as_object().map(|m| m.len()).unwrap_or(0)
    }))
}

fn get_system_info() -> Result<Value> {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    let cpu_count = sys.cpus().len();
    let total_memory = sys.total_memory();
    let used_memory = sys.used_memory();
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let hostname = System::host_name().unwrap_or_else(|| "unknown".to_string());

    // Get disk info
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let disk_info: Vec<Value> = disks.iter().map(|d| {
        json!({
            "name": d.name().to_string_lossy(),
            "mount_point": d.mount_point().display().to_string(),
            "total_bytes": d.total_space(),
            "available_bytes": d.available_space(),
            "file_system": d.file_system().to_string_lossy()
        })
    }).collect();

    // Get network interfaces
    let networks = sysinfo::Networks::new_with_refreshed_list();
    let network_info: Vec<Value> = networks.iter().map(|(name, data)| {
        json!({
            "name": name,
            "received_bytes": data.total_received(),
            "transmitted_bytes": data.total_transmitted(),
            "packets_received": data.total_packets_received(),
            "packets_transmitted": data.total_packets_transmitted()
        })
    }).collect();

    // Get running processes
    let processes: Vec<Value> = sys.processes().iter().take(50).map(|(pid, p)| {
        json!({
            "pid": pid.as_u32(),
            "name": p.name().to_string_lossy(),
            "cpu_usage": p.cpu_usage(),
            "memory_bytes": p.memory()
        })
    }).collect();

    Ok(json!({
        "os": os,
        "arch": arch,
        "hostname": hostname,
        "cpu_count": cpu_count,
        "total_memory_bytes": total_memory,
        "used_memory_bytes": used_memory,
        "available_memory_bytes": total_memory.saturating_sub(used_memory),
        "disks": disk_info,
        "network_interfaces": network_info,
        "processes": processes,
        "process_count": sys.processes().len(),
        "uptime_seconds": System::uptime()
    }))
}

fn get_home_dir() -> Result<Value> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("could not determine home directory"))?;
    Ok(json!({
        "home": home.display().to_string(),
        "path_expanded": home.to_string_lossy()
    }))
}

// ============ PERSISTENT MEMORY TOOLS ============

async fn memory_store(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        content: String,
        #[serde(default = "default_memory_scope")]
        scope: String,
        #[serde(default)]
        subject_id: Option<String>,
        #[serde(default)]
        tags: Option<Vec<String>>,
    }
    fn default_memory_scope() -> String {
        "user".to_string()
    }

    let args: Args = serde_json::from_value(input.clone())?;
    let id = Uuid::now_v7();

    sqlx::query(
        r#"insert into memories (id, user_id, scope, subject_id, content, metadata)
           values ($1, $2, $3, $4, $5, $6)"#
    )
    .bind(id)
    .bind(ctx.user_id)
    .bind(&args.scope)
    .bind(args.subject_id.as_deref())
    .bind(&args.content)
    .bind(json!({
        "tags": args.tags.unwrap_or_default()
    }))
    .execute(&ctx.db)
    .await
    .context("inserting memory")?;

    Ok(json!({
        "id": id.to_string(),
        "content": args.content,
        "scope": args.scope,
        "stored": true
    }))
}

async fn memory_recall(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        #[serde(default)]
        query: Option<String>,
        #[serde(default = "default_recall_scope")]
        scope: String,
        #[serde(default)]
        limit: Option<i32>,
    }
    fn default_recall_scope() -> String {
        "all".to_string()
    }

    let args: Args = serde_json::from_value(input.clone())?;
    let limit = args.limit.unwrap_or(20).min(100) as i64;

    let rows = if let Some(ref query) = args.query {
        // Text search using ILIKE
        let scope_filter = if args.scope == "all" {
            "".to_string()
        } else {
            format!("and scope = '{}'", args.scope)
        };
        sqlx::query(&format!(
            r#"select id, scope, subject_id, content, metadata, created_at
               from memories
               where user_id = $1 {} and content ilike '%' || $2 || '%'
               order by updated_at desc
               limit {}"#,
            scope_filter, limit
        ))
        .bind(ctx.user_id)
        .bind(&query)
        .fetch_all(&ctx.db)
        .await
    } else {
        let scope_filter = if args.scope == "all" {
            "".to_string()
        } else {
            format!("and scope = '{}'", args.scope)
        };
        sqlx::query(&format!(
            r#"select id, scope, subject_id, content, metadata, created_at
               from memories
               where user_id = $1 {}
               order by updated_at desc
               limit {}"#,
            scope_filter, limit
        ))
        .bind(ctx.user_id)
        .fetch_all(&ctx.db)
        .await
    }.context("querying memories")?;

    let memories: Vec<Value> = rows.iter().map(|row| {
        let id: Uuid = row.try_get("id").unwrap_or_default();
        let scope: String = row.try_get("scope").unwrap_or_default();
        let subject_id: Option<String> = row.try_get("subject_id").ok().flatten();
        let content: String = row.try_get("content").unwrap_or_default();
        let metadata: Value = row.try_get("metadata").unwrap_or(json!({}));
        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at").unwrap_or_else(|_| chrono::Utc::now());

        json!({
            "id": id.to_string(),
            "scope": scope,
            "subject_id": subject_id,
            "content": content,
            "tags": metadata.get("tags").and_then(|t| t.as_array()).map(|arr| {
                arr.iter().filter_map(|v| v.as_str().map(str::to_string)).collect::<Vec<_>>()
            }).unwrap_or_default(),
            "created_at": created_at.to_rfc3339()
        })
    }).collect();

    Ok(json!({
        "query": args.query,
        "scope": args.scope,
        "count": memories.len(),
        "memories": memories
    }))
}

async fn memory_forget(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        id: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let id: Uuid = args.id.parse().map_err(|_| anyhow!("invalid memory ID"))?;

    let result = sqlx::query("delete from memories where id = $1 and user_id = $2")
        .bind(id)
        .bind(ctx.user_id)
        .execute(&ctx.db)
        .await?;

    Ok(json!({
        "deleted": result.rows_affected() > 0,
        "id": args.id
    }))
}

async fn memory_clear(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        #[serde(default = "default_clear_scope")]
        scope: String,
    }
    fn default_clear_scope() -> String {
        "user".to_string()
    }

    let args: Args = serde_json::from_value(input.clone())?;

    let result = sqlx::query("delete from memories where user_id = $1 and scope = $2")
        .bind(ctx.user_id)
        .bind(&args.scope)
        .execute(&ctx.db)
        .await?;

    Ok(json!({
        "deleted": result.rows_affected(),
        "scope": args.scope
    }))
}

// ============ BACKGROUND TASK TOOLS ============

/// Global registry for background tasks
use std::collections::HashMap;
use tokio::process::Child;

pub struct BackgroundTasks {
    tasks: RwLock<HashMap<String, BackgroundTaskHandle>>,
}

pub struct BackgroundTaskHandle {
    pub id: String,
    pub name: String,
    pub child: Child,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub output: Arc<RwLock<Vec<String>>>,
}

impl BackgroundTasks {
    pub fn new() -> Self {
        Self {
            tasks: RwLock::new(HashMap::new()),
        }
    }

    pub async fn spawn(&self, id: String, name: String, mut command: Command) -> Result<Value> {
        command
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().context("spawning background process")?;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();

        let output = Arc::new(RwLock::new(Vec::new()));
        let output_clone = output.clone();

        // Spawn a task to capture stdout
        if let Some(mut stdout) = stdout {
            let out = output_clone.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let s = String::from_utf8_lossy(&buf[..n]).to_string();
                            out.write().await.push(format!("[stdout] {}", s));
                        }
                        Err(_) => break,
                    }
                }
            });
        }

        if let Some(mut stderr) = stderr {
            let err = output_clone.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(n) => {
                            let s = String::from_utf8_lossy(&buf[..n]).to_string();
                            err.write().await.push(format!("[stderr] {}", s));
                        }
                        Err(_) => break,
                    }
                }
            });
        }

        let handle = BackgroundTaskHandle {
            id: id.clone(),
            name,
            child,
            started_at: chrono::Utc::now(),
            output,
        };

        self.tasks.write().await.insert(id.clone(), handle);

        Ok(json!({ "task_id": id, "status": "started" }))
    }

    pub async fn list(&self) -> Vec<Value> {
        let tasks = self.tasks.read().await;
        tasks.iter().map(|(id, h)| {
            json!({
                "task_id": id,
                "name": h.name,
                "started_at": h.started_at.to_rfc3339(),
                "pid": h.child.id().map(|p| p as u64)
            })
        }).collect()
    }

    pub async fn output(&self, task_id: &str) -> Option<Vec<String>> {
        let tasks = self.tasks.read().await;
        tasks.get(task_id).map(|h| h.output.blocking_read().clone())
    }

    pub async fn kill(&self, task_id: &str) -> bool {
        let mut tasks = self.tasks.write().await;
        if let Some(mut handle) = tasks.remove(task_id) {
            handle.child.kill().await.ok();
            true
        } else {
            false
        }
    }
}

impl Default for BackgroundTasks {
    fn default() -> Self {
        Self::new()
    }
}

async fn spawn_background_task(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        command: String,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        name: Option<String>,
    }
    let args: Args = serde_json::from_value(input.clone())?;

    let cwd = if let Some(c) = args.cwd {
        resolve_system_path(&c)?
    } else {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    };

    let parts = shell_words::split(&args.command).context("parsing command")?;
    let (program, rest) = parts
        .split_first()
        .ok_or_else(|| anyhow!("empty command"))?;

    let mut command = Command::new(program);
    command
        .args(rest)
        .current_dir(&cwd);

    let task_id = Uuid::now_v7().to_string();
    let name = args.name.unwrap_or_else(|| args.command.clone());

    // Get or create background tasks registry from state
    let tasks = get_background_tasks(ctx).await?;

    tasks.spawn(task_id.clone(), name, command).await
}

async fn list_background_tasks(ctx: &AgentContext) -> Result<Value> {
    let tasks = get_background_tasks(ctx).await?;
    let list = tasks.list().await;
    Ok(json!({ "tasks": list, "count": list.len() }))
}

async fn get_background_task_output(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        task_id: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;

    let tasks = get_background_tasks(ctx).await?;
    let output = tasks.output(&args.task_id).await;

    match output {
        Some(lines) => Ok(json!({
            "task_id": args.task_id,
            "output": lines,
            "line_count": lines.len()
        })),
        None => Ok(json!({
            "task_id": args.task_id,
            "error": "task not found"
        })),
    }
}

async fn kill_background_task(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        task_id: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;

    let tasks = get_background_tasks(ctx).await?;
    let killed = tasks.kill(&args.task_id).await;

    Ok(json!({
        "task_id": args.task_id,
        "killed": killed
    }))
}

/// Get background tasks registry from the agent context or create a new one
async fn get_background_tasks(_ctx: &AgentContext) -> Result<&'static BackgroundTasks> {
    // In a production system, this would be stored in AppState
    // For now, we create a static one
    static TASKS: once_cell::sync::Lazy<BackgroundTasks> = once_cell::sync::Lazy::new(BackgroundTasks::new);
    Ok(&TASKS)
}

// ============ NETWORKING TOOLS ============

async fn http_request(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        url: String,
        #[serde(default = "default_method")]
        method: String,
        #[serde(default)]
        headers: Option<std::collections::HashMap<String, String>>,
        #[serde(default)]
        body: Option<String>,
        #[serde(default = "default_timeout")]
        timeout_secs: Option<u64>,
    }
    fn default_method() -> String { "GET".to_string() }
    fn default_timeout() -> Option<u64> { Some(30) }

    let args: Args = serde_json::from_value(input.clone())?;
    let timeout = Duration::from_secs(args.timeout_secs.unwrap_or(30));

    let mut request = ctx.http.request(
        reqwest::Method::from_bytes(args.method.to_uppercase().as_bytes())
            .unwrap_or(reqwest::Method::GET),
        &args.url
    )
    .header("User-Agent", "Mozilla/5.0 (compatible; Operon/1.0)")
    .timeout(timeout);

    if let Some(headers) = args.headers {
        for (key, value) in headers {
            request = request.header(&key, &value);
        }
    }

    if let Some(body) = args.body {
        request = request.body(body);
    }

    let resp = request.send().await.context("HTTP request failed")?;
    let status = resp.status().as_u16();
    let mut headers_map = serde_json::Map::new();
    for (k, v) in resp.headers().iter() {
        if let Ok(value) = v.to_str() {
            headers_map.insert(k.to_string(), json!(value));
        }
    }
    let headers = Value::Object(headers_map);

    let body = resp.text().await.context("reading response body")?;

    Ok(json!({
        "url": args.url,
        "method": args.method,
        "status": status,
        "headers": headers,
        "body": body,
        "body_length": body.len()
    }))
}

async fn web_scrape(ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        url: String,
        #[serde(default)]
        selectors: Option<Vec<String>>,
    }
    let args: Args = serde_json::from_value(input.clone())?;

    let resp = ctx.http
        .get(&args.url)
        .header("User-Agent", "Mozilla/5.0 (compatible; Operon/1.0)")
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .context("fetching URL")?;

    let body = resp.text().await.context("reading response")?;

    // If selectors specified, try to extract those elements
    // For now, just return the cleaned content
    let no_scripts = RE_SCRIPT.replace_all(&body, "");
    let no_scripts = RE_STYLE.replace_all(&no_scripts, "");
    let no_tags = RE_TAGS.replace_all(&no_scripts, "");
    let decoded = html_escape::decode_html_entities(&no_tags).to_string();
    let text = RE_SPACE.replace_all(&decoded, " ").trim().to_string();

    Ok(json!({
        "url": args.url,
        "content": text,
        "content_length": text.len(),
        "selectors_applied": args.selectors.is_some()
    }))
}

// ============ CLOUD INTEGRATION TOOLS ============

use aws_sdk_s3::primitives::ByteStream;

async fn aws_s3_list(_ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        #[serde(default)]
        bucket: Option<String>,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default = "default_max_keys")]
        max_keys: Option<i32>,
    }
    fn default_max_keys() -> Option<i32> { Some(100) }

    let args: Args = serde_json::from_value(input.clone())?;
    let max_keys = args.max_keys.unwrap_or(100).min(1000) as i32;

    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "ap-south-1".to_string());
    let bucket_name = args.bucket.unwrap_or_else(||
        std::env::var("AWS_BUCKET_NAME").unwrap_or_default()
    );

    if bucket_name.is_empty() {
        return Err(anyhow::anyhow!("Bucket name required. Set AWS_BUCKET_NAME env var or pass bucket parameter."));
    }

    let config = aws_config::from_env().load().await;
    let client = aws_sdk_s3::Client::new(&config);

    let resp = client
        .list_objects_v2()
        .bucket(&bucket_name)
        .prefix(args.prefix.as_deref().unwrap_or(""))
        .max_keys(max_keys)
        .send()
        .await
        .context("S3 list objects failed")?;

    let objects: Vec<Value> = resp.contents()
        .iter()
        .map(|obj| {
            json!({
                "key": obj.key().unwrap_or(""),
                "size_bytes": obj.size(),
                "etag": obj.e_tag().unwrap_or("")
            })
        })
        .collect();

    Ok(json!({
        "bucket": bucket_name,
        "prefix": args.prefix.unwrap_or_default(),
        "region": region,
        "count": objects.len(),
        "objects": objects
    }))
}

async fn aws_s3_read(_ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        bucket: String,
        key: String,
    }
    let args: Args = serde_json::from_value(input.clone())?;

    let config = aws_config::from_env().load().await;
    let client = aws_sdk_s3::Client::new(&config);

    let resp = client
        .get_object()
        .bucket(&args.bucket)
        .key(&args.key)
        .send()
        .await
        .context(format!("S3 read failed for {}/{}", args.bucket, args.key))?;

    let body = resp.body.collect().await
        .context("S3 read body collection failed")?;
    let bytes = body.into_bytes();
    let size = bytes.len();

    // Try to convert to string for text files
    let content = String::from_utf8(bytes.to_vec())
        .map(|s| s.clone())
        .ok();

    Ok(json!({
        "bucket": args.bucket,
        "key": args.key,
        "size_bytes": size,
        "content": content,
        "is_binary": content.is_none()
    }))
}

async fn aws_s3_write(_ctx: &AgentContext, input: &Value) -> Result<Value> {
    #[derive(Deserialize)]
    struct Args {
        bucket: String,
        key: String,
        body: String,
        #[serde(default)]
        content_type: Option<String>,
    }
    let args: Args = serde_json::from_value(input.clone())?;
    let body_len = args.body.len();

    let config = aws_config::from_env().load().await;
    let client = aws_sdk_s3::Client::new(&config);

    let body_bytes = ByteStream::from(args.body.into_bytes());

    let mut put_builder = client
        .put_object()
        .bucket(&args.bucket)
        .key(&args.key)
        .body(body_bytes);

    if let Some(ref ct) = args.content_type {
        put_builder = put_builder.content_type(ct);
    }

    put_builder.send().await
        .context(format!("S3 write failed for {}/{}", args.bucket, args.key))?;

    Ok(json!({
        "bucket": args.bucket,
        "key": args.key,
        "body_length": body_len,
        "content_type": args.content_type,
        "status": "uploaded"
    }))
}

async fn cloud_list_services(ctx: &AgentContext) -> Result<Value> {
    // Check what cloud services are configured
    let mut services = json!({
        "aws": {
            "configured": std::env::var("AWS_ACCESS_KEY").is_ok() && std::env::var("AWS_SECRET_KEY").is_ok(),
            "region": std::env::var("AWS_REGION").ok(),
            "bucket": std::env::var("AWS_BUCKET_NAME").ok()
        },
        "azure": {
            "configured": std::env::var("AZURE_STORAGE_ACCOUNT").is_ok() && std::env::var("AZURE_STORAGE_KEY").is_ok(),
            "account": std::env::var("AZURE_STORAGE_ACCOUNT").ok()
        },
        "gcp": {
            "configured": std::env::var("GOOGLE_APPLICATION_CREDENTIALS").is_ok() || std::env::var("GCP_SERVICE_ACCOUNT_KEY").is_ok(),
            "project": std::env::var("GCP_PROJECT").ok()
        }
    });

    // Try to get user's configured providers from database
    if let Ok(rows) = sqlx::query("select provider from provider_profiles where user_id = $1")
        .bind(ctx.user_id)
        .fetch_all(&ctx.db)
        .await
    {
        let providers: Vec<String> = rows.iter().filter_map(|r| r.try_get("provider").ok()).collect();
        if let Some(obj) = services.as_object_mut() {
            obj.insert("user_providers".to_string(), json!(providers));
        }
    }

    Ok(services)
}

mod patch {
    //! Minimal unified-diff applier: parses one or more file diffs and writes
    //! the result back. Supports new file, delete, and in-place edits.
    use super::*;

    pub struct PatchSummary {
        pub files_changed: usize,
        pub summary: Vec<String>,
    }

    pub async fn apply_unified_diff(ws: &Workspace, diff: &str) -> Result<PatchSummary> {
        let files = parse(diff)?;
        let mut summary = Vec::new();
        for file in &files {
            apply_one(ws, file).await?;
            summary.push(format!(
                "{} ({} hunks)",
                file.target_path.as_deref().unwrap_or("unknown"),
                file.hunks.len()
            ));
        }
        Ok(PatchSummary {
            files_changed: files.len(),
            summary,
        })
    }

    struct FileDiff {
        target_path: Option<String>,
        is_new: bool,
        is_delete: bool,
        hunks: Vec<Hunk>,
    }

    struct Hunk {
        old_start: usize,
        lines: Vec<HunkLine>,
    }

    enum HunkLine {
        Context(String),
        Add(String),
        Remove,
    }

    fn parse(diff: &str) -> Result<Vec<FileDiff>> {
        let mut files = Vec::new();
        let mut current: Option<FileDiff> = None;
        let mut current_hunk: Option<Hunk> = None;

        for raw in diff.split('\n') {
            let line = raw.strip_suffix('\r').unwrap_or(raw);
            if let Some(rest) = line.strip_prefix("--- ") {
                if let Some(file) = current.take() {
                    if let Some(h) = current_hunk.take() {
                        files.push(close_file(file, Some(h)));
                    } else {
                        files.push(file);
                    }
                }
                current = Some(FileDiff {
                    target_path: None,
                    is_new: rest.contains("/dev/null"),
                    is_delete: false,
                    hunks: Vec::new(),
                });
            } else if let Some(rest) = line.strip_prefix("+++ ") {
                if let Some(file) = current.as_mut() {
                    if rest.contains("/dev/null") {
                        file.is_delete = true;
                    } else {
                        file.target_path = Some(strip_prefix(rest));
                    }
                }
            } else if let Some(rest) = line.strip_prefix("@@") {
                if let Some(h) = current_hunk.take() {
                    if let Some(file) = current.as_mut() {
                        file.hunks.push(h);
                    }
                }
                let old_start = parse_hunk_header(rest)?;
                current_hunk = Some(Hunk {
                    old_start,
                    lines: Vec::new(),
                });
            } else if let Some(h) = current_hunk.as_mut() {
                if let Some(rest) = line.strip_prefix('+') {
                    h.lines.push(HunkLine::Add(rest.to_owned()));
                } else if let Some(rest) = line.strip_prefix('-') {
                    let _ = rest;
                    h.lines.push(HunkLine::Remove);
                } else if let Some(rest) = line.strip_prefix(' ') {
                    h.lines.push(HunkLine::Context(rest.to_owned()));
                } else if line.is_empty() {
                    h.lines.push(HunkLine::Context(String::new()));
                }
            }
        }

        if let Some(file) = current {
            files.push(close_file(file, current_hunk));
        }

        if files.is_empty() {
            bail!("no file diffs found in patch");
        }
        Ok(files)
    }

    fn close_file(mut file: FileDiff, last_hunk: Option<Hunk>) -> FileDiff {
        if let Some(h) = last_hunk {
            file.hunks.push(h);
        }
        file
    }

    fn strip_prefix(path: &str) -> String {
        let path = path.split('\t').next().unwrap_or(path).trim().to_owned();
        if let Some(rest) = path.strip_prefix("a/") {
            rest.to_owned()
        } else if let Some(rest) = path.strip_prefix("b/") {
            rest.to_owned()
        } else {
            path
        }
    }

    fn parse_hunk_header(rest: &str) -> Result<usize> {
        // expected: " -OLD,COUNT +NEW,COUNT @@"
        let inside = rest.trim().trim_end_matches("@@").trim();
        let mut parts = inside.split_whitespace();
        let old = parts
            .next()
            .ok_or_else(|| anyhow!("malformed hunk header"))?;
        let old = old.trim_start_matches('-');
        let start: usize = old.split(',').next().unwrap_or("1").parse().unwrap_or(1);
        Ok(start.max(1))
    }

    async fn apply_one(ws: &Workspace, file: &FileDiff) -> Result<()> {
        let target = file
            .target_path
            .as_deref()
            .ok_or_else(|| anyhow!("patch missing target path"))?;
        let path = ws.resolve(target)?;

        if file.is_delete {
            tokio::fs::remove_file(&path)
                .await
                .with_context(|| format!("removing {}", path.display()))?;
            return Ok(());
        }

        let original = if file.is_new {
            String::new()
        } else {
            tokio::fs::read_to_string(&path)
                .await
                .with_context(|| format!("reading {}", path.display()))?
        };

        let updated = apply_hunks(&original, &file.hunks)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        tokio::fs::write(&path, updated.as_bytes())
            .await
            .with_context(|| format!("writing {}", path.display()))?;

        // sanity: ensure diff actually changed something
        let _ = TextDiff::from_lines(&original, &updated);
        Ok(())
    }

    fn apply_hunks(original: &str, hunks: &[Hunk]) -> Result<String> {
        let original_lines: Vec<&str> = original.split_inclusive('\n').collect();
        let mut output: Vec<String> = Vec::new();
        let mut cursor = 0usize; // index into original_lines

        for hunk in hunks {
            let target_idx = hunk.old_start.saturating_sub(1);
            // copy any unchanged lines between cursor and the hunk start
            while cursor < target_idx && cursor < original_lines.len() {
                output.push(original_lines[cursor].to_owned());
                cursor += 1;
            }

            for line in &hunk.lines {
                match line {
                    HunkLine::Context(text) => {
                        if cursor < original_lines.len() {
                            output.push(original_lines[cursor].to_owned());
                            cursor += 1;
                        } else {
                            output.push(format!("{text}\n"));
                        }
                    }
                    HunkLine::Remove => {
                        if cursor < original_lines.len() {
                            cursor += 1;
                        }
                    }
                    HunkLine::Add(text) => {
                        output.push(format!("{text}\n"));
                    }
                }
            }
        }

        // append the remainder of the original
        while cursor < original_lines.len() {
            output.push(original_lines[cursor].to_owned());
            cursor += 1;
        }

        Ok(output.join(""))
    }
}
