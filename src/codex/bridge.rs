use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, OnceCell, broadcast, mpsc, oneshot};

const NOTIFICATION_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct CodexBridge {
    command: Arc<str>,
    session: Arc<OnceCell<Arc<Session>>>,
    init_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Serialize)]
pub struct CodexHealth {
    pub status: CodexHealthStatus,
    pub command: String,
    pub app_server_available: bool,
    pub session_started: bool,
    pub details: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexHealthStatus {
    Ready,
    Unavailable,
}

#[allow(dead_code)]
impl CodexBridge {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: Arc::from(command.into()),
            session: Arc::new(OnceCell::new()),
            init_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub async fn ensure_session(&self) -> Result<Arc<Session>> {
        if let Some(s) = self.session.get() {
            return Ok(s.clone());
        }
        let _guard = self.init_lock.lock().await;
        if let Some(s) = self.session.get() {
            return Ok(s.clone());
        }
        let session = spawn_session(self.command.as_ref()).await?;
        let _ = self.session.set(session.clone());
        Ok(session)
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let session = self.ensure_session().await?;
        session.request(method, params).await
    }

    pub async fn subscribe(&self) -> Result<broadcast::Receiver<Value>> {
        let session = self.ensure_session().await?;
        Ok(session.notifier.subscribe())
    }

    pub async fn health(&self) -> CodexHealth {
        match self.ensure_session().await {
            Ok(_) => CodexHealth {
                status: CodexHealthStatus::Ready,
                command: self.command.to_string(),
                app_server_available: true,
                session_started: true,
                details: "codex app-server initialized".to_owned(),
            },
            Err(err) => CodexHealth {
                status: CodexHealthStatus::Unavailable,
                command: self.command.to_string(),
                app_server_available: false,
                session_started: false,
                details: err.to_string(),
            },
        }
    }
}

#[allow(dead_code)]
pub struct Session {
    next_id: AtomicI64,
    writer_tx: mpsc::UnboundedSender<String>,
    pending: Arc<Mutex<HashMap<i64, oneshot::Sender<JsonRpcOutcome>>>>,
    pub notifier: broadcast::Sender<Value>,
    _child: Mutex<Child>,
}

#[derive(Debug)]
struct JsonRpcOutcome {
    result: Option<Value>,
    error: Option<Value>,
}

impl Session {
    pub async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);

        let envelope = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let line = serde_json::to_string(&envelope)?;
        self.writer_tx
            .send(line)
            .map_err(|_| anyhow!("codex writer channel closed"))?;

        let outcome = rx
            .await
            .map_err(|_| anyhow!("codex request {method} cancelled"))?;
        if let Some(err) = outcome.error {
            return Err(anyhow!("codex error: {err}"));
        }
        Ok(outcome.result.unwrap_or(Value::Null))
    }

    fn send_raw(&self, value: Value) -> Result<()> {
        let line = serde_json::to_string(&value)?;
        self.writer_tx
            .send(line)
            .map_err(|_| anyhow!("codex writer channel closed"))?;
        Ok(())
    }
}

async fn spawn_session(command: &str) -> Result<Arc<Session>> {
    tracing::info!(target: "codex", "spawning codex app-server: {command}");
    let mut child = Command::new(command)
        .arg("app-server")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to spawn `{command} app-server`"))?;

    let stdin = child.stdin.take().context("codex stdin missing")?;
    let stdout = child.stdout.take().context("codex stdout missing")?;
    let stderr = child.stderr.take().context("codex stderr missing")?;

    let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<String>();
    let pending: Arc<Mutex<HashMap<i64, oneshot::Sender<JsonRpcOutcome>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let (notifier, _) = broadcast::channel::<Value>(NOTIFICATION_CAPACITY);

    // writer task
    let mut stdin = stdin;
    tokio::spawn(async move {
        while let Some(line) = writer_rx.recv().await {
            tracing::trace!(target: "codex", "→ {line}");
            if stdin.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            if stdin.write_all(b"\n").await.is_err() {
                break;
            }
            if stdin.flush().await.is_err() {
                break;
            }
        }
    });

    // stderr drain (debug logging)
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            tracing::debug!(target: "codex", "stderr: {line}");
        }
    });

    // reader task
    let pending_reader = pending.clone();
    let notifier_reader = notifier.clone();
    let writer_tx_reader = writer_tx.clone();
    tokio::spawn(async move {
        let mut reader = BufReader::new(stdout).lines();
        loop {
            match reader.next_line().await {
                Ok(Some(line)) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    tracing::trace!(target: "codex", "← {trimmed}");
                    let value: Value = match serde_json::from_str(trimmed) {
                        Ok(v) => v,
                        Err(err) => {
                            tracing::warn!(target: "codex", "parse error: {err} line={trimmed}");
                            continue;
                        }
                    };
                    handle_inbound(&value, &pending_reader, &notifier_reader, &writer_tx_reader)
                        .await;
                }
                Ok(None) => {
                    tracing::warn!(target: "codex", "stdout closed");
                    break;
                }
                Err(err) => {
                    tracing::error!(target: "codex", "read error: {err}");
                    break;
                }
            }
        }
    });

    let session = Arc::new(Session {
        next_id: AtomicI64::new(1),
        writer_tx,
        pending,
        notifier,
        _child: Mutex::new(child),
    });

    // Initialize handshake
    let init_resp = session
        .request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "operon",
                    "title": "Operon",
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": {
                    "experimentalApi": true
                }
            }),
        )
        .await
        .context("codex initialize failed")?;
    tracing::info!(target: "codex", "initialize response: {init_resp}");

    // Send 'initialized' notification
    session.send_raw(json!({
        "jsonrpc": "2.0",
        "method": "initialized"
    }))?;

    Ok(session)
}

async fn handle_inbound(
    value: &Value,
    pending: &Arc<Mutex<HashMap<i64, oneshot::Sender<JsonRpcOutcome>>>>,
    notifier: &broadcast::Sender<Value>,
    writer_tx: &mpsc::UnboundedSender<String>,
) {
    let id = value.get("id").cloned();
    let method = value.get("method").and_then(|m| m.as_str()).map(str::to_owned);

    match (id, method) {
        (Some(id), Some(method)) => {
            let result = build_auto_approval(&method, value.get("params"));
            let response = json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result,
            });
            tracing::info!(target: "codex", "auto-approving server request: {method}");
            if let Ok(line) = serde_json::to_string(&response) {
                let _ = writer_tx.send(line);
            }
            let _ = notifier.send(json!({
                "method": "_operon/serverRequestAutoApproved",
                "params": {
                    "originalMethod": method,
                    "originalParams": value.get("params"),
                    "decision": result,
                }
            }));
        }
        (Some(id), None) => {
            if let Some(id_i64) = id.as_i64() {
                let mut map = pending.lock().await;
                if let Some(tx) = map.remove(&id_i64) {
                    let _ = tx.send(JsonRpcOutcome {
                        result: value.get("result").cloned(),
                        error: value.get("error").cloned(),
                    });
                }
            }
        }
        (None, Some(_)) => {
            let _ = notifier.send(value.clone());
        }
        (None, None) => {
            tracing::warn!(target: "codex", "unrecognized inbound: {value}");
        }
    }
}

fn build_auto_approval(method: &str, _params: Option<&Value>) -> Value {
    match method {
        "applyPatchApproval" | "execCommandApproval" => json!({ "decision": "approved" }),
        "item/fileChange/requestApproval" => json!({ "decision": "acceptForSession" }),
        "item/commandExecution/requestApproval" => json!({ "decision": "acceptForSession" }),
        "item/permissions/requestApproval" => json!({
            "permissions": {},
            "scope": "session"
        }),
        "item/tool/requestUserInput" => json!({ "answers": [] }),
        "mcpServer/elicitation/request" => json!({ "action": "decline" }),
        _ => json!({}),
    }
}
