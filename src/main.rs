mod agent;
mod codex;
mod config;
mod db;
mod http;
mod state;
mod tools;

use std::{io, net::SocketAddr};

use anyhow::Result;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    agent::{AgentRegistry, ProviderRegistry},
    codex::CodexBridge,
    config::Config,
    state::AppState,
};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    init_tracing();

    let config = Config::from_env()?;
    let pool = db::connect(&config.database_url).await?;
    if let Err(error) = db::migrate(&pool).await {
        tracing::warn!(error = %error, "operonx: automatic migrations failed; continuing boot so /readyz can report the issue");
    }

    let state = AppState {
        codex: CodexBridge::new(config.codex_command.clone()),
        agents: AgentRegistry::new(),
        config: config.clone(),
        db: pool,
        providers: std::sync::Arc::new(ProviderRegistry::with_defaults()),
    };

    let app = http::router(state).layer(TraceLayer::new_for_http());
    let (listener, bound_addr) = bind_with_dev_fallback(config.bind_addr).await?;

    tracing::info!(addr = %bound_addr, "operonx api listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("operonx=debug,tower_http=info,sqlx=warn"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .init();
}

async fn bind_with_dev_fallback(preferred: SocketAddr) -> Result<(TcpListener, SocketAddr)> {
    match TcpListener::bind(preferred).await {
        Ok(listener) => return Ok((listener, preferred)),
        Err(error) if is_addr_in_use(&error) && preferred.port() != 0 => {
            tracing::warn!(
                addr = %preferred,
                error = %error,
                "preferred API port is already in use; trying nearby development ports"
            );
        }
        Err(error) => return Err(error.into()),
    }

    for port in preferred.port().saturating_add(1)..=preferred.port().saturating_add(10) {
        let candidate = SocketAddr::new(preferred.ip(), port);
        match TcpListener::bind(candidate).await {
            Ok(listener) => {
                tracing::warn!(
                    preferred = %preferred,
                    bound = %candidate,
                    "operonx bound to fallback development port"
                );
                return Ok((listener, candidate));
            }
            Err(error) if is_addr_in_use(&error) => continue,
            Err(error) => return Err(error.into()),
        }
    }

    anyhow::bail!("no available operonx API port found near {preferred}")
}

fn is_addr_in_use(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::AddrInUse
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
