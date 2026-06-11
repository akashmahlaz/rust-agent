use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub jwt_secret: String,
    pub web_origin: String,
    pub codex_command: String,
    pub access_token_ttl_seconds: i64,
    pub cookie_secure: bool,
    pub openai_api_key: Option<String>,
    pub default_agent_model: String,
    pub workspace_root: PathBuf,
    pub internal_secret: Option<String>,
    pub jwt_audience: String,
    pub google_client_id: Option<String>,
    pub google_client_secret: Option<String>,
    pub github_client_id: Option<String>,
    pub github_client_secret: Option<String>,
    pub oauth_redirect_base: String,
    pub aws_region: Option<String>,
    pub aws_bucket_name: Option<String>,
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env::var("OPERON_API_BIND")
            .unwrap_or_else(|_| "127.0.0.1:8080".to_owned())
            .parse()
            .context("OPERON_API_BIND must be a socket address")?;

        let database_url = env::var("DATABASE_URL").context("DATABASE_URL is required")?;
        let jwt_secret = env::var("OPERON_JWT_SECRET").context("OPERON_JWT_SECRET is required")?;
        let web_origin =
            env::var("OPERON_WEB_ORIGIN").unwrap_or_else(|_| "http://localhost:3000".to_owned());
        let codex_command = env::var("OPERON_CODEX_COMMAND").unwrap_or_else(|_| {
            if cfg!(windows) {
                "codex.cmd".to_owned()
            } else {
                "codex".to_owned()
            }
        });
        let access_token_ttl_seconds = env::var("OPERON_ACCESS_TOKEN_TTL_SECONDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60 * 60 * 24 * 30); // 30 days
        let cookie_secure = env::var("OPERON_COOKIE_SECURE")
            .ok()
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
        let openai_api_key = env::var("OPENAI_API_KEY").ok().filter(|v| !v.is_empty());
        let default_agent_model = env::var("OPERON_AGENT_MODEL")
            .unwrap_or_else(|_| "gpt-4o-mini".to_owned());
        let workspace_root = env::var("OPERON_WORKSPACE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./workspaces"));
        let internal_secret = env::var("OPERON_INTERNAL_SECRET")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| {
                cfg!(debug_assertions).then(|| "operon-development-secret".to_owned())
            });
        let jwt_audience = env::var("OPERON_JWT_AUDIENCE")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "operon".to_owned());
        let google_client_id = env::var("GOOGLE_CLIENT_ID").ok().filter(|v| !v.is_empty());
        let google_client_secret = env::var("GOOGLE_CLIENT_SECRET").ok().filter(|v| !v.is_empty());
        let github_client_id = env::var("GITHUB_CLIENT_ID").ok().filter(|v| !v.is_empty());
        let github_client_secret = env::var("GITHUB_CLIENT_SECRET").ok().filter(|v| !v.is_empty());
        let oauth_redirect_base = env::var("OPERON_OAUTH_REDIRECT_BASE")
            .unwrap_or_else(|_| "http://127.0.0.1:8080".to_owned());
        let aws_region = env::var("AWS_REGION").ok().filter(|v| !v.is_empty());
        let aws_bucket_name = env::var("AWS_BUCKET_NAME").ok().filter(|v| !v.is_empty());
        let aws_access_key = env::var("AWS_ACCESS_KEY").ok().filter(|v| !v.is_empty());
        let aws_secret_key = env::var("AWS_SECRET_KEY").ok().filter(|v| !v.is_empty());

        Ok(Self {
            bind_addr,
            database_url,
            jwt_secret,
            web_origin,
            codex_command,
            access_token_ttl_seconds,
            cookie_secure,
            openai_api_key,
            default_agent_model,
            workspace_root,
            internal_secret,
            jwt_audience,
            google_client_id,
            google_client_secret,
            github_client_id,
            github_client_secret,
            oauth_redirect_base,
            aws_region,
            aws_bucket_name,
            aws_access_key,
            aws_secret_key,
        })
    }
}
