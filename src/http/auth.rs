use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, header},
    response::Redirect,
};
use base64ct::{Base64UrlUnpadded, Encoding};
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    EndpointNotSet, EndpointSet,
    RedirectUrl, Scope,
    TokenResponse, TokenUrl, basic::BasicClient,
};
use oauth2::reqwest;

type GoogleClient = oauth2::Client<
    oauth2::basic::BasicErrorResponse,
    oauth2::basic::BasicTokenResponse,
    oauth2::basic::BasicTokenIntrospectionResponse,
    oauth2::StandardRevocableToken,
    oauth2::basic::BasicRevocationErrorResponse,
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointSet,
>;
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::{Error as SqlxError, Row};
use uuid::Uuid;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

const ACCESS_COOKIE: &str = "operon_access_token";
const TOKEN_VERSION: &str = "v1";

type HmacSha256 = Hmac<Sha256>;

#[derive(Deserialize)]
pub struct SignupRequest {
    email: String,
    password: String,
    display_name: Option<String>,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    user: UserResponse,
    access_token: String,
    expires_at: i64,
}

#[derive(Serialize)]
pub struct UserResponse {
    id: Uuid,
    email: String,
    display_name: Option<String>,
    name: Option<String>,
    image: Option<String>,
}

#[derive(Deserialize)]
pub struct OAuthCallbackQuery {
    code: String,
    #[serde(default)]
    state: Option<String>,
}

#[derive(Deserialize)]
struct GoogleUserInfo {
    email: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    picture: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: Uuid,
    email: String,
    iss: String,
    aud: String,
    exp: usize,
    iat: usize,
}

struct UserRecord {
    id: Uuid,
    email: String,
    display_name: Option<String>,
    avatar_url: Option<String>,
    password_hash: Option<String>,
}

pub async fn signup(
    State(state): State<AppState>,
    Json(payload): Json<SignupRequest>,
) -> AppResult<(HeaderMap, Json<AuthResponse>)> {
    validate_email_password(&payload.email, &payload.password)?;

    let user_id = Uuid::now_v7();
    let password_hash = hash_password(&payload.password)?;

    let result = sqlx::query(
        "insert into users (id, email, display_name, password_hash) values ($1, lower($2), $3, $4) returning id, email, display_name, avatar_url, password_hash",
    )
    .bind(user_id)
    .bind(payload.email.trim())
    .bind(payload.display_name.as_deref().map(str::trim).filter(|value| !value.is_empty()))
    .bind(password_hash)
    .fetch_one(&state.db)
    .await;

    let row = match result {
        Ok(row) => row,
        Err(SqlxError::Database(error)) if error.constraint() == Some("users_email_key") => {
            return Err(AppError::Conflict("email already exists".to_owned()));
        }
        Err(error) => return Err(error.into()),
    };

    let user = user_from_row(row)?;
    issue_auth_response(&state, user)
}

pub async fn login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> AppResult<(HeaderMap, Json<AuthResponse>)> {
    validate_email(&payload.email)?;

    let user = find_user_by_email(&state, &payload.email).await?;
    let password_hash = user
        .password_hash
        .as_deref()
        .ok_or(AppError::Unauthorized)?;
    verify_password(password_hash, &payload.password)?;

    issue_auth_response(&state, user)
}

pub async fn logout() -> AppResult<(HeaderMap, Json<serde_json::Value>)> {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        clear_cookie_header()?
            .parse()
            .map_err(|_| AppError::Internal("internal error".into()))?,
    );

    Ok((headers, Json(serde_json::json!({ "ok": true }))))
}

pub async fn google_oauth_start(State(state): State<AppState>) -> AppResult<Redirect> {
    let client = google_oauth_client(&state)?;
    let (auth_url, _csrf_token) = client
        .authorize_url(CsrfToken::new_random)
        .add_scope(Scope::new("openid".to_owned()))
        .add_scope(Scope::new("email".to_owned()))
        .add_scope(Scope::new("profile".to_owned()))
        .url();
    Ok(Redirect::temporary(auth_url.as_str()))
}

pub async fn google_oauth_callback(
    State(state): State<AppState>,
    Query(query): Query<OAuthCallbackQuery>,
) -> AppResult<Redirect> {
    let client = google_oauth_client(&state)?;
    let http_client = reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| AppError::Internal("internal error".into()))?;
    let token = client
        .exchange_code(AuthorizationCode::new(query.code))
        .request_async(&http_client)
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("google oauth: {err}")))?;
    let access_token = token.access_token().secret();
    let profile = reqwest::Client::new()
        .get("https://openidconnect.googleapis.com/v1/userinfo")
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("google userinfo: {err}")))?
        .json::<GoogleUserInfo>()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("google profile: {err}")))?;

    validate_email(&profile.email)?;
    let user = upsert_external_user(
        &state,
        &profile.email,
        profile.name.as_deref(),
        profile.picture.as_deref(),
    )
    .await?;
    let auth = issue_bearer_token(&state, user)?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("token", &auth.access_token)
        .append_pair("expires_at", &auth.expires_at.to_string())
        .append_pair("user_id", &auth.user.id.to_string())
        .append_pair("email", &auth.user.email)
        .append_pair("name", auth.user.display_name.as_deref().unwrap_or(""))
        .append_pair("image", auth.user.image.as_deref().unwrap_or(""))
        .finish();
    let callback = format!(
        "{}/auth/callback?{}",
        state.config.web_origin.trim_end_matches('/'),
        query,
    );
    Ok(Redirect::temporary(&callback))
}

// ---------------------------------------------------------------------------
// GitHub OAuth — links a GitHub account to an already-authenticated user.
// `state` query param carries a short-lived signed payload {user_id, exp, nonce}.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct GithubOAuthStartQuery {
    /// Operon JWT — required because the user must already be signed in to
    /// link a GitHub account to their Operon profile.
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct GithubLinkState {
    user_id: Uuid,
    exp: i64,
}

#[derive(Deserialize)]
struct GithubUserInfo {
    id: i64,
    login: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    email: Option<String>,
}

type GithubClient = oauth2::Client<
    oauth2::basic::BasicErrorResponse,
    oauth2::basic::BasicTokenResponse,
    oauth2::basic::BasicTokenIntrospectionResponse,
    oauth2::StandardRevocableToken,
    oauth2::basic::BasicRevocationErrorResponse,
    EndpointSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointNotSet,
    EndpointSet,
>;

fn github_oauth_client(state: &AppState) -> AppResult<GithubClient> {
    let client_id = state
        .config
        .github_client_id
        .clone()
        .ok_or_else(|| AppError::ServiceUnavailable("GITHUB_CLIENT_ID not configured".into()))?;
    let client_secret = state
        .config
        .github_client_secret
        .clone()
        .ok_or_else(|| AppError::ServiceUnavailable("GITHUB_CLIENT_SECRET not configured".into()))?;
    let redirect = format!(
        "{}/auth/oauth/github/callback",
        state.config.oauth_redirect_base.trim_end_matches('/')
    );
    let redirect_url = RedirectUrl::new(redirect).map_err(|_| AppError::Internal("internal error".into()))?;
    Ok(BasicClient::new(ClientId::new(client_id))
        .set_client_secret(ClientSecret::new(client_secret))
        .set_auth_uri(
            AuthUrl::new("https://github.com/login/oauth/authorize".to_owned())
                .map_err(|_| AppError::Internal("internal error".into()))?,
        )
        .set_token_uri(
            TokenUrl::new("https://github.com/login/oauth/access_token".to_owned())
                .map_err(|_| AppError::Internal("internal error".into()))?,
        )
        .set_redirect_uri(redirect_url))
}

fn encode_link_state(secret: &str, user_id: Uuid) -> AppResult<String> {
    let payload = GithubLinkState {
        user_id,
        exp: (Utc::now() + Duration::minutes(10)).timestamp(),
    };
    let json = serde_json::to_vec(&payload).map_err(|_| AppError::Internal("internal error".into()))?;
    let payload_b64 = Base64UrlUnpadded::encode_string(&json);
    let signature = sign(secret, payload_b64.as_bytes())?;
    Ok(format!("{payload_b64}.{signature}"))
}

fn decode_link_state(secret: &str, raw: &str) -> AppResult<Uuid> {
    let (payload_b64, sig) = raw
        .split_once('.')
        .ok_or_else(|| AppError::BadRequest("invalid state".into()))?;
    let expected = sign(secret, payload_b64.as_bytes())?;
    if expected != sig {
        return Err(AppError::BadRequest("state signature mismatch".into()));
    }
    let json =
        Base64UrlUnpadded::decode_vec(payload_b64).map_err(|_| AppError::BadRequest("bad state".into()))?;
    let payload: GithubLinkState =
        serde_json::from_slice(&json).map_err(|_| AppError::BadRequest("bad state".into()))?;
    if payload.exp < Utc::now().timestamp() {
        return Err(AppError::BadRequest("state expired".into()));
    }
    Ok(payload.user_id)
}

pub async fn github_oauth_start(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<GithubOAuthStartQuery>,
) -> AppResult<Redirect> {
    let token = query
        .token
        .as_deref()
        .or_else(|| token_from_headers(&headers))
        .ok_or(AppError::Unauthorized)?;
    let user_id = decode_claims_public(&state, token)?;
    let signed_state = encode_link_state(&state.config.jwt_secret, user_id)?;
    let client = github_oauth_client(&state)?;
    let (auth_url, _csrf) = client
        .authorize_url(|| CsrfToken::new(signed_state.clone()))
        .add_scope(Scope::new("read:user".to_owned()))
        .add_scope(Scope::new("user:email".to_owned()))
        .add_scope(Scope::new("repo".to_owned()))
        .url();
    Ok(Redirect::temporary(auth_url.as_str()))
}

pub async fn github_oauth_callback(
    State(state): State<AppState>,
    Query(query): Query<OAuthCallbackQuery>,
) -> AppResult<Redirect> {
    let coding_url = format!(
        "{}/dashboard/coding",
        state.config.web_origin.trim_end_matches('/')
    );

    let raw_state = query
        .state
        .as_deref()
        .ok_or_else(|| AppError::BadRequest("missing state".into()))?;
    let user_id = match decode_link_state(&state.config.jwt_secret, raw_state) {
        Ok(uid) => uid,
        Err(_) => return Ok(Redirect::temporary(&format!("{coding_url}?error=invalid_state"))),
    };

    let client = github_oauth_client(&state)?;
    let http_client = reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| AppError::Internal("internal error".into()))?;
    let token = client
        .exchange_code(AuthorizationCode::new(query.code))
        .request_async(&http_client)
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("github oauth: {err}")))?;
    let access_token = token.access_token().secret().clone();

    let profile = reqwest::Client::new()
        .get("https://api.github.com/user")
        .header(header::USER_AGENT, "operonx")
        .header(header::ACCEPT, "application/vnd.github+json")
        .bearer_auth(&access_token)
        .send()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("github user: {err}")))?
        .json::<GithubUserInfo>()
        .await
        .map_err(|err| AppError::ServiceUnavailable(format!("github profile: {err}")))?;

    let scopes: Vec<String> = vec![
        "read:user".into(),
        "user:email".into(),
        "repo".into(),
    ];

    sqlx::query(
        "insert into oauth_accounts (id, user_id, provider, provider_account_id, access_token_ciphertext, scopes)
             values ($1, $2, 'github', $3, $4, $5)
             on conflict (provider, provider_account_id) do update
                 set user_id = excluded.user_id,
                     access_token_ciphertext = excluded.access_token_ciphertext,
                     scopes = excluded.scopes,
                     updated_at = now()",
    )
    .bind(Uuid::now_v7())
    .bind(user_id)
    .bind(profile.id.to_string())
    .bind(&access_token)
    .bind(&scopes)
    .execute(&state.db)
    .await?;

    // Best-effort: stash the user's GitHub display name on the user row if
    // they don't have one yet. (No avatar — that comes from Google.)
    if let Some(ref name) = profile.name {
        let _ = sqlx::query(
            "update users set display_name = coalesce(display_name, $1), updated_at = now() where id = $2",
        )
        .bind(name)
        .bind(user_id)
        .execute(&state.db)
        .await;
    }
    let _ = profile.email; // not stored

    Ok(Redirect::temporary(&format!(
        "{coding_url}?connected=github&login={}",
        url::form_urlencoded::byte_serialize(profile.login.as_bytes()).collect::<String>()
    )))
}

#[derive(Deserialize)]
pub struct InternalExchangeRequest {
    pub email: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

/// Trusted server-to-server endpoint: the Next.js app exchanges an
/// authenticated NextAuth session for an operonx JWT. Auto-provisions a
/// password-less user row on first call. Gated by `OPERON_INTERNAL_SECRET`.
pub async fn internal_exchange(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<InternalExchangeRequest>,
) -> AppResult<Json<AuthResponse>> {
    let configured = state
        .config
        .internal_secret
        .as_deref()
        .ok_or(AppError::Unauthorized)?;
    let provided = headers
        .get("x-operon-internal-secret")
        .and_then(|v| v.to_str().ok())
        .ok_or(AppError::Unauthorized)?;
    if !constant_time_eq(provided.as_bytes(), configured.as_bytes()) {
        return Err(AppError::Unauthorized);
    }

    validate_email(&payload.email)?;

    let display_name = payload
        .display_name
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());

    let row = sqlx::query(
        "insert into users (id, email, display_name) values ($1, lower($2), $3)
             on conflict (email) do update
                 set display_name = coalesce(excluded.display_name, users.display_name),
                     updated_at = now()
             returning id, email, display_name, avatar_url, password_hash",
    )
    .bind(Uuid::now_v7())
    .bind(payload.email.trim())
    .bind(display_name)
    .fetch_one(&state.db)
    .await?;

    let user = user_from_row(row)?;
    Ok(Json(issue_bearer_token(&state, user)?))
}

fn google_oauth_client(state: &AppState) -> AppResult<GoogleClient> {
    let client_id = state
        .config
        .google_client_id
        .clone()
        .ok_or_else(|| AppError::ServiceUnavailable("GOOGLE_CLIENT_ID not configured".into()))?;
    let client_secret = state
        .config
        .google_client_secret
        .clone()
        .ok_or_else(|| AppError::ServiceUnavailable("GOOGLE_CLIENT_SECRET not configured".into()))?;
    let redirect = format!(
        "{}/auth/oauth/google/callback",
        state.config.oauth_redirect_base.trim_end_matches('/')
    );
    let redirect_url = RedirectUrl::new(redirect).map_err(|_| AppError::Internal("internal error".into()))?;
    Ok(BasicClient::new(ClientId::new(client_id))
        .set_client_secret(ClientSecret::new(client_secret))
        .set_auth_uri(AuthUrl::new("https://accounts.google.com/o/oauth2/v2/auth".to_owned()).map_err(|_| AppError::Internal("internal error".into()))?)
        .set_token_uri(TokenUrl::new("https://oauth2.googleapis.com/token".to_owned()).map_err(|_| AppError::Internal("internal error".into()))?)
        .set_redirect_uri(redirect_url))
}

async fn upsert_external_user(
    state: &AppState,
    email: &str,
    display_name: Option<&str>,
    avatar_url: Option<&str>,
) -> AppResult<UserRecord> {
    let row = sqlx::query(
        "insert into users (id, email, display_name, avatar_url) values ($1, lower($2), $3, $4)
             on conflict (email) do update
                 set display_name = coalesce(excluded.display_name, users.display_name),
                     avatar_url = coalesce(excluded.avatar_url, users.avatar_url),
                     updated_at = now()
             returning id, email, display_name, avatar_url, password_hash",
    )
    .bind(Uuid::now_v7())
    .bind(email.trim())
    .bind(display_name.map(str::trim).filter(|value| !value.is_empty()))
    .bind(avatar_url.map(str::trim).filter(|value| !value.is_empty()))
    .fetch_one(&state.db)
    .await?;
    user_from_row(row)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub async fn me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<UserResponse>> {
    let token = token_from_headers(&headers).ok_or(AppError::Unauthorized)?;
    let claims = decode_claims(&state, token)?;

    let row = sqlx::query("select id, email, display_name, avatar_url, password_hash from users where id = $1")
        .bind(claims.sub)
        .fetch_optional(&state.db)
        .await?
        .ok_or(AppError::Unauthorized)?;

    Ok(Json(user_response(user_from_row(row)?)))
}

fn validate_email_password(email: &str, password: &str) -> AppResult<()> {
    validate_email(email)?;
    if password.len() < 12 {
        return Err(AppError::BadRequest(
            "password must be at least 12 characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_email(email: &str) -> AppResult<()> {
    let email = email.trim();
    if email.len() < 3 || !email.contains('@') {
        return Err(AppError::BadRequest("valid email is required".to_owned()));
    }
    Ok(())
}

fn hash_password(password: &str) -> AppResult<String> {
    let salt = SaltString::generate(&mut OsRng);
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)?
        .to_string())
}

fn verify_password(password_hash: &str, password: &str) -> AppResult<()> {
    let parsed_hash = PasswordHash::new(password_hash)?;
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .map_err(|_| AppError::Unauthorized)
}

async fn find_user_by_email(state: &AppState, email: &str) -> AppResult<UserRecord> {
    let row = sqlx::query(
        "select id, email, display_name, avatar_url, password_hash from users where email = lower($1)",
    )
    .bind(email.trim())
    .fetch_optional(&state.db)
    .await?
    .ok_or(AppError::Unauthorized)?;

    user_from_row(row)
}

fn user_from_row(row: sqlx::postgres::PgRow) -> AppResult<UserRecord> {
    Ok(UserRecord {
        id: row.try_get("id")?,
        email: row.try_get("email")?,
        display_name: row.try_get("display_name")?,
        avatar_url: row.try_get("avatar_url")?,
        password_hash: row.try_get("password_hash")?,
    })
}

fn issue_auth_response(
    state: &AppState,
    user: UserRecord,
) -> AppResult<(HeaderMap, Json<AuthResponse>)> {
    let now = Utc::now();
    let expires_at = now + Duration::seconds(state.config.access_token_ttl_seconds);
    let claims = Claims {
        sub: user.id,
        email: user.email.clone(),
        iss: "operon".to_owned(),
        aud: state.config.jwt_audience.clone(),
        iat: now.timestamp() as usize,
        exp: expires_at.timestamp() as usize,
    };
    let token = encode_claims(&state.config.jwt_secret, &claims)?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        access_cookie_header(
            &token,
            state.config.access_token_ttl_seconds,
            state.config.cookie_secure,
        )?
        .parse()
        .map_err(|_| AppError::Internal("internal error".into()))?,
    );

    Ok((
        headers,
        Json(AuthResponse {
            user: user_response(user),
            access_token: token,
            expires_at: expires_at.timestamp(),
        }),
    ))
}

fn issue_bearer_token(state: &AppState, user: UserRecord) -> AppResult<AuthResponse> {
    let now = Utc::now();
    let expires_at = now + Duration::seconds(state.config.access_token_ttl_seconds);
    let claims = Claims {
        sub: user.id,
        email: user.email.clone(),
        iss: "operon".to_owned(),
        aud: state.config.jwt_audience.clone(),
        iat: now.timestamp() as usize,
        exp: expires_at.timestamp() as usize,
    };
    let token = encode_claims(&state.config.jwt_secret, &claims)?;
    Ok(AuthResponse {
        user: user_response(user),
        access_token: token,
        expires_at: expires_at.timestamp(),
    })
}

fn user_response(user: UserRecord) -> UserResponse {
    UserResponse {
        id: user.id,
        email: user.email,
        name: user.display_name.clone(),
        display_name: user.display_name,
        image: user.avatar_url,
    }
}

fn decode_claims(state: &AppState, token: &str) -> AppResult<Claims> {
    let (version, claims_part, signature_part) = parse_token(token)?;
    if version != TOKEN_VERSION {
        return Err(AppError::Unauthorized);
    }

    let signed_payload = format!("{version}.{claims_part}");
    let expected_signature = sign(&state.config.jwt_secret, signed_payload.as_bytes())?;
    if !constant_time_eq(expected_signature.as_bytes(), signature_part.as_bytes()) {
        return Err(AppError::Unauthorized);
    }

    let claims_bytes =
        Base64UrlUnpadded::decode_vec(claims_part).map_err(|_| AppError::Unauthorized)?;
    let claims: Claims =
        serde_json::from_slice(&claims_bytes).map_err(|_| AppError::Unauthorized)?;
    if claims.exp < Utc::now().timestamp() as usize {
        return Err(AppError::Unauthorized);
    }
    if claims.iss != "operon" {
        return Err(AppError::Unauthorized);
    }
    if claims.aud != state.config.jwt_audience {
        return Err(AppError::Unauthorized);
    }

    Ok(claims)
}

fn encode_claims(secret: &str, claims: &Claims) -> AppResult<String> {
    let claims_json = serde_json::to_vec(claims).map_err(|_| AppError::Internal("internal error".into()))?;
    let claims_part = Base64UrlUnpadded::encode_string(&claims_json);
    let signed_payload = format!("{TOKEN_VERSION}.{claims_part}");
    let signature = sign(secret, signed_payload.as_bytes())?;

    Ok(format!("{signed_payload}.{signature}"))
}

fn sign(secret: &str, payload: &[u8]) -> AppResult<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| AppError::Internal("internal error".into()))?;
    mac.update(payload);
    Ok(Base64UrlUnpadded::encode_string(
        &mac.finalize().into_bytes(),
    ))
}

fn parse_token(token: &str) -> AppResult<(&str, &str, &str)> {
    let mut parts = token.split('.');
    let version = parts.next().ok_or(AppError::Unauthorized)?;
    let claims = parts.next().ok_or(AppError::Unauthorized)?;
    let signature = parts.next().ok_or(AppError::Unauthorized)?;

    if parts.next().is_some() {
        return Err(AppError::Unauthorized);
    }

    Ok((version, claims, signature))
}

/// Bearer header → cookie (no query-string fallback for security).
pub fn token_from_request(headers: &HeaderMap) -> Option<&str> {
    token_from_headers(headers)
}

/// Verify token + return the user id (`sub` claim).
pub fn decode_claims_public(
    state: &AppState,
    token: &str,
) -> AppResult<Uuid> {
    Ok(decode_claims(state, token)?.sub)
}

fn token_from_headers(headers: &HeaderMap) -> Option<&str> {
    if let Some(header_value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        if let Some(token) = header_value.strip_prefix("Bearer ") {
            return Some(token.trim());
        }
    }

    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|cookie| {
                let (name, value) = cookie.trim().split_once('=')?;
                (name == ACCESS_COOKIE).then_some(value)
            })
        })
}

fn access_cookie_header(token: &str, max_age_seconds: i64, secure: bool) -> AppResult<String> {
    let secure = if secure { "; Secure" } else { "" };
    Ok(format!(
        "{ACCESS_COOKIE}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age_seconds}{secure}"
    ))
}

fn clear_cookie_header() -> AppResult<String> {
    Ok(format!(
        "{ACCESS_COOKIE}=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0"
    ))
}
