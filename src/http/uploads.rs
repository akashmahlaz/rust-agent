use std::path::PathBuf;

use axum::{Json, extract::Multipart, extract::State, http::HeaderMap};
use serde_json::json;
use uuid::Uuid;

use crate::{
    http::error::{AppError, AppResult},
    state::AppState,
};

/// Maximum accepted file size (matches the route-level DefaultBodyLimit minus
/// some multipart overhead).
const MAX_FILE_BYTES: usize = 100 * 1024 * 1024;

/// Local directory used when S3 credentials are not configured. Files are
/// served back via `GET /local-uploads/:filename` (ServeDir in router).
pub const LOCAL_UPLOADS_DIR: &str = "./local_uploads";

/// Get user ID from JWT without requiring auth (for optional auth like file uploads)
fn get_optional_user(state: &AppState, headers: &HeaderMap) -> Option<Uuid> {
    let token = super::token_from_request(headers)?;
    super::decode_claims_public(state, token).ok()
}

/// URL-encode every byte of an S3/local path segment that isn't an unreserved
/// character per RFC 3986. Spaces, commas, parentheses, etc. become `%XX`
/// so URLs are safe to embed in chat markdown and to fetch directly —
/// important for filenames like `ChatGPT Image Apr 26, 2026, 05_52_16 PM.png`.
fn percent_encode_path_segment(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        let is_unreserved = matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~'
        );
        if is_unreserved {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Extract a clean lowercase extension from an original filename.
/// Max 12 chars, alphanumeric only. Returns `None` for extension-less files.
fn safe_extension(filename: &str) -> Option<String> {
    let trimmed = filename.trim_end_matches('.');
    let last_dot = trimmed.rfind('.')?;
    let ext = &trimmed[last_dot + 1..];
    if ext.is_empty() || ext.len() > 12 {
        return None;
    }
    let lowered: String = ext
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    if lowered.is_empty() { None } else { Some(lowered) }
}

/// POST /uploads — multipart file upload.
///
/// Priority:
///   1. If all four AWS env vars (AWS_REGION, AWS_BUCKET_NAME, AWS_ACCESS_KEY,
///      AWS_SECRET_KEY) are set → upload to S3. Key is `uploads/<uuid>.<ext>`
///      (never the raw filename) to avoid URL-unsafe chars.
///   2. Otherwise → write to `./local_uploads/<uuid>.<ext>` on disk and
///      return a URL at `GET /local-uploads/<uuid>.<ext>`. This keeps dev
///      working with zero cloud config.
///
/// Both paths return the same JSON shape so the frontend is unaware of which
/// backend was used.
pub async fn create_upload(
    headers: HeaderMap,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> AppResult<Json<serde_json::Value>> {
    tracing::info!("upload: receiving multipart request");

    let user_id = get_optional_user(&state, &headers);

    // --- extract file field + conversation_id --------------------------------
    let mut file_data: Option<(String, String, Vec<u8>)> = None;
    let mut conversation_id: Option<Uuid> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "upload: failed to read multipart field");
            AppError::BadRequest(format!("Failed to read multipart field: {e}"))
        })?
    {
        let field_name = field.name().unwrap_or("").to_string();
        tracing::debug!(field = %field_name, "upload: processing multipart field");
        if field_name == "conversation_id" {
            if let Ok(text) = field.text().await {
                conversation_id = Uuid::parse_str(&text).ok();
            }
        } else if field_name == "file" {
            let filename = field.file_name().unwrap_or("unnamed").to_string();
            let content_type = field
                .content_type()
                .unwrap_or("application/octet-stream")
                .to_string();
            let bytes = field.bytes().await.map_err(|e| {
                tracing::error!(error = %e, filename = %filename, "upload: failed to read file bytes");
                AppError::BadRequest(format!("Failed to read file bytes: {e}"))
            })?;
            tracing::info!(
                filename = %filename,
                content_type = %content_type,
                bytes = bytes.len(),
                "upload: received file"
            );
            file_data = Some((filename, content_type, bytes.to_vec()));
            break;
        }
    }

    let (original_filename, content_type, bytes) = file_data.ok_or_else(|| {
        tracing::error!("upload: no 'file' field found in multipart body");
        AppError::BadRequest("No 'file' field found in multipart body".into())
    })?;

    // --- size validation -----------------------------------------------------
    let size = bytes.len();
    if size == 0 {
        tracing::warn!(filename = %original_filename, "upload: file is empty");
        return Err(AppError::BadRequest("File is empty".into()));
    }
    if size > MAX_FILE_BYTES {
        tracing::warn!(
            filename = %original_filename,
            size_mb = size as f64 / 1024.0 / 1024.0,
            limit_mb = MAX_FILE_BYTES / 1024 / 1024,
            "upload: file too large"
        );
        return Err(AppError::BadRequest(format!(
            "File exceeds {} MB limit (got {:.1} MB)",
            MAX_FILE_BYTES / 1024 / 1024,
            size as f64 / 1024.0 / 1024.0
        )));
    }

    // --- build upload id + key -----------------------------------------------
    let upload_id = Uuid::now_v7();
    let key = match safe_extension(&original_filename) {
        Some(ext) => format!("{upload_id}.{ext}"),
        None => upload_id.to_string(),
    };

    // --- route to S3 or local disk ------------------------------------------
    let has_s3 = state.config.aws_region.is_some()
        && state.config.aws_bucket_name.is_some()
        && state.config.aws_access_key.is_some()
        && state.config.aws_secret_key.is_some();

    tracing::info!(has_s3 = has_s3, filename = %original_filename, size_bytes = size, "upload: deciding storage path");

    if has_s3 {
        tracing::info!("upload: using S3 storage");
        upload_to_s3(&state, &key, &original_filename, &content_type, bytes, size, user_id, conversation_id).await
    } else {
        tracing::warn!(
            "upload: AWS credentials not configured \
             (AWS_REGION / AWS_BUCKET_NAME / AWS_ACCESS_KEY / AWS_SECRET_KEY). \
             Falling back to local disk storage at {LOCAL_UPLOADS_DIR}. \
             Files will NOT persist across server restarts in production."
        );
        upload_to_local(&state, &key, &original_filename, &content_type, bytes, size, user_id, conversation_id).await
    }
}

// ---------------------------------------------------------------------------
// S3 path
// ---------------------------------------------------------------------------
async fn upload_to_s3(
    state: &AppState,
    key: &str,
    original_filename: &str,
    content_type: &str,
    bytes: Vec<u8>,
    size: usize,
    user_id: Option<Uuid>,
    conversation_id: Option<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let region = state.config.aws_region.as_deref().unwrap();
    let bucket = state.config.aws_bucket_name.as_deref().unwrap();
    let access_key = state.config.aws_access_key.as_deref().unwrap();
    let secret_key = state.config.aws_secret_key.as_deref().unwrap();

    let s3_key = format!("uploads/{key}");

    tracing::info!(
        s3_key = %s3_key,
        bucket = %bucket,
        region = %region,
        size_bytes = size,
        "upload: uploading to S3"
    );

    let creds =
        aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "env");
    let s3_config = aws_sdk_s3::Config::builder()
        .region(aws_sdk_s3::config::Region::new(region.to_string()))
        .credentials_provider(creds)
        .behavior_version_latest()
        .build();
    let client = aws_sdk_s3::Client::from_conf(s3_config);

    tracing::info!(region = %region, bucket = %bucket, s3_key = %s3_key, "upload: starting S3 upload");

    let result = client
        .put_object()
        .bucket(bucket)
        .key(&s3_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
        .content_type(content_type)
        .content_disposition(format!(
            "attachment; filename=\"{}\"",
            original_filename.replace('"', "'")
        ))
        .send()
        .await;

    match result {
        Ok(_) => {
            tracing::info!(s3_key = %s3_key, "upload: S3 put_object succeeded");
        }
        Err(e) => {
            tracing::error!(error = %e, s3_key = %s3_key, bucket = %bucket, region = %region, "upload: S3 put_object FAILED");
            return Err(AppError::Internal(format!("S3 upload failed: {e}")));
        }
    }

    let encoded_key = s3_key
        .split('/')
        .map(percent_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    let public_url = format!("https://{bucket}.s3.{region}.amazonaws.com/{encoded_key}");

    // Store file metadata if user is authenticated
    let file_id = if let (Some(uid), Some(cid)) = (user_id, conversation_id) {
        let id = Uuid::now_v7();
        if sqlx::query(
            r#"insert into conversation_files
               (id, conversation_id, user_id, original_filename, storage_key, storage_type, content_type, size_bytes, url)
               values ($1, $2, $3, $4, $5, 's3', $6, $7, $8)"#
        )
        .bind(id)
        .bind(cid)
        .bind(uid)
        .bind(original_filename)
        .bind(&s3_key)
        .bind(content_type)
        .bind(size as i64)
        .bind(&public_url)
        .execute(&state.db)
        .await
        .is_ok()
        {
            Some(id)
        } else {
            None
        }
    } else {
        None
    };

    tracing::info!(public_url = %public_url, "upload: S3 upload complete");
    Ok(Json(json!({
        "url": public_url,
        "publicUrl": public_url,
        "key": s3_key,
        "filename": original_filename,
        "contentType": content_type,
        "size": size,
        "storage": "s3",
        "fileId": file_id
    })))
}

// ---------------------------------------------------------------------------
// Local disk fallback (dev / no-S3 path)
// ---------------------------------------------------------------------------
async fn upload_to_local(
    state: &AppState,
    key: &str,
    original_filename: &str,
    content_type: &str,
    bytes: Vec<u8>,
    size: usize,
    user_id: Option<Uuid>,
    conversation_id: Option<Uuid>,
) -> AppResult<Json<serde_json::Value>> {
    let dir = PathBuf::from(LOCAL_UPLOADS_DIR);
    tokio::fs::create_dir_all(&dir).await.map_err(|e| {
        tracing::error!(error = %e, dir = %dir.display(), "upload: failed to create local uploads dir");
        AppError::Internal(format!("Failed to create uploads directory: {e}"))
    })?;

    let file_path = dir.join(key);
    tokio::fs::write(&file_path, &bytes).await.map_err(|e| {
        tracing::error!(error = %e, path = %file_path.display(), "upload: failed to write local file");
        AppError::Internal(format!("Failed to write file to disk: {e}"))
    })?;

    // Use OPERON_PUBLIC_URL when set (required in production where the Rust
    // server's bind address is an internal socket, not the public domain).
    // Example: OPERON_PUBLIC_URL=https://api.operon.ai
    // Falls back to bind_addr for local dev (zero-config).
    let public_base = std::env::var("OPERON_PUBLIC_URL")
        .map(|u| u.trim_end_matches('/').to_string())
        .unwrap_or_else(|_| format!("http://{}", state.config.bind_addr));
    let public_url = format!("{public_base}/local-uploads/{key}");

    // Store file metadata if user is authenticated
    let file_id = if let (Some(uid), Some(cid)) = (user_id, conversation_id) {
        let id = Uuid::now_v7();
        if sqlx::query(
            r#"insert into conversation_files
               (id, conversation_id, user_id, original_filename, storage_key, storage_type, content_type, size_bytes, url)
               values ($1, $2, $3, $4, $5, 'local', $6, $7, $8)"#
        )
        .bind(id)
        .bind(cid)
        .bind(uid)
        .bind(original_filename)
        .bind(key)
        .bind(content_type)
        .bind(size as i64)
        .bind(&public_url)
        .execute(&state.db)
        .await
        .is_ok()
        {
            Some(id)
        } else {
            None
        }
    } else {
        None
    };

    tracing::info!(
        path = %file_path.display(),
        public_url = %public_url,
        size_bytes = size,
        "upload: saved to local disk"
    );
    Ok(Json(json!({
        "url": public_url,
        "publicUrl": public_url,
        "key": key,
        "filename": original_filename,
        "contentType": content_type,
        "size": size,
        "storage": "local",
        "fileId": file_id
    })))
}
