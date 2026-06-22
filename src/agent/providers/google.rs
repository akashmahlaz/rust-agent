//! Google Gemini adapter.
//!
//! Wire formats (from Google's Gemini API reference):
//!
//!   image / PDF (via public URL):
//!     `{"file_data": {"file_uri": "https://...", "mime_type": "image/png"}}`
//!     (Gemini fetches the URL itself when given a public file_uri.)
//!
//!   image / PDF (inline bytes):
//!     `{"inline_data": {"mime_type": "...", "data": "<base64>"}}`
//!
//!   pre-uploaded via Gemini Files API:
//!     `{"file_data": {"file_uri": "https://generativelanguage.googleapis.com/...", "mime_type": "..."}}`
//!     (the returned URI from `media.upload` is itself a fetchable URL.)

use anyhow::Result;
use async_trait::async_trait;
use base64ct::{Base64, Encoding};
use reqwest::Client;
use serde_json::{Value, json};

use super::{FilePart, FileSource, ProviderAdapter, fetch_bytes, is_image};

pub struct GoogleAdapter;

#[async_trait]
impl ProviderAdapter for GoogleAdapter {
    fn provider_id(&self) -> &'static str {
        "google"
    }

    fn supports_pdf(&self) -> bool {
        true
    }

    fn supports_image(&self) -> bool {
        true
    }

    async fn convert_file_part(&self, _client: &Client, part: &FilePart) -> Result<Value> {
        let media_type = part.media_type.as_str();
        match &part.source {
            FileSource::Url(url) => {
                // Gemini fetches public URLs via file_data.file_uri.
                // Works for images AND PDFs as long as the URL is reachable.
                Ok(json!({
                    "file_data": {
                        "file_uri": url,
                        "mime_type": media_type,
                    }
                }))
            }
            FileSource::ProviderFile(uri) => {
                // Pre-uploaded via Gemini Files API; uri is the returned
                // `https://generativelanguage.googleapis.com/...` URL.
                Ok(json!({
                    "file_data": {
                        "file_uri": uri,
                        "mime_type": media_type,
                    }
                }))
            }
            FileSource::Inline(base64) => {
                let _ = is_image(media_type); // shape is the same for both
                Ok(json!({
                    "inline_data": {
                        "mime_type": media_type,
                        "data": base64,
                    }
                }))
            }
        }
    }

    /// Gemini accepts raw bytes via the openai-compat shim only for chat-style
    /// endpoints; native Gemini uses inline_data with base64. We mark this
    /// false so runner.rs routes through a future google.rs native caller.
    fn openai_compatible(&self) -> bool {
        false
    }
}

/// Helper kept for future pre-upload path: base64-encode bytes for inline_data.
#[allow(dead_code)]
pub fn base64_inline(bytes: &[u8]) -> String {
    Base64::encode_string(bytes)
}

/// Helper kept for future pre-upload path: fetch a URL into inline bytes when
/// the model doesn't accept URI references (older Gemini models).
#[allow(dead_code)]
pub async fn fetch_and_base64(client: &Client, url: &str) -> Result<String> {
    let bytes = fetch_bytes(client, url).await?;
    Ok(Base64::encode_string(&bytes))
}