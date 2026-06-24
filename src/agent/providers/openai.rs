//! OpenAI adapter — supports BOTH wire formats since Operon routes the same
//! `gpt-*` model through either API based on capabilities:
//!
//!   * **Responses API** (`/v1/responses`): modern API; uses `input_image` and
//!     `input_file` content parts. OpenAI fetches the URL itself, extracts
//!     PDF text and page images, supports vision natively.
//!     Used for: `gpt-5*`, `codex*`, future reasoning models.
//!
//!   * **Chat Completions API** (`/v1/chat/completions`): classic API; uses
//!     `image_url` and `file` content parts. `gpt-4o-mini`, `gpt-4o`, etc.
//!     OpenAI fetches the URL for images; for PDFs, the API requires either
//!     `file_data` (base64 data URL) OR a pre-uploaded `file_id` via the
//!     OpenAI Files API. The URL pass-through is NOT supported in Chat
//!     Completions, so we fetch + base64 inline when no `file_id` is cached.
//!
//! The runner picks the wire format via `requires_responses_api(model)` and
//! forwards the flag here. The adapter's `convert_file_part` honors it.

use anyhow::Result;
use async_trait::async_trait;
use base64ct::{Base64, Encoding};
use reqwest::Client;
use serde_json::{Value, json};

use super::{FilePart, FileSource, ProviderAdapter, fetch_bytes, is_image};

/// Wire-format selector for OpenAI-compatible endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiApiStyle {
    /// Modern `/v1/responses` API — `input_image` / `input_file`.
    Responses,
    /// Classic `/v1/chat/completions` API — `image_url` / `file`.
    ChatCompletions,
}

pub struct OpenAiAdapter {
    pub api_style: OpenAiApiStyle,
}

impl OpenAiAdapter {
    pub fn responses() -> Self {
        Self {
            api_style: OpenAiApiStyle::Responses,
        }
    }

    pub fn chat_completions() -> Self {
        Self {
            api_style: OpenAiApiStyle::ChatCompletions,
        }
    }
}

#[async_trait]
impl ProviderAdapter for OpenAiAdapter {
    fn provider_id(&self) -> &'static str {
        "openai"
    }

    fn supports_pdf(&self) -> bool {
        true
    }

    fn supports_image(&self) -> bool {
        true
    }

    async fn convert_file_part(&self, client: &Client, part: &FilePart) -> Result<Value> {
        match self.api_style {
            OpenAiApiStyle::Responses => self.convert_responses(part).await,
            OpenAiApiStyle::ChatCompletions => self.convert_chat_completions(client, part).await,
        }
    }
}

impl OpenAiAdapter {
    async fn convert_responses(&self, part: &FilePart) -> Result<Value> {
        let media_type = part.media_type.as_str();
        match &part.source {
            FileSource::Url(url) => {
                if is_image(media_type) {
                    // Responses API accepts image URLs natively.
                    Ok(json!({
                        "type": "input_image",
                        "image_url": url,
                    }))
                } else {
                    // PDF / other files: the Responses API does NOT support
                    // a `file_url` field on `input_file` — it accepts either
                    // inline base64 (`file_data`) or a pre-uploaded `file_id`.
                    // The runner is now wired to always carry a fetchable URL
                    // (S3 or local uploads), so we download the bytes here
                    // and inline as base64. This is what fixes gpt-5.1 / gpt-5
                    // with PDFs: the model actually receives the file content.
                    let client = reqwest::Client::new();
                    let bytes = fetch_bytes(&client, url).await?;
                    let b64 = Base64::encode_string(&bytes);
                    Ok(json!({
                        "type": "input_file",
                        "filename": part.filename,
                        "file_data": super::data_url(media_type, &b64),
                    }))
                }
            }
            FileSource::ProviderFile(file_id) => {
                if is_image(media_type) {
                    Ok(json!({ "type": "input_image", "file_id": file_id }))
                } else {
                    Ok(json!({ "type": "input_file", "file_id": file_id }))
                }
            }
            FileSource::Inline(base64) => {
                let data = super::data_url(media_type, base64);
                if is_image(media_type) {
                    Ok(json!({ "type": "input_image", "image_url": data }))
                } else {
                    Ok(json!({
                        "type": "input_file",
                        "filename": part.filename,
                        "file_data": data,
                    }))
                }
            }
            FileSource::DataUrl(data) => {
                if is_image(media_type) {
                    Ok(json!({ "type": "input_image", "image_url": data }))
                } else {
                    Ok(json!({
                        "type": "input_file",
                        "filename": part.filename,
                        "file_data": data,
                    }))
                }
            }
        }
    }

    async fn convert_chat_completions(
        &self,
        client: &Client,
        part: &FilePart,
    ) -> Result<Value> {
        let media_type = part.media_type.as_str();
        match &part.source {
            FileSource::Url(url) => {
                if is_image(media_type) {
                    // Chat Completions accepts image URLs directly.
                    Ok(json!({
                        "type": "image_url",
                        "image_url": { "url": url },
                    }))
                } else {
                    // Chat Completions does NOT support file_url pass-through.
                    // Must either pre-upload (file_id) or inline base64.
                    // Fetch + inline keeps the MVP simple — pre-upload can be
                    // layered on later as an optimization.
                    let bytes = fetch_bytes(client, url).await?;
                    let b64 = Base64::encode_string(&bytes);
                    Ok(json!({
                        "type": "file",
                        "file": {
                            "filename": part.filename,
                            "file_data": super::data_url(media_type, &b64),
                        },
                    }))
                }
            }
            FileSource::ProviderFile(file_id) => {
                // Pre-uploaded via OpenAI Files API.
                if is_image(media_type) {
                    Ok(json!({
                        "type": "image_url",
                        "image_url": { "url": format!("file://{file_id}") },
                    }))
                } else {
                    Ok(json!({
                        "type": "file",
                        "file": { "file_id": file_id },
                    }))
                }
            }
            FileSource::Inline(base64) => {
                if is_image(media_type) {
                    Ok(json!({
                        "type": "image_url",
                        "image_url": { "url": super::data_url(media_type, base64) },
                    }))
                } else {
                    Ok(json!({
                        "type": "file",
                        "file": {
                            "filename": part.filename,
                            "file_data": super::data_url(media_type, base64),
                        },
                    }))
                }
            }
            FileSource::DataUrl(data) => {
                if is_image(media_type) {
                    Ok(json!({
                        "type": "image_url",
                        "image_url": { "url": data },
                    }))
                } else {
                    Ok(json!({
                        "type": "file",
                        "file": {
                            "filename": part.filename,
                            "file_data": data,
                        },
                    }))
                }
            }
        }
    }
}