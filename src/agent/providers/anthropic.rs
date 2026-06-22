//! Anthropic Messages API adapter.
//!
//! Wire formats (from Anthropic's Messages API reference):
//!
//!   image (vision):
//!     `{"type":"image", "source":{"type":"url","url":"https://..."}}`
//!     (Anthropic DOES accept image URLs and fetches them itself.)
//!
//!   PDF / document:
//!     `{"type":"document", "source":{"type":"base64","media_type":"application/pdf","data":"<b64>"}}`
//!     (Anthropic does NOT accept URLs for documents — must inline base64.)
//!
//!   pre-uploaded via Anthropic Files API (beta):
//!     `{"type":"document", "source":{"type":"file","file_id":"file_..."}}`
//!     (requires `anthropic-beta: files-api-2025-04-14` header; not wired here yet.)

use anyhow::Result;
use async_trait::async_trait;
use base64ct::{Base64, Encoding};
use reqwest::Client;
use serde_json::{Value, json};

use super::{FilePart, FileSource, ProviderAdapter, fetch_bytes, is_image, is_pdf};

pub struct AnthropicAdapter;

#[async_trait]
impl ProviderAdapter for AnthropicAdapter {
    fn provider_id(&self) -> &'static str {
        "anthropic"
    }

    fn supports_pdf(&self) -> bool {
        true
    }

    fn supports_image(&self) -> bool {
        true
    }

    async fn convert_file_part(&self, client: &Client, part: &FilePart) -> Result<Value> {
        let media_type = part.media_type.as_str();

        // Images: Anthropic accepts URLs, so we pass them straight through.
        if is_image(media_type) {
            return match &part.source {
                FileSource::Url(url) => Ok(json!({
                    "type": "image",
                    "source": { "type": "url", "url": url },
                })),
                FileSource::ProviderFile(file_id) => Ok(json!({
                    "type": "image",
                    "source": { "type": "file", "file_id": file_id },
                })),
                FileSource::Inline(base64) => Ok(json!({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": base64,
                    },
                })),
            };
        }

        // PDFs / documents: Anthropic requires inline base64 (no URL fetch).
        if is_pdf(media_type) {
            match &part.source {
                FileSource::ProviderFile(file_id) => {
                    // Pre-uploaded via Anthropic Files API.
                    Ok(json!({
                        "type": "document",
                        "source": { "type": "file", "file_id": file_id },
                    }))
                }
                FileSource::Inline(base64) => Ok(json!({
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": base64,
                    },
                })),
                FileSource::Url(url) => {
                    // Fetch and inline.
                    let bytes = fetch_bytes(client, url).await?;
                    let b64 = Base64::encode_string(&bytes);
                    Ok(json!({
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64,
                        },
                    }))
                }
            }
        } else {
            // Other document types (txt, docx, csv, ...) — fetch + inline as
            // a generic document. Anthropic infers parsing from media_type.
            match &part.source {
                FileSource::ProviderFile(file_id) => Ok(json!({
                    "type": "document",
                    "source": { "type": "file", "file_id": file_id },
                })),
                FileSource::Inline(base64) => Ok(json!({
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": base64,
                    },
                })),
                FileSource::Url(url) => {
                    let bytes = fetch_bytes(client, url).await?;
                    let b64 = Base64::encode_string(&bytes);
                    Ok(json!({
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64,
                        },
                    }))
                }
            }
        }
    }
}