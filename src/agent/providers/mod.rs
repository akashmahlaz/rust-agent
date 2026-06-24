//! Provider-agnostic file attachment abstraction.
//!
//! Operon supports multiple LLM providers (OpenAI, Anthropic, Google, ...).
//! Each provider accepts files in a different wire format:
//!
//! | Provider | Image                    | PDF / Document            |
//! |----------|--------------------------|---------------------------|
//! | OpenAI Responses API | `{"type":"input_image","image_url":...}` | `{"type":"input_file","file_url":...}` |
//! | Anthropic Messages   | `{"type":"image","source":{"type":"url",...}}` | `{"type":"document","source":{"type":"base64",...}}` |
//! | Google Gemini        | `{"file_data":{"file_uri":...,"mime_type":...}}` | same |
//!
//! `ProviderAdapter` hides these differences behind a single trait.
//! `runner.rs` builds a canonical [`FilePart`] for each attachment and
//! delegates translation to whichever adapter the active model uses.
//!
//! ChatGPT-quality file UX comes from letting the model **see** the file
//! natively (vision for images, document parser for PDFs) instead of
//! inlining text via OS-level PDF extractors.

use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;

pub mod anthropic;
pub mod google;
pub mod openai;

pub use anthropic::AnthropicAdapter;
pub use google::GoogleAdapter;

// ---------------------------------------------------------------------------
// Canonical types — the only file shapes that flow through the agent loop.
// ---------------------------------------------------------------------------

/// Provider-agnostic file attachment. Each adapter translates this into the
/// provider's native content block (`input_file`, `document`, `file_data`, ...).
#[derive(Debug, Clone)]
pub struct FilePart {
    pub filename: String,
    pub media_type: String,
    pub source: FileSource,
}

/// Where the file bytes live. The adapter picks the right `FileSource` for
/// each provider — e.g. OpenAI Responses accepts a URL pass-through, but
/// Anthropic documents require inline base64.
#[derive(Debug, Clone)]
#[allow(dead_code)] // ProviderFile / Inline are reserved for the Files API
                   // pre-upload path (OpenAI file_id, Anthropic file_id, ...)
                   // which is on the roadmap but not yet wired in.
pub enum FileSource {
    /// Public URL (S3, local uploads route). Provider may fetch internally
    /// (OpenAI, Anthropic images, Google) or we fetch and inline (Anthropic PDFs).
    Url(String),
    /// Already uploaded to the provider's Files API; carry the native id.
    ProviderFile(String),
    /// Raw bytes base64-encoded. Used when the provider requires inline content
    /// (Anthropic PDFs) or when bytes were fetched by the adapter itself.
    Inline(String),
    /// Fully-formed `data:<mime>;base64,<...>` URL. Provider receives inline
    /// content without us having to know its base64 wire format. Used when
    /// the source URL is a private local-upload path that the provider cannot
    /// fetch (so we read from disk and embed the bytes ourselves).
    DataUrl(String),
}

// ---------------------------------------------------------------------------
// ProviderAdapter — only the file shape varies across providers. Text, tools,
// streaming, and tool results all flow through unchanged.
// ---------------------------------------------------------------------------

#[async_trait]
#[allow(dead_code)] // supports_pdf / supports_image / openai_compatible are
                    // reserved for callers that need a quick provider-level
                    // capability check; per-model routing now goes through
                    // `super::model_caps::lookup`.
pub trait ProviderAdapter: Send + Sync {
    /// Stable provider id: "openai" | "anthropic" | "google" | ...
    fn provider_id(&self) -> &'static str;

    /// Build the provider-native content block for one [`FilePart`].
    ///
    /// May fetch the URL inline if the provider doesn't accept URLs for that
    /// media type (Anthropic PDFs go through base64; OpenAI Responses fetches
    /// the URL itself). Image URLs are accepted by all three providers, so
    /// they pass through unchanged.
    async fn convert_file_part(&self, client: &Client, part: &FilePart) -> Result<Value>;

    /// Whether this provider natively understands PDFs (so we don't need to
    /// fall back to text extraction).
    fn supports_pdf(&self) -> bool;

    /// Whether this provider natively understands images (vision).
    fn supports_image(&self) -> bool;

    /// True if the adapter expects API calls in OpenAI-compatible form (so
    /// `runner.rs` can route through `openai::stream_chat` for non-OpenAI
    /// providers that share the wire format).
    fn openai_compatible(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Registry — picks the right adapter for the active model.
// ---------------------------------------------------------------------------

pub struct ProviderRegistry {
    adapters: std::collections::HashMap<&'static str, Box<dyn ProviderAdapter>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self {
            adapters: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, adapter: Box<dyn ProviderAdapter>) {
        self.adapters.insert(adapter.provider_id(), adapter);
    }

    pub fn get(&self, provider_id: &str) -> Option<&dyn ProviderAdapter> {
        self.adapters.get(provider_id).map(|a| a.as_ref())
    }

    /// Pick the right adapter for a specific (provider, model) pair.
    /// OpenAI needs a style-aware adapter because the same provider routes
    /// through different wire formats depending on the model
    /// (Responses API for `gpt-5*` / `codex*`, Chat Completions for the rest).
    /// Other providers map 1:1 to a single adapter.
    pub fn for_model(&self, provider: &str, model: &str) -> Option<&dyn ProviderAdapter> {
        if provider == "openai" {
            let needs_responses = Self::needs_openai_responses_api(model);
            let key: &'static str = if needs_responses {
                "openai:responses"
            } else {
                "openai:chat"
            };
            self.adapters.get(key).map(|a| a.as_ref())
        } else {
            self.get(provider)
        }
    }

    /// Mirrors `super::openai::requires_responses_api` so the providers
    /// module stays independent (no upward dependency on the streaming
    /// module). Keep these in sync if the routing rules change.
    fn needs_openai_responses_api(model: &str) -> bool {
        let m = model.to_ascii_lowercase();
        m.starts_with("gpt-5") || m.contains("codex") || m.contains("5.3")
    }

    /// Standard registry with all built-in adapters. Cheap to clone via
    /// `Arc<ProviderRegistry>` at the call site if hot.
    pub fn with_defaults() -> Self {
        let mut r = Self::new();
        r.register(Box::new(openai::OpenAiAdapter::chat_completions()));
        r.register(Box::new(openai::OpenAiAdapter::responses()));
        r.register(Box::new(AnthropicAdapter));
        r.register(Box::new(GoogleAdapter));
        r
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

// ---------------------------------------------------------------------------
// Helpers shared by adapters.
// ---------------------------------------------------------------------------

/// True for MIME types the adapters should treat as images.
pub fn is_image(media_type: &str) -> bool {
    media_type.starts_with("image/")
}

/// True for MIME types the adapters should treat as PDFs.
pub fn is_pdf(media_type: &str) -> bool {
    media_type == "application/pdf"
}

/// Fetch a URL and return its bytes. Used by adapters that need to inline
/// (Anthropic PDFs, Google non-URI providers). Best-effort: returns an
/// error so the caller can decide whether to fall back to a placeholder.
pub async fn fetch_bytes(client: &Client, url: &str) -> Result<Vec<u8>> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "fetch_bytes: HTTP {} fetching {}",
            resp.status().as_u16(),
            url
        );
    }
    let bytes = resp.bytes().await?;
    Ok(bytes.to_vec())
}

/// Build the standard base64 inline data URL the LLM providers expect
/// (`data:<media>;base64,<...>`).
pub fn data_url(media_type: &str, base64: &str) -> String {
    format!("data:{};base64,{}", media_type, base64)
}