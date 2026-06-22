//! Per-(provider, model) capability table.
//!
//! Provider-level flags alone are not enough — wire formats vary even within
//! one provider. OpenAI Chat Completions, for example, accepts `file` blocks
//! on `gpt-4o`/`o4-mini` but rejects them on `gpt-4.1` ("invalid argument
//! type"). OpenRouter routes every model through the OpenAI Chat Completions
//! API but most hosted models (Poolside/Laguna, Llama, Mistral, …) do not
//! implement vision or file inputs at all.
//!
//! [`lookup`] returns the conservative capability tuple so the runner can
//! decide between native content blocks and the text-extraction fallback.
//! The default is text fallback — that path already extracts PDF/DOCX/XLSX
//! text server-side, so the model sees the content inline even when no
//! native block is sent.

/// Per-model wire-format capabilities. Both default to `false` (text fallback).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModelCaps {
    /// Model accepts native image content blocks (`image_url`, `input_image`,
    /// Anthropic `image`, Gemini `inlineData`).
    pub native_image: bool,
    /// Model accepts native file/PDF content blocks (`file` with `file_data`,
    /// Responses `input_file`, Anthropic `document`, Gemini `file_data`).
    pub native_pdf: bool,
}

impl Default for ModelCaps {
    fn default() -> Self {
        Self {
            native_image: false,
            native_pdf: false,
        }
    }
}

/// Conservative default — text extraction fallback. Always safe; never breaks
/// a request because of an unsupported content type.
pub const TEXT_FALLBACK: ModelCaps = ModelCaps {
    native_image: false,
    native_pdf: false,
};

/// Resolve the capability tuple for a (provider, model) pair.
///
/// Pass `provider` as the canonical id (`"openai"`, `"anthropic"`, `"google"`,
/// `"openrouter"`, …) and `model` as the user-selected model string.
pub fn lookup(provider: &str, model: &str) -> ModelCaps {
    let p = provider.trim().to_ascii_lowercase();
    let m = model.trim().to_ascii_lowercase();

    // ---- OpenAI Responses API (gpt-5*, codex*) -------------------------
    // Uses `input_image` + `input_file` content parts; OpenAI fetches URLs
    // itself. Both modalities work natively.
    if p == "openai" && (m.starts_with("gpt-5") || m.contains("codex") || m.contains("5.3")) {
        return ModelCaps {
            native_image: true,
            native_pdf: true,
        };
    }

    // ---- OpenAI Chat Completions (everything else under "openai") ------
    if p == "openai" {
        // gpt-4o family — image_url + file (file_data) both supported.
        if m.starts_with("gpt-4o") {
            return ModelCaps {
                native_image: true,
                native_pdf: true,
            };
        }
        // o1 / o3 / o4 family — same wire format as gpt-4o.
        if m.starts_with("o1") || m.starts_with("o3") || m.starts_with("o4") {
            return ModelCaps {
                native_image: true,
                native_pdf: true,
            };
        }
        // gpt-4.1 family — vision yes, but Chat Completions `file` block
        // is rejected with "invalid argument type" as of late 2025. PDF
        // has to go through text extraction.
        if m.starts_with("gpt-4.1") {
            return ModelCaps {
                native_image: true,
                native_pdf: false,
            };
        }
        // gpt-3.5 / unknown GPT — no vision, no file blocks.
        return TEXT_FALLBACK;
    }

    // ---- OpenAI-compatible proxies (OpenRouter, Groq, Together, …) ----
    // Even though provider="openai" with a different base_url, the host
    // model is rarely vision-capable. Default to text fallback so requests
    // always succeed; vision models can be opted in explicitly by the user.
    if p == "openrouter" || p == "groq" || p == "together" || p == "fireworks" || p == "anyscale"
    {
        return TEXT_FALLBACK;
    }

    // ---- Anthropic ----------------------------------------------------
    // image (url OR base64) + document (base64 only) both supported natively.
    if p == "anthropic" {
        return ModelCaps {
            native_image: true,
            native_pdf: true,
        };
    }

    // ---- Google Gemini ------------------------------------------------
    // inlineData for images, file_data for PDFs. Both supported natively.
    if p == "google" || p == "gemini" || p == "vertex" {
        return ModelCaps {
            native_image: true,
            native_pdf: true,
        };
    }

    // ---- Default ------------------------------------------------------
    // Unknown provider → conservative text fallback.
    TEXT_FALLBACK
}

/// Whether a model supports OpenAI's `reasoning_effort` parameter (or
/// Anthropic's `thinking` budget). Sending this to a non-reasoning model
/// yields a 400 "Unrecognized request argument supplied: reasoning_effort"
/// from OpenAI, or is silently dropped by Anthropic.
///
/// Supported: `o1*`, `o3*`, `o4*`, `gpt-5*`, `codex*`.
/// Not supported: `gpt-4o*`, `gpt-4.1*`, `gpt-3.5*`, and most hosted models
/// on OpenAI-compatible proxies.
pub fn supports_reasoning_effort(model: &str) -> bool {
    let m = model.trim().to_ascii_lowercase();
    m.starts_with("o1")
        || m.starts_with("o3")
        || m.starts_with("o4")
        || m.starts_with("gpt-5")
        || m.contains("codex")
        || m.contains("5.3")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpt_4o_supports_both() {
        let caps = lookup("openai", "gpt-4o");
        assert!(caps.native_image);
        assert!(caps.native_pdf);
    }

    #[test]
    fn gpt_4_1_vision_yes_pdf_no() {
        let caps = lookup("openai", "gpt-4.1");
        assert!(caps.native_image);
        assert!(!caps.native_pdf);
    }

    #[test]
    fn gpt_5_supports_both() {
        let caps = lookup("openai", "gpt-5");
        assert!(caps.native_image);
        assert!(caps.native_pdf);
    }

    #[test]
    fn openrouter_text_fallback() {
        let caps = lookup("openrouter", "meta-llama/llama-3.3-70b-instruct");
        assert!(!caps.native_image);
        assert!(!caps.native_pdf);
    }

    #[test]
    fn poolside_laguna_text_fallback() {
        let caps = lookup("openrouter", "poolside/laguna-1");
        assert_eq!(caps, TEXT_FALLBACK);
    }

    #[test]
    fn anthropic_supports_both() {
        let caps = lookup("anthropic", "claude-sonnet-4-6");
        assert!(caps.native_image);
        assert!(caps.native_pdf);
    }

    #[test]
    fn gemini_supports_both() {
        let caps = lookup("google", "gemini-2.5-pro");
        assert!(caps.native_image);
        assert!(caps.native_pdf);
    }

    #[test]
    fn unknown_provider_text_fallback() {
        let caps = lookup("mystery-provider", "mystery-model");
        assert_eq!(caps, TEXT_FALLBACK);
    }

    #[test]
    fn reasoning_effort_supported_on_o_series() {
        assert!(supports_reasoning_effort("o1"));
        assert!(supports_reasoning_effort("o1-mini"));
        assert!(supports_reasoning_effort("o3-mini"));
        assert!(supports_reasoning_effort("o4-mini"));
    }

    #[test]
    fn reasoning_effort_supported_on_gpt5() {
        assert!(supports_reasoning_effort("gpt-5"));
        assert!(supports_reasoning_effort("gpt-5-mini"));
        assert!(supports_reasoning_effort("codex-mini"));
    }

    #[test]
    fn reasoning_effort_not_supported_on_gpt4() {
        // The exact failure mode the user hit.
        assert!(!supports_reasoning_effort("gpt-4.1"));
        assert!(!supports_reasoning_effort("gpt-4.1-mini"));
        assert!(!supports_reasoning_effort("gpt-4o"));
        assert!(!supports_reasoning_effort("gpt-4o-mini"));
        assert!(!supports_reasoning_effort("gpt-3.5-turbo"));
    }

    #[test]
    fn reasoning_effort_not_supported_on_openrouter() {
        // OpenAI-compatible proxies rarely support reasoning_effort even if
        // the underlying model is a reasoning model. We send to the
        // upstream provider which already routes correctly.
        assert!(!supports_reasoning_effort("meta-llama/llama-3.3-70b-instruct"));
        assert!(!supports_reasoning_effort("poolside/laguna-1"));
    }
}