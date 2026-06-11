//! Token decryption utilities.
//!
//! Matches the legacy Next.js encryption format from `lib/services/auth-profiles.ts`:
//! `v1:iv_base64url:tag_base64url:ciphertext_base64url` (AES-256-GCM).
//!
//! The encryption key is derived with SHA-256 from `OPERON_JWT_SECRET`
//! (or `AUTH_SECRET` fallback), matching the old Node.js implementation.

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Context, Result};
use base64ct::{Base64Url, Encoding};
use sha2::{Digest, Sha256};

const NONCE_SIZE: usize = 12; // 96 bits for GCM
const TAG_SIZE: usize = 16; // 128-bit authentication tag

fn derive_key(secret: &str) -> [u8; 32] {
    let digest = Sha256::digest(secret.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}

/// Decrypt a token stored by the former Next.js auth-profile service.
///
/// Format: `v1:iv_base64url:tag_base64url:ciphertext_base64url`.
/// Non-v1 tokens are treated as legacy plaintext values.
pub fn decrypt_token(encrypted: &str) -> Result<String> {
    let parts: Vec<&str> = encrypted.split(':').collect();
    if parts.len() != 4 || parts[0] != "v1" {
        return Ok(encrypted.to_owned());
    }

    let iv = Base64Url::decode_vec(parts[1]).context("invalid base64url iv")?;
    let tag = Base64Url::decode_vec(parts[2]).context("invalid base64url auth tag")?;
    let ciphertext = Base64Url::decode_vec(parts[3]).context("invalid base64url ciphertext")?;

    if iv.len() != NONCE_SIZE {
        anyhow::bail!(
            "invalid iv length: expected {} bytes, got {}",
            NONCE_SIZE,
            iv.len()
        );
    }
    if tag.len() != TAG_SIZE {
        anyhow::bail!(
            "invalid auth tag length: expected {} bytes, got {}",
            TAG_SIZE,
            tag.len()
        );
    }

    let key = derive_key(&jwt_secret());
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|_| anyhow!("failed to initialize AES-256-GCM cipher"))?;
    let nonce = Nonce::from_slice(&iv);

    // The Rust `aead` API expects ciphertext || tag as the payload.
    let mut payload = ciphertext;
    payload.extend_from_slice(&tag);

    let plaintext = cipher
        .decrypt(nonce, payload.as_ref())
        .map_err(|_| anyhow!("AES-GCM decryption failed — token corrupted or wrong key"))?;

    String::from_utf8(plaintext).context("decrypted token is not valid UTF-8")
}

fn jwt_secret() -> String {
    std::env::var("OPERON_JWT_SECRET")
        .or_else(|_| std::env::var("AUTH_SECRET"))
        .unwrap_or_else(|_| "operon-development-secret".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_v1_token_is_plaintext() {
        let legacy = "plaintext-token-abc123";
        assert_eq!(legacy, decrypt_token(legacy).unwrap());
    }
}
