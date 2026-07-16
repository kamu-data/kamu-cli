// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Minimal JWE (JSON Web Encryption, RFC 7516) support restricted to the one
//! profile this codebase needs: **direct key use** (`alg=dir`) with
//! **AES-256-GCM** content encryption (`enc=A256GCM`), serialized in the
//! **compact** form.
//!
//! The compact serialization is five base64url (no padding) segments joined by
//! dots:
//! ```text
//! BASE64URL(protected_header) '.' BASE64URL(encrypted_key) '.'
//!     BASE64URL(iv) '.' BASE64URL(ciphertext) '.' BASE64URL(tag)
//! ```
//! For `alg=dir` the `encrypted_key` segment is empty — the recipient already
//! holds the symmetric content-encryption key. The protected header (its
//! base64url ASCII form) is used verbatim as the AEAD additional authenticated
//! data, exactly as RFC 7516 §5.1 requires.
//!
//! We implement this directly on top of the `aes-gcm` crate we already depend
//! on rather than pulling in a full JOSE stack: the mature Rust JOSE libraries
//! either require OpenSSL (which this workspace deliberately avoids) or do not
//! implement JWE encryption, and this narrow profile is small and
//! well-specified.

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{AeadCore, AeadInPlace, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Key};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64URL;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The compact JWE protected header for `alg=dir`, `enc=A256GCM`.
///
/// Serialized without whitespace so its base64url form is stable and can be
/// recomputed for validation. It is emitted once and never parsed back
/// field-by-field — on decrypt we only require that the header base64url
/// segment matches (it is bound as AAD, so a mismatch fails authentication
/// anyway).
const PROTECTED_HEADER_JSON: &str = r#"{"alg":"dir","enc":"A256GCM"}"#;

/// AES-256-GCM key length in bytes (matches `enc=A256GCM`).
pub const JWE_KEY_LEN: usize = 32;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Encrypt `plaintext` into a compact JWE token (`alg=dir`, `enc=A256GCM`)
/// using the supplied 32-byte content-encryption key.
pub fn encrypt_compact(key: &[u8; JWE_KEY_LEN], plaintext: &[u8]) -> Result<String, JweError> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));

    // The protected header base64url is the AAD (RFC 7516 §5.1 step 14).
    let encoded_header = BASE64URL.encode(PROTECTED_HEADER_JSON.as_bytes());

    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    let mut buffer = plaintext.to_vec();
    let tag = cipher
        .encrypt_in_place_detached(&nonce, encoded_header.as_bytes(), &mut buffer)
        .map_err(|_| JweError::Encryption)?;

    Ok(format!(
        "{header}..{iv}.{ciphertext}.{tag}",
        header = encoded_header,
        iv = BASE64URL.encode(nonce.as_slice()),
        ciphertext = BASE64URL.encode(&buffer),
        tag = BASE64URL.encode(tag.as_slice()),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The outcome of splitting and structurally validating a compact JWE token's
/// five segments, shared by [`decrypt_compact`] and [`looks_like_compact`] so
/// the segment-count/`dir`/header/base64url/length checks can never drift
/// apart between the two.
enum SegmentCheck<'a> {
    /// Wrong segment count.
    WrongCount,
    /// Right segment count, but the `dir` (empty encrypted-key) or header
    /// checks failed — distinguished from base64url/length problems because
    /// callers report it as `UnsupportedAlgorithm`, not `Malformed`.
    UnsupportedAlgorithm,
    /// Header and `dir` are fine, but a segment isn't valid base64url, an
    /// IV/tag decodes to the wrong length for AES-256-GCM, or the ciphertext
    /// is empty (would decrypt to an empty secret).
    Malformed,
    /// All structural checks passed. Does not mean the token decrypts (no key
    /// is used here, and the ciphertext/tag are never authenticated) — only
    /// that reshaping it as a JWE is possible.
    Ok {
        encoded_header: &'a str,
        iv: Vec<u8>,
        ciphertext: Vec<u8>,
        tag: Vec<u8>,
    },
}

fn check_segments(token: &str) -> SegmentCheck<'_> {
    let segments: Vec<&str> = token.split('.').collect();
    if segments.len() != 5 {
        return SegmentCheck::WrongCount;
    }

    // `dir` means there must be no encrypted key.
    if !segments[1].is_empty() {
        return SegmentCheck::UnsupportedAlgorithm;
    }

    // Invalid base64url is malformed; valid base64url decoding to the wrong
    // header is an unsupported algorithm (a different `alg`/`enc` profile).
    let Ok(header) = BASE64URL.decode(segments[0]) else {
        return SegmentCheck::Malformed;
    };
    if header != PROTECTED_HEADER_JSON.as_bytes() {
        return SegmentCheck::UnsupportedAlgorithm;
    }

    let Ok(iv) = BASE64URL.decode(segments[2]) else {
        return SegmentCheck::Malformed;
    };
    let Ok(ciphertext) = BASE64URL.decode(segments[3]) else {
        return SegmentCheck::Malformed;
    };
    let Ok(tag) = BASE64URL.decode(segments[4]) else {
        return SegmentCheck::Malformed;
    };

    // AES-GCM ciphertext length equals plaintext length; the tag is stored
    // separately. Reject empty ciphertext so empty secrets cannot sneak in
    // disguised as `jwe`.
    if iv.len() != 12 || tag.len() != 16 || ciphertext.is_empty() {
        return SegmentCheck::Malformed;
    }

    SegmentCheck::Ok {
        encoded_header: segments[0],
        iv,
        ciphertext,
        tag,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decrypt a compact JWE token produced by [`encrypt_compact`] using the
/// supplied 32-byte content-encryption key.
///
/// Rejects tokens whose management/encryption algorithms are not the supported
/// `dir` / `A256GCM` profile, tokens that are not exactly five segments, and
/// any token that fails AEAD authentication (wrong key, tampering, or a
/// mismatched header).
pub fn decrypt_compact(key: &[u8; JWE_KEY_LEN], token: &str) -> Result<Vec<u8>, JweError> {
    let (encoded_header, iv, mut ciphertext, tag) = match check_segments(token) {
        SegmentCheck::WrongCount | SegmentCheck::Malformed => return Err(JweError::Malformed),
        SegmentCheck::UnsupportedAlgorithm => return Err(JweError::UnsupportedAlgorithm),
        SegmentCheck::Ok {
            encoded_header,
            iv,
            ciphertext,
            tag,
        } => (encoded_header, iv, ciphertext, tag),
    };

    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));

    cipher
        .decrypt_in_place_detached(
            GenericArray::from_slice(&iv),
            // AAD is the *encoded* header, verbatim, as it appeared in the token.
            encoded_header.as_bytes(),
            &mut ciphertext,
            GenericArray::from_slice(&tag),
        )
        .map_err(|_| JweError::Decryption)?;

    Ok(ciphertext)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Structural check that `token` looks like a compact JWE — see
/// [`check_segments`] for exactly what is checked. Does not require the key,
/// so it is usable in spec validation without needing to authenticate the
/// ciphertext. A `true` result does not guarantee the token decrypts (wrong
/// key or tampering still fail authentication), but it does rule out tokens
/// that are malformed regardless of key — the case `decrypt_compact` would
/// otherwise only catch later, at reconciliation or reveal time.
pub fn looks_like_compact(token: &str) -> bool {
    matches!(check_segments(token), SegmentCheck::Ok { .. })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum JweError {
    #[error("JWE token is malformed")]
    Malformed,

    #[error("unsupported JWE algorithm (only dir/A256GCM is supported)")]
    UnsupportedAlgorithm,

    #[error("JWE encryption failed")]
    Encryption,

    #[error("JWE decryption failed")]
    Decryption,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    const KEY: &[u8; JWE_KEY_LEN] = b"0123456789abcdef0123456789abcdef";

    #[test]
    fn round_trips() {
        let token = encrypt_compact(KEY, b"my-secret-value").unwrap();
        assert!(looks_like_compact(&token), "token: {token}");
        assert_eq!(token.split('.').count(), 5);

        let decrypted = decrypt_compact(KEY, &token).unwrap();
        assert_eq!(decrypted, b"my-secret-value");
    }

    #[test]
    fn nonce_is_random_per_encryption() {
        let a = encrypt_compact(KEY, b"same").unwrap();
        let b = encrypt_compact(KEY, b"same").unwrap();
        assert_ne!(a, b, "each encryption must use a fresh IV");
        assert_eq!(decrypt_compact(KEY, &a).unwrap(), b"same");
        assert_eq!(decrypt_compact(KEY, &b).unwrap(), b"same");
    }

    #[test]
    fn wrong_key_fails() {
        let token = encrypt_compact(KEY, b"my-secret-value").unwrap();
        let other_key: &[u8; JWE_KEY_LEN] = b"ffffffffffffffffffffffffffffffff";
        assert_matches!(
            decrypt_compact(other_key, &token),
            Err(JweError::Decryption)
        );
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let token = encrypt_compact(KEY, b"my-secret-value").unwrap();
        let mut parts: Vec<&str> = token.split('.').collect();
        // Flip the ciphertext segment to a different (still valid base64url) value.
        let tampered_ct = BASE64URL.encode(b"tampered-tampered");
        parts[3] = &tampered_ct;
        let tampered = parts.join(".");
        assert_matches!(decrypt_compact(KEY, &tampered), Err(JweError::Decryption));
    }

    #[test]
    fn wrong_segment_count_is_malformed() {
        assert_matches!(decrypt_compact(KEY, "a.b.c"), Err(JweError::Malformed));
    }

    #[test]
    fn looks_like_compact_rejects_non_base64url_segments() {
        let token = encrypt_compact(KEY, b"x").unwrap();
        let mut parts: Vec<&str> = token.split('.').collect();
        parts[3] = "not!valid!base64url!!!";
        let tampered = parts.join(".");
        assert!(
            !looks_like_compact(&tampered),
            "a non-base64url ciphertext segment must fail the structural check, not just later \
             decryption"
        );
    }

    #[test]
    fn looks_like_compact_rejects_wrong_segment_lengths() {
        let token = encrypt_compact(KEY, b"x").unwrap();
        let mut parts: Vec<&str> = token.split('.').collect();
        // Valid base64url, but decodes to the wrong number of bytes for an IV.
        let short_iv = BASE64URL.encode(b"short");
        parts[2] = &short_iv;
        let tampered = parts.join(".");
        assert!(
            !looks_like_compact(&tampered),
            "an IV segment of the wrong length must fail the structural check"
        );
    }

    #[test]
    fn looks_like_compact_rejects_empty_ciphertext() {
        // A JWE of empty plaintext has an empty ciphertext segment (GCM doesn't
        // expand plaintext into ciphertext) — must not look like a valid
        // encrypted secret, since that would let an empty value bypass the
        // `EmptySecretValue` validation rule under the `jwe` encoding.
        let token = encrypt_compact(KEY, b"").unwrap();
        assert!(
            !looks_like_compact(&token),
            "a JWE token of empty plaintext must fail the structural check"
        );
        assert_matches!(decrypt_compact(KEY, &token), Err(JweError::Malformed));
    }

    #[test]
    fn non_base64url_header_is_malformed_not_unsupported_algorithm() {
        let token = encrypt_compact(KEY, b"x").unwrap();
        let mut parts: Vec<&str> = token.split('.').collect();
        parts[0] = "not!valid!base64url!!!";
        let tampered = parts.join(".");
        assert_matches!(decrypt_compact(KEY, &tampered), Err(JweError::Malformed));
        assert!(!looks_like_compact(&tampered));
    }

    #[test]
    fn valid_base64url_wrong_header_is_unsupported_algorithm() {
        let token = encrypt_compact(KEY, b"x").unwrap();
        let mut parts: Vec<&str> = token.split('.').collect();
        let wrong_header = BASE64URL.encode(r#"{"alg":"dir","enc":"A128GCM"}"#);
        parts[0] = &wrong_header;
        let tampered = parts.join(".");
        assert_matches!(
            decrypt_compact(KEY, &tampered),
            Err(JweError::UnsupportedAlgorithm)
        );
        assert!(!looks_like_compact(&tampered));
    }

    #[test]
    fn non_dir_encrypted_key_rejected() {
        let token = encrypt_compact(KEY, b"x").unwrap();
        let mut parts: Vec<&str> = token.split('.').collect();
        parts[1] = "not-empty";
        let modified = parts.join(".");
        assert_matches!(
            decrypt_compact(KEY, &modified),
            Err(JweError::UnsupportedAlgorithm)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
