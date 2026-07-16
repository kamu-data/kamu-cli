// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};

use crate::jwe::{self, JWE_KEY_LEN};
use crate::{AesGcmEncryptor, Encryptor, ParseEncryptionKey};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A symmetric-key bundle for reading and writing `SecretSet` secret values.
///
/// It holds the one 32-byte key in the two forms secret handling needs:
/// - as a JWE content-encryption key, for the current `contentEncoding: "jwe"`
///   write/read path, and
/// - as an [`AesGcmEncryptor`], for decrypting the legacy `contentEncoding:
///   "aes256gcm"` form emitted only by the env-var backfill migrations
///   (`hex(nonce ‖ ciphertext)`).
///
/// Build it once (it validates the key length up front) and pass it wherever a
/// secret must be encrypted or decrypted.
pub struct SecretCryptor {
    key: [u8; JWE_KEY_LEN],
    aes: AesGcmEncryptor,
}

impl SecretCryptor {
    /// AES-GCM nonce length used by the legacy `aes256gcm` encoding.
    pub const AES_NONCE_LEN: usize = 12;

    pub fn try_new(encryption_key: &str) -> Result<Self, ParseEncryptionKey> {
        let key_bytes = encryption_key.as_bytes();
        let key: [u8; JWE_KEY_LEN] = key_bytes
            .try_into()
            .map_err(|_| ParseEncryptionKey::InvalidEncryptionKeyLength)?;
        let aes = AesGcmEncryptor::try_new(encryption_key)?;
        Ok(Self { key, aes })
    }

    /// Encrypt `plaintext` into a compact JWE token (the write-path form).
    pub fn encrypt_to_jwe(&self, plaintext: &[u8]) -> Result<String, InternalError> {
        jwe::encrypt_compact(&self.key, plaintext).int_err()
    }

    /// Decrypt a compact JWE token produced by [`Self::encrypt_to_jwe`].
    pub fn decrypt_jwe(&self, token: &str) -> Result<Vec<u8>, InternalError> {
        jwe::decrypt_compact(&self.key, token).int_err()
    }

    /// Decrypt a legacy `aes256gcm` value: lowercase hex of
    /// `nonce ‖ ciphertext`, as emitted by the env-var backfill migrations.
    pub fn decrypt_legacy_aes256gcm_hex(&self, hex_value: &str) -> Result<Vec<u8>, InternalError> {
        let combined = hex::decode(hex_value).int_err()?;
        if combined.len() <= Self::AES_NONCE_LEN {
            return InternalError::bail(
                "aes256gcm value is too short to contain a nonce and ciphertext",
            );
        }
        let (nonce, ciphertext) = combined.split_at(Self::AES_NONCE_LEN);
        self.aes.decrypt_bytes(ciphertext, nonce).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn jwe_round_trip() {
        let cryptor = SecretCryptor::try_new(KEY).unwrap();
        let token = cryptor.encrypt_to_jwe(b"hello").unwrap();
        assert_eq!(cryptor.decrypt_jwe(&token).unwrap(), b"hello");
    }

    #[test]
    fn rejects_wrong_key_length() {
        // `SecretCryptor` deliberately holds key material and is not `Debug`, so
        // match on the error arm directly rather than via `assert_matches!` on the
        // whole `Result`.
        match SecretCryptor::try_new("too-short") {
            Err(ParseEncryptionKey::InvalidEncryptionKeyLength) => {}
            Err(other) => panic!("unexpected error: {other:?}"),
            Ok(_) => panic!("expected a key-length error"),
        }
    }

    #[test]
    fn decrypts_legacy_aes256gcm_hex() {
        // Produce a legacy blob the same way the AES encryptor does, then pack
        // it as hex(nonce ‖ ciphertext) — the exact shape the backfill SQL emits.
        let cryptor = SecretCryptor::try_new(KEY).unwrap();
        let (ciphertext, nonce) = cryptor.aes.encrypt_bytes(b"legacy-secret").unwrap();
        let mut combined = nonce.clone();
        combined.extend_from_slice(&ciphertext);
        let hex_value = hex::encode(&combined);

        assert_eq!(
            cryptor.decrypt_legacy_aes256gcm_hex(&hex_value).unwrap(),
            b"legacy-secret"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
