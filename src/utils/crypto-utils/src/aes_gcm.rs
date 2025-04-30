// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aes_gcm::aead::consts::U12;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::aes::Aes256;
use aes_gcm::{Aes256Gcm, AesGcm, Key};
use internal_error::ErrorIntoInternal;
use secrecy::SecretString;

use crate::{EncryptionError, Encryptor, ParseEncryptionKey};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AesGcmEncryptor {
    cipher: AesGcm<Aes256, U12>,
}

impl AesGcmEncryptor {
    pub fn try_new(encryption_key: &str) -> Result<Self, ParseEncryptionKey> {
        let key_bytes = encryption_key.as_bytes();
        match std::panic::catch_unwind(|| Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key_bytes))) {
            Ok(aes_gcm) => Ok(Self { cipher: aes_gcm }),
            Err(_) => Err(ParseEncryptionKey::InvalidEncryptionKeyLength),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Encryptor for AesGcmEncryptor {
    fn encrypt_str(&self, value: &str) -> Result<(Vec<u8>, Vec<u8>), EncryptionError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let cipher = self
            .cipher
            .encrypt(&nonce, value.as_bytes())
            .map_err(|err| EncryptionError::InvalidCipherKeyError {
                source: Box::new(AesGcmError(err)),
            })?;
        Ok((cipher, nonce.to_vec()))
    }

    fn decrypt_str(
        &self,
        value: &str,
        secret_nonce: &[u8],
    ) -> Result<SecretString, EncryptionError> {
        let decrypted_value = self
            .cipher
            .decrypt(GenericArray::from_slice(secret_nonce), value.as_bytes())
            .map_err(|err| EncryptionError::InvalidCipherKeyError {
                source: Box::new(AesGcmError(err)),
            })?;
        Ok(SecretString::from(
            std::str::from_utf8(decrypted_value.as_slice())
                .map_err(|err| EncryptionError::InternalError(err.int_err()))?,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct AesGcmError(aes_gcm::Error);

impl std::fmt::Display for AesGcmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AES-GCM error")
    }
}

impl std::error::Error for AesGcmError {}
