// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aes_gcm::aead::consts::U12;
use aes_gcm::aead::{Aead, Generate, KeyInit};
use aes_gcm::aes::Aes256;
use aes_gcm::{Aes256Gcm, AesGcm, Nonce};
use thiserror::Error;

use crate::{EncryptionError, Encryptor, ParseEncryptionKey};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AesGcmEncryptor {
    cipher: AesGcm<Aes256, U12>,
}

impl AesGcmEncryptor {
    pub fn try_new(encryption_key: &str) -> Result<Self, ParseEncryptionKey> {
        let cipher = Aes256Gcm::new_from_slice(encryption_key.as_bytes())
            .map_err(|_| ParseEncryptionKey::InvalidEncryptionKeyLength)?;
        Ok(Self { cipher })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Encryptor for AesGcmEncryptor {
    fn encrypt_bytes(&self, value: &[u8]) -> Result<(Vec<u8>, Vec<u8>), EncryptionError> {
        let nonce = Nonce::<U12>::generate();
        let cipher = self.cipher.encrypt(&nonce, value).map_err(|err| {
            EncryptionError::InvalidCipherKeyError {
                source: Box::new(AesGcmError(err)),
            }
        })?;
        Ok((cipher, nonce.to_vec()))
    }

    fn decrypt_bytes(&self, value: &[u8], secret_nonce: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        // A nonce of the wrong length cannot have come from `encrypt_bytes()`,
        // so it fails the same way as any other tampered input
        let nonce = Nonce::<U12>::try_from(secret_nonce).map_err(|_| {
            EncryptionError::InvalidCipherKeyError {
                source: Box::new(AesGcmError(aes_gcm::Error)),
            }
        })?;
        let decrypted_value = self.cipher.decrypt(&nonce, value).map_err(|err| {
            EncryptionError::InvalidCipherKeyError {
                source: Box::new(AesGcmError(err)),
            }
        })?;
        Ok(decrypted_value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("AES-GCM error")]
struct AesGcmError(aes_gcm::Error);
