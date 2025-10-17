// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crypto_utils::{AesGcmEncryptor, EncryptionError, Encryptor, ParseEncryptionKey};
use internal_error::{ErrorIntoInternal, InternalError};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookSubscriptionSecret {
    value: Vec<u8>,
    secret_nonce: Option<Vec<u8>>,
}

impl Eq for WebhookSubscriptionSecret {}

impl WebhookSubscriptionSecret {
    pub fn try_new(
        encryption_key_maybe: Option<&SecretString>,
        secret_value: &SecretString,
    ) -> Result<Self, EncryptionError> {
        let encryption_key = if let Some(encryption_key) = encryption_key_maybe {
            encryption_key.expose_secret().to_owned()
        } else {
            return Ok(Self {
                value: secret_value.expose_secret().as_bytes().to_vec(),
                secret_nonce: None,
            });
        };

        let encryptor = AesGcmEncryptor::try_new(&encryption_key)?;
        let encryption_res = encryptor.encrypt_bytes(secret_value.expose_secret().as_bytes())?;

        Ok(Self {
            value: encryption_res.0,
            secret_nonce: Some(encryption_res.1),
        })
    }

    pub fn get_exposed_value(
        &self,
        encryption_key_maybe: Option<&SecretString>,
    ) -> Result<Vec<u8>, WebhookSecretDecryptionError> {
        if self.secret_nonce.is_some() && encryption_key_maybe.is_none() {
            return Err(WebhookSecretDecryptionError::ConfigurationMismatch(
                WebhookSecretEncryptionKeyMissingError {},
            ));
        }

        let Some(secret_nonce) = self.secret_nonce.as_ref() else {
            return Ok(self.value.clone());
        };

        let encryptor = AesGcmEncryptor::try_new(encryption_key_maybe.unwrap().expose_secret())?;
        let decrypted_value = encryptor.decrypt_bytes(&self.value, secret_nonce.as_slice())?;

        return Ok(std::str::from_utf8(decrypted_value.as_slice())
            .map_err(|err| WebhookSecretDecryptionError::Internal(err.int_err()))?
            .as_bytes()
            .to_vec());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum WebhookSecretDecryptionError {
    #[error(transparent)]
    Encryption(#[from] EncryptionError),

    #[error(transparent)]
    ConfigurationMismatch(#[from] WebhookSecretEncryptionKeyMissingError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[error("Unable to decrypt encrypted webhook secret, encryption key is missing in WebhooksConfig")]
pub struct WebhookSecretEncryptionKeyMissingError {}

impl From<ParseEncryptionKey> for WebhookSecretDecryptionError {
    fn from(_value: ParseEncryptionKey) -> Self {
        Self::Encryption(EncryptionError::InvalidEncryptionKey)
    }
}
