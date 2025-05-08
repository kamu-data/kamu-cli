// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crypto_utils::{AesGcmEncryptor, EncryptionError, Encryptor};
use internal_error::ResultIntoInternal;
use odf::metadata::PrivateKey;

use crate::DidEntityType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DidSecretKey {
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DidSecretKey {
    pub fn try_new(secret_key: &PrivateKey, encryption_key: &str) -> Result<Self, EncryptionError> {
        let encryptor = AesGcmEncryptor::try_new(encryption_key)?;
        let encryption_result = encryptor.encrypt_bytes(secret_key.as_bytes())?;

        Ok(Self {
            secret_key: encryption_result.0,
            secret_nonce: encryption_result.1,
        })
    }

    pub fn get_decrypted_private_key(
        &self,
        encryption_key: &str,
    ) -> Result<PrivateKey, EncryptionError> {
        let encryptor = AesGcmEncryptor::try_new(encryption_key)?;
        let decrypted_bytes = encryptor.decrypt_bytes(&self.secret_key, &self.secret_nonce)?;

        Ok(PrivateKey::from_bytes(
            decrypted_bytes.as_slice().try_into().int_err()?,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct DidSecretKeyRowModel {
    pub entity_type: DidEntityType,
    pub entity_id: String,
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

#[cfg(feature = "sqlx")]
impl From<DidSecretKeyRowModel> for DidSecretKey {
    fn from(row_model: DidSecretKeyRowModel) -> Self {
        Self {
            secret_key: row_model.secret_key,
            secret_nonce: row_model.secret_nonce,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
