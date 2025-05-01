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
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY: &str = "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DidSecretKey {
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DidSecretKey {
    pub fn try_new(secret_key: PrivateKey, encryption_key: &str) -> Result<Self, EncryptionError> {
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
pub struct DatasetDidSecretKeyRowModel {
    pub dataset_id: odf::DatasetID,
    pub owner_id: odf::AccountID,
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct AccountDidSecretKeyRowModel {
    pub account_id: odf::AccountID,
    pub owner_id: odf::AccountID,
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
impl From<DatasetDidSecretKeyRowModel> for DidSecretKey {
    fn from(value: DatasetDidSecretKeyRowModel) -> Self {
        DidSecretKey {
            secret_key: value.secret_key,
            secret_nonce: value.secret_nonce,
        }
    }
}

#[cfg(feature = "sqlx")]
impl From<AccountDidSecretKeyRowModel> for DidSecretKey {
    fn from(value: AccountDidSecretKeyRowModel) -> Self {
        DidSecretKey {
            secret_key: value.secret_key,
            secret_nonce: value.secret_nonce,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DidSecretEncryptionConfig {
    /// The encryption key must be a 32-character alphanumeric string, which
    /// includes both uppercase and lowercase Latin letters (A-Z, a-z) and
    /// digits (0-9).
    ///
    /// # Example
    /// let config = DidSecretEncryptionConfig {
    ///     encryption_key: String::from("aBcDeFgHiJkLmNoPqRsTuVwXyZ012345")
    /// };
    /// ```
    pub encryption_key: String,
}

impl DidSecretEncryptionConfig {
    pub fn sample() -> Self {
        Self {
            encryption_key: SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY.to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
