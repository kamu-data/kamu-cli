// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use crypto_utils::{AesGcmEncryptor, EncryptionError, Encryptor};
use secrecy::{ExposeSecret, SecretString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DidSecretKey {
    pub id: uuid::Uuid,
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DidSecretKey {
    pub fn try_new(
        secret_key: SecretString,
        encryption_key: &str,
    ) -> Result<Self, EncryptionError> {
        let secret_key_id = uuid::Uuid::new_v4();
        let encryptor = AesGcmEncryptor::try_new(encryption_key)?;
        let encryption_result = encryptor.encrypt_str(secret_key.expose_secret().as_ref())?;

        Ok(Self {
            id: secret_key_id,
            secret_key: encryption_result.0,
            secret_nonce: encryption_result.1,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct DatasetDidSecretKeyRowModel {
    pub id: uuid::Uuid,
    pub dataset_id: odf::DatasetID,
    pub value: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct AccountDidSecretKeyRowModel {
    pub id: uuid::Uuid,
    pub account_id: odf::AccountID,
    pub value: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
impl From<DatasetDidSecretKeyRowModel> for DidSecretKey {
    fn from(value: DatasetDidSecretKeyRowModel) -> Self {
        DidSecretKey {
            id: value.id,
            secret_key: value.value,
            secret_nonce: value.secret_nonce,
        }
    }
}

#[cfg(feature = "sqlx")]
impl From<AccountDidSecretKeyRowModel> for DidSecretKey {
    fn from(value: AccountDidSecretKeyRowModel) -> Self {
        DidSecretKey {
            id: value.id,
            secret_key: value.value,
            secret_nonce: value.secret_nonce,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
