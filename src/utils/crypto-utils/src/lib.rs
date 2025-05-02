// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod aes_gcm;
mod argon2_hash;
mod entities;

pub use aes_gcm::*;
pub use argon2_hash::*;
pub use entities::*;
use internal_error::{BoxedError, InternalError};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY: &str = "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait Encryptor {
    fn encrypt_bytes(&self, value: &[u8]) -> Result<(Vec<u8>, Vec<u8>), EncryptionError>;
    fn decrypt_bytes(&self, value: &[u8], secret_nonce: &[u8]) -> Result<Vec<u8>, EncryptionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum EncryptionError {
    #[error("{source}")]
    InvalidCipherKeyError { source: BoxedError },
    #[error("Invalid encryption key")]
    InvalidEncryptionKey,
    #[error(transparent)]
    InternalError(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum ParseEncryptionKey {
    #[error("Invalid encryption key length")]
    InvalidEncryptionKeyLength,
    #[error(transparent)]
    InternalError(#[from] InternalError),
}

impl From<ParseEncryptionKey> for EncryptionError {
    fn from(value: ParseEncryptionKey) -> Self {
        match value {
            ParseEncryptionKey::InvalidEncryptionKeyLength => Self::InvalidEncryptionKey,
            ParseEncryptionKey::InternalError(err) => Self::InternalError(err),
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
