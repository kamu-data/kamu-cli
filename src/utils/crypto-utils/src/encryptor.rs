// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{BoxedError, InternalError};
use thiserror::Error;

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
