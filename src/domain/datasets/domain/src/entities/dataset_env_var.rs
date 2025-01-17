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
use chrono::{DateTime, Utc};
use internal_error::{BoxedError, ErrorIntoInternal, InternalError};
use merge::Merge;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use thiserror::Error;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY: &str = "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetEnvVar {
    pub id: Uuid,
    pub key: String,
    pub value: Vec<u8>,
    pub secret_nonce: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub dataset_id: odf::DatasetID,
}

impl DatasetEnvVar {
    pub fn new(
        dataset_env_var_key: &str,
        creation_date: DateTime<Utc>,
        dataset_env_var_value: &DatasetEnvVarValue,
        dataset_id: &odf::DatasetID,
        encryption_key: &str,
    ) -> Result<Self, DatasetEnvVarEncryptionError> {
        let dataset_env_var_id = Uuid::new_v4();
        let mut secret_nonce: Option<Vec<u8>> = None;
        let final_value: Vec<u8>;

        match dataset_env_var_value {
            DatasetEnvVarValue::Secret(secret_value) => {
                let cipher = Self::try_asm_256_gcm_from_str(encryption_key)?;
                let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                secret_nonce = Some(nonce.to_vec());
                final_value = cipher
                    .encrypt(&nonce, secret_value.expose_secret().as_ref())
                    .map_err(|err| DatasetEnvVarEncryptionError::InvalidCipherKeyError {
                        source: Box::new(AesGcmError(err)),
                    })?;
            }
            DatasetEnvVarValue::Regular(value) => final_value = value.as_bytes().to_vec(),
        }

        Ok(DatasetEnvVar {
            id: dataset_env_var_id,
            value: final_value,
            secret_nonce,
            key: dataset_env_var_key.to_string(),
            created_at: creation_date,
            dataset_id: dataset_id.clone(),
        })
    }

    pub fn get_non_secret_value(&self) -> Option<String> {
        if self.secret_nonce.is_none() {
            return Some(std::str::from_utf8(&self.value).unwrap().to_string());
        }
        None
    }

    pub fn try_asm_256_gcm_from_str(
        encryption_key: &str,
    ) -> Result<AesGcm<Aes256, U12>, ParseEncryptionKey> {
        let key_bytes = encryption_key.as_bytes();
        match std::panic::catch_unwind(|| Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key_bytes))) {
            Ok(aes_gcm) => Ok(aes_gcm),
            Err(_) => Err(ParseEncryptionKey::InvalidEncryptionKeyLength),
        }
    }

    pub fn get_exposed_decrypted_value(
        &self,
        encryption_key: &str,
    ) -> Result<String, DatasetEnvVarEncryptionError> {
        if let Some(secret_nonce) = self.secret_nonce.as_ref() {
            let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(encryption_key.as_bytes()));
            let decrypted_value = cipher
                .decrypt(
                    GenericArray::from_slice(secret_nonce.as_slice()),
                    self.value.as_ref(),
                )
                .map_err(|err| DatasetEnvVarEncryptionError::InvalidCipherKeyError {
                    source: Box::new(AesGcmError(err)),
                })?;
            return Ok(std::str::from_utf8(decrypted_value.as_slice())
                .map_err(|err| DatasetEnvVarEncryptionError::InternalError(err.int_err()))?
                .to_string());
        }
        Ok(std::str::from_utf8(&self.value).unwrap().to_string())
    }

    pub fn generate_new_value(
        &self,
        dataset_env_var_new_value: &DatasetEnvVarValue,
        encryption_key: &str,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), DatasetEnvVarEncryptionError> {
        let new_value_and_nonce = match dataset_env_var_new_value {
            DatasetEnvVarValue::Secret(secret_value) => {
                let cipher = Self::try_asm_256_gcm_from_str(encryption_key)?;
                let nonce = self
                    .secret_nonce
                    .as_ref()
                    .map_or(Aes256Gcm::generate_nonce(&mut OsRng), |nonce_bytes| {
                        *GenericArray::from_slice(nonce_bytes.as_slice())
                    });
                (
                    cipher
                        .encrypt(&nonce, secret_value.expose_secret().as_ref())
                        .map_err(|err| DatasetEnvVarEncryptionError::InvalidCipherKeyError {
                            source: Box::new(AesGcmError(err)),
                        })?,
                    Some(nonce.to_vec()),
                )
            }
            DatasetEnvVarValue::Regular(value) => (value.as_bytes().to_vec(), None),
        };
        Ok(new_value_and_nonce)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct DatasetEnvVarRowModel {
    pub id: Uuid,
    pub key: String,
    pub value: Vec<u8>,
    pub secret_nonce: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub dataset_id: odf::DatasetID,
}

#[cfg(feature = "sqlx")]
impl From<DatasetEnvVarRowModel> for DatasetEnvVar {
    fn from(value: DatasetEnvVarRowModel) -> Self {
        DatasetEnvVar {
            id: value.id,
            key: value.key,
            value: value.value,
            secret_nonce: value.secret_nonce,
            created_at: value.created_at,
            dataset_id: value.dataset_id,
        }
    }
}

pub enum DatasetEnvVarValue {
    Secret(SecretString),
    Regular(String),
}

impl DatasetEnvVarValue {
    pub fn get_exposed_value(&self) -> &str {
        match self {
            Self::Regular(value) => value,
            Self::Secret(secret_value) => secret_value.expose_secret(),
        }
    }

    pub fn into_exposed_value(self) -> String {
        match self {
            Self::Regular(value) => value,
            // TODO: Secrecy crate does not provide a way to extract inner value
            Self::Secret(secret_value) => secret_value.expose_secret().to_owned(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DatasetEnvVarEncryptionError {
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

impl From<ParseEncryptionKey> for DatasetEnvVarEncryptionError {
    fn from(value: ParseEncryptionKey) -> Self {
        match value {
            ParseEncryptionKey::InvalidEncryptionKeyLength => Self::InvalidEncryptionKey,
            ParseEncryptionKey::InternalError(err) => Self::InternalError(err),
        }
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Default, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetEnvVarsConfig {
    pub enabled: Option<bool>,
    /// Represents the encryption key for the dataset env vars. This field is
    /// required if `enabled` is `true` or `None`.
    ///
    /// The encryption key must be a 32-character alphanumeric string, which
    /// includes both uppercase and lowercase Latin letters (A-Z, a-z) and
    /// digits (0-9).
    ///
    /// # Example
    /// let config = DatasetEnvVarsConfig {
    ///     enabled: Some(true),
    ///     encryption_key:
    /// Some(String::from("aBcDeFgHiJkLmNoPqRsTuVwXyZ012345")) };
    /// ```
    pub encryption_key: Option<String>,
}

impl DatasetEnvVarsConfig {
    pub fn sample() -> Self {
        Self {
            enabled: Some(true),
            encryption_key: Some(SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY.to_string()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        if let Some(enabled) = self.enabled
            && enabled
            && self.encryption_key.is_some()
        {
            return true;
        }
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use secrecy::SecretString;

    use crate::{DatasetEnvVar, SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY};

    #[test]
    fn test_secret_env_var_generation() {
        let secret_value = "foo";
        let new_env_var = DatasetEnvVar::new(
            "foo_key",
            Utc::now(),
            &crate::DatasetEnvVarValue::Secret(SecretString::from(secret_value.to_string())),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
            SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
        )
        .unwrap();

        let original_value = new_env_var
            .get_exposed_decrypted_value(SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY)
            .unwrap();
        assert_eq!(secret_value, original_value.as_str());
    }

    #[test]
    fn test_non_secret_env_var_generation() {
        let value = "foo";
        let new_env_var = DatasetEnvVar::new(
            "foo_key",
            Utc::now(),
            &crate::DatasetEnvVarValue::Regular(value.to_string()),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
            SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY,
        )
        .unwrap();

        let original_value = new_env_var
            .get_exposed_decrypted_value(SAMPLE_DATASET_ENV_VAR_ENCRYPTION_KEY)
            .unwrap();
        assert_eq!(value, original_value.as_str());
    }
}
