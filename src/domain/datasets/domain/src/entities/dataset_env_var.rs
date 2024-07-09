// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng};
use aes_gcm::aes::cipher::typenum::U12;
use aes_gcm::aes::Aes256;
use aes_gcm::{Aes256Gcm, AesGcm, Key};
use chrono::{DateTime, Utc};
use internal_error::{BoxedError, ErrorIntoInternal, InternalError};
use opendatafabric::DatasetID;
use secrecy::{ExposeSecret, Secret};
use thiserror::Error;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DATASET_ENV_VAR_ENCRYPTION_KEY_VAR: &str = "DATASET_ENV_VAR_ENCRYPTION_KEY";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetEnvVar {
    pub id: Uuid,
    pub key: String,
    pub value: Vec<u8>,
    pub secret_nonce: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

impl DatasetEnvVar {
    pub fn new(
        dataset_env_var_key: &str,
        creation_date: DateTime<Utc>,
        dataset_env_var_value: &DatasetEnvVarValue,
        dataset_id: &DatasetID,
    ) -> Result<Self, InvalidCipherKeyError> {
        let dataset_env_var_id = Uuid::new_v4();
        let mut secret_nonce: Option<Vec<u8>> = None;
        let final_value: Vec<u8>;

        match dataset_env_var_value {
            DatasetEnvVarValue::Secret(secret_value) => {
                let cipher =
                    Self::get_cipher().map_err(|err| InvalidCipherKeyError::new(Box::new(err)))?;
                let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                secret_nonce = Some(nonce.to_vec());
                final_value = cipher
                    .encrypt(&nonce, secret_value.expose_secret().as_ref())
                    .map_err(|err| InvalidCipherKeyError::new(Box::new(AesGcmError(err))))?;
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

    fn get_cipher() -> Result<AesGcm<Aes256, U12>, InternalError> {
        match std::env::var(DATASET_ENV_VAR_ENCRYPTION_KEY_VAR) {
            Ok(key_string) => {
                let key_bytes = key_string.as_bytes();
                Ok(Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key_bytes)))
            }
            Err(err) => Err(err.int_err()),
        }
    }

    pub fn get_non_secret_value(&self) -> Option<String> {
        if self.secret_nonce.is_none() {
            return Some(std::str::from_utf8(&self.value).unwrap().to_string());
        }
        None
    }

    pub fn get_exposed_value(&self) -> Result<String, InvalidCipherKeyError> {
        if let Some(secret_nonce) = self.secret_nonce.as_ref() {
            let cipher =
                Self::get_cipher().map_err(|err| InvalidCipherKeyError::new(Box::new(err)))?;

            let decypted_value = cipher
                .decrypt(
                    GenericArray::from_slice(secret_nonce.as_slice()),
                    self.value.as_ref(),
                )
                .map_err(|err| InvalidCipherKeyError::new(Box::new(AesGcmError(err))))?;
            return Ok(std::str::from_utf8(decypted_value.as_slice())
                .map_err(|err| InvalidCipherKeyError::new(Box::new(err)))?
                .to_string());
        }
        Ok(std::str::from_utf8(&self.value).unwrap().to_string())
    }

    pub fn generate_new_value(
        &self,
        dataset_env_var_new_value: &DatasetEnvVarValue,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), InvalidCipherKeyError> {
        let new_value_and_nonce = match dataset_env_var_new_value {
            DatasetEnvVarValue::Secret(secret_value) => {
                let cipher =
                    Self::get_cipher().map_err(|err| InvalidCipherKeyError::new(Box::new(err)))?;
                let nonce = self
                    .secret_nonce
                    .as_ref()
                    .map_or(Aes256Gcm::generate_nonce(&mut OsRng), |nonce_bytes| {
                        *GenericArray::from_slice(nonce_bytes.as_slice())
                    });
                (
                    cipher
                        .encrypt(&nonce, secret_value.expose_secret().as_ref())
                        .map_err(|err| InvalidCipherKeyError::new(Box::new(AesGcmError(err))))?,
                    Some(nonce.to_vec()),
                )
            }
            DatasetEnvVarValue::Regular(value) => (value.as_bytes().to_vec(), None),
        };
        Ok(new_value_and_nonce)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct DatasetEnvVarRowModel {
    pub id: Uuid,
    pub key: String,
    pub value: Vec<u8>,
    pub secret_nonce: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

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
    Secret(Secret<String>),
    Regular(String),
}

#[derive(Debug, Error)]
#[error("Invalid cipher key")]
pub struct InvalidCipherKeyError {
    #[source]
    source: BoxedError,
}

impl InvalidCipherKeyError {
    pub fn new(source: BoxedError) -> Self {
        Self { source }
    }
}

#[derive(Debug)]
struct AesGcmError(aes_gcm::Error);

impl std::fmt::Display for AesGcmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AES-GCM error")
    }
}

impl std::error::Error for AesGcmError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use chrono::Utc;
    use opendatafabric::DatasetID;
    use secrecy::Secret;

    use crate::{DatasetEnvVar, InvalidCipherKeyError, DATASET_ENV_VAR_ENCRYPTION_KEY_VAR};

    #[test]
    fn test_secret_env_var_generation() {
        std::env::set_var(
            DATASET_ENV_VAR_ENCRYPTION_KEY_VAR,
            "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC",
        );

        let secret_value = "foo";
        let new_env_var = DatasetEnvVar::new(
            "foo_key",
            Utc::now(),
            &crate::DatasetEnvVarValue::Secret(Secret::new(secret_value.to_string())),
            &DatasetID::new_seeded_ed25519(b"foo"),
        )
        .unwrap();

        let original_value = new_env_var.get_exposed_value().unwrap();
        assert_eq!(secret_value, original_value.as_str());
    }

    #[test]
    fn test_secret_env_var_generation_with_encyption_key() {
        let secret_value = "foo";
        let new_env_var_result = DatasetEnvVar::new(
            "foo_key",
            Utc::now(),
            &crate::DatasetEnvVarValue::Secret(Secret::new(secret_value.to_string())),
            &DatasetID::new_seeded_ed25519(b"foo"),
        );
        assert_matches!(new_env_var_result, Err(InvalidCipherKeyError { source: _ }));
    }

    #[test]
    fn test_non_secret_env_var_generation() {
        let value = "foo";
        let new_env_var = DatasetEnvVar::new(
            "foo_key",
            Utc::now(),
            &crate::DatasetEnvVarValue::Regular(value.to_string()),
            &DatasetID::new_seeded_ed25519(b"foo"),
        )
        .unwrap();

        let original_value = new_env_var.get_exposed_value().unwrap();
        assert_eq!(value, original_value.as_str());
    }
}
