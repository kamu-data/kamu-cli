// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use crypto_utils::{AesGcmEncryptor, EncryptionError, Encryptor};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use secrecy::{ExposeSecret, SecretString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SAMPLE_SECRETS_ENCRYPTION_KEY: &str = "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetEnvVar {
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
    ) -> Result<Self, EncryptionError> {
        let mut secret_nonce: Option<Vec<u8>> = None;
        let final_value: Vec<u8>;

        match dataset_env_var_value {
            DatasetEnvVarValue::Secret(secret_value) => {
                let encryptor = AesGcmEncryptor::try_new(encryption_key)?;
                let encryption_result =
                    encryptor.encrypt_bytes(secret_value.expose_secret().as_bytes())?;
                secret_nonce = Some(encryption_result.1);
                final_value = encryption_result.0;
            }
            DatasetEnvVarValue::Regular(value) => final_value = value.as_bytes().to_vec(),
        }

        Ok(DatasetEnvVar {
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

    pub fn get_exposed_decrypted_value(
        &self,
        encryption_key: &str,
    ) -> Result<String, EncryptionError> {
        if let Some(secret_nonce) = self.secret_nonce.as_ref() {
            let encryptor = AesGcmEncryptor::try_new(encryption_key)?;
            let decrypted_value = encryptor.decrypt_bytes(&self.value, secret_nonce.as_slice())?;

            return Ok(std::str::from_utf8(decrypted_value.as_slice())
                .map_err(|err| EncryptionError::InternalError(err.int_err()))?
                .to_string());
        }
        Ok(std::str::from_utf8(&self.value).unwrap().to_string())
    }

    pub fn generate_new_value(
        &self,
        dataset_env_var_new_value: &DatasetEnvVarValue,
        encryption_key: &str,
    ) -> Result<(Vec<u8>, Option<Vec<u8>>), EncryptionError> {
        let new_value_and_nonce = match dataset_env_var_new_value {
            DatasetEnvVarValue::Secret(secret_value) => {
                let encryptor = AesGcmEncryptor::try_new(encryption_key)?;
                let encryption_res =
                    encryptor.encrypt_bytes(secret_value.expose_secret().as_bytes())?;
                (encryption_res.0, Some(encryption_res.1))
            }
            DatasetEnvVarValue::Regular(value) => (value.as_bytes().to_vec(), None),
        };
        Ok(new_value_and_nonce)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[derive(setty::Config, setty::Default)]
pub struct SecretsEncryptionConfig {
    #[config(default = false)]
    pub enabled: bool,

    /// Represents the encryption key for secrets. This field is required if
    /// `enabled` is `true` or `None`.
    ///
    /// The encryption key must be a 32-character alphanumeric string, which
    /// includes both uppercase and lowercase Latin letters (A-Z, a-z) and
    /// digits (0-9).
    ///
    /// To generate use:
    /// ```sh
    /// tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32; echo
    /// ```
    pub encryption_key: Option<String>,
}

impl SecretsEncryptionConfig {
    pub fn sample() -> Self {
        Self {
            enabled: true,
            encryption_key: Some(SAMPLE_SECRETS_ENCRYPTION_KEY.to_string()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        if self.enabled && self.encryption_key.is_some() {
            return true;
        }
        false
    }

    /// Build an [`AesGcmEncryptor`] from the configured encryption key.
    ///
    /// Returns a graceful error (rather than panicking) when no key is
    /// configured — the same situation `app.rs` treats as "feature disabled".
    /// Callers that need to encrypt/decrypt secrets (e.g. `SecretSet`
    /// processing) use this so a missing key surfaces as an `InternalError`
    /// instead of an `Option::unwrap()` panic.
    pub fn new_encryptor(&self) -> Result<AesGcmEncryptor, InternalError> {
        let Some(encryption_key) = self.encryption_key.as_ref() else {
            return InternalError::bail(
                "Secrets encryption key is not configured; set `secretsEncryption.encryptionKey`",
            );
        };

        AesGcmEncryptor::try_new(encryption_key).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use secrecy::SecretString;

    use crate::{DatasetEnvVar, SAMPLE_SECRETS_ENCRYPTION_KEY};

    #[test]
    fn test_secret_env_var_generation() {
        let secret_value = "foo";
        let new_env_var = DatasetEnvVar::new(
            "foo_key",
            Utc::now(),
            &crate::DatasetEnvVarValue::Secret(SecretString::from(secret_value.to_string())),
            &odf::DatasetID::new_seeded_ed25519(b"foo"),
            SAMPLE_SECRETS_ENCRYPTION_KEY,
        )
        .unwrap();

        let original_value = new_env_var
            .get_exposed_decrypted_value(SAMPLE_SECRETS_ENCRYPTION_KEY)
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
            SAMPLE_SECRETS_ENCRYPTION_KEY,
        )
        .unwrap();

        let original_value = new_env_var
            .get_exposed_decrypted_value(SAMPLE_SECRETS_ENCRYPTION_KEY)
            .unwrap();
        assert_eq!(value, original_value.as_str());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
