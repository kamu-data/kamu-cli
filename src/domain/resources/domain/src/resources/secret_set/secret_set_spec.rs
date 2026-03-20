// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ResourceValidateSpec;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetSpec {
    pub secrets: BTreeMap<String, SecretSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSpec {
    pub value: String, // TODO: encrypted value
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetSpec {
    pub const MAX_SECRETS: usize = 256;
    pub const MAX_SECRET_VALUE_LEN: usize = 16 * 1024;

    fn is_valid_secret_name(name: &str) -> bool {
        let mut chars = name.chars();

        match chars.next() {
            Some(c) if c == '_' || c.is_ascii_uppercase() => {}
            _ => return false,
        }

        chars.all(|c| c == '_' || c.is_ascii_uppercase() || c.is_ascii_digit())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceValidateSpec for SecretSetSpec {
    type ValidationError = SecretSetValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.secrets.is_empty() {
            return Err(SecretSetValidationError::EmptySecrets);
        }

        if self.secrets.len() > Self::MAX_SECRETS {
            return Err(SecretSetValidationError::TooManySecrets {
                actual: self.secrets.len(),
                max: Self::MAX_SECRETS,
            });
        }

        for (name, secret) in &self.secrets {
            if !Self::is_valid_secret_name(name) {
                return Err(SecretSetValidationError::InvalidSecretName { name: name.clone() });
            }

            if secret.value.is_empty() {
                return Err(SecretSetValidationError::EmptySecretValue { name: name.clone() });
            }

            if secret.value.len() > Self::MAX_SECRET_VALUE_LEN {
                return Err(SecretSetValidationError::SecretValueTooLong {
                    name: name.clone(),
                    actual: secret.value.len(),
                    max: Self::MAX_SECRET_VALUE_LEN,
                });
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum SecretSetValidationError {
    #[error("secret set must contain at least one secret")]
    EmptySecrets,

    #[error("too many secrets: got {actual}, max is {max}")]
    TooManySecrets { actual: usize, max: usize },

    #[error("invalid secret name '{name}': expected regex ^[A-Z_][A-Z0-9_]*$")]
    InvalidSecretName { name: String },

    #[error("secret '{name}' has empty value")]
    EmptySecretValue { name: String },

    #[error("secret '{name}' value is too long: got {actual}, max is {max}")]
    SecretValueTooLong {
        name: String,
        actual: usize,
        max: usize,
    },

    #[error("description is too long: got {actual}, max is {max}")]
    DescriptionTooLong { actual: usize, max: usize },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
