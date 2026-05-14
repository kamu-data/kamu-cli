// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use kamu_resources::{ResourceLinterSpec, ResourceValidateSpec, ResourceWarning};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretSetSpec {
    pub secrets: BTreeMap<String, SecretSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, untagged)]
pub enum SecretSpec {
    Literal(String),
    Value(SecretValueSpec),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretValueSpec {
    pub value: String,
}

impl SecretSpec {
    pub fn literal_value(&self) -> &str {
        match self {
            Self::Literal(value) => value,
            Self::Value(value) => &value.value,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetSpec {
    pub const MAX_SECRETS: usize = 256;
    pub const MAX_SECRET_VALUE_LEN: usize = 16 * 1024;
    pub const WARNING_SECRET_VALUE_LEN: usize = 1024;
    pub const RESERVED_SECRET_PREFIX: &str = "KAMU_";

    pub const WARNING_CODE_RESERVED_SECRET_PREFIX: &str = "reserved_secret_prefix";
    pub const WARNING_CODE_LONG_SECRET_VALUE: &str = "long_secret_value";
    pub const WARNING_CODE_LOWERCASE_SECRET_NAME: &str = "lowercase_secret_name";

    fn is_valid_secret_name(name: &str) -> bool {
        let mut chars = name.chars();

        match chars.next() {
            Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
            _ => return false,
        }

        chars.all(|c| c == '_' || c.is_ascii_alphabetic() || c.is_ascii_digit())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceValidateSpec for SecretSetSpec {
    type ValidationError = SecretSetSpecValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.secrets.is_empty() {
            return Err(SecretSetSpecValidationError::EmptySecrets);
        }

        if self.secrets.len() > Self::MAX_SECRETS {
            return Err(SecretSetSpecValidationError::TooManySecrets {
                actual: self.secrets.len(),
                max: Self::MAX_SECRETS,
            });
        }

        for (name, secret) in &self.secrets {
            let value = secret.literal_value();

            if !Self::is_valid_secret_name(name) {
                return Err(SecretSetSpecValidationError::InvalidSecretName { name: name.clone() });
            }

            if value.is_empty() {
                return Err(SecretSetSpecValidationError::EmptySecretValue { name: name.clone() });
            }

            if value.len() > Self::MAX_SECRET_VALUE_LEN {
                return Err(SecretSetSpecValidationError::SecretValueTooLong {
                    name: name.clone(),
                    actual: value.len(),
                    max: Self::MAX_SECRET_VALUE_LEN,
                });
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceLinterSpec for SecretSetSpec {
    fn lint_warnings(&self) -> Vec<ResourceWarning> {
        let mut warnings = Vec::new();

        for (name, secret) in &self.secrets {
            let value = secret.literal_value();

            if name.starts_with(Self::RESERVED_SECRET_PREFIX) {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_RESERVED_SECRET_PREFIX,
                    path: Some(format!("spec.secrets.{name}")),
                    message: format!(
                        "Secret '{name}' uses reserved '{prefix}' prefix",
                        prefix = Self::RESERVED_SECRET_PREFIX
                    ),
                });
            }

            if name.chars().any(|c| c.is_ascii_lowercase()) {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_LOWERCASE_SECRET_NAME,
                    path: Some(format!("spec.secrets.{name}")),
                    message: format!(
                        "Secret '{name}' uses lowercase letters; prefer uppercase names like '{}'",
                        name.to_uppercase()
                    ),
                });
            }

            if value.len() > Self::WARNING_SECRET_VALUE_LEN {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_LONG_SECRET_VALUE,
                    path: Some(match secret {
                        SecretSpec::Literal(_) => format!("spec.secrets.{name}"),
                        SecretSpec::Value(_) => format!("spec.secrets.{name}.value"),
                    }),
                    message: format!(
                        "Secret '{name}' value is unusually long: got {actual}, warning threshold \
                         is {threshold}",
                        actual = value.len(),
                        threshold = Self::WARNING_SECRET_VALUE_LEN
                    ),
                });
            }
        }

        warnings
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SecretSetSpecValidationError {
    #[error("secret set must contain at least one secret")]
    EmptySecrets,

    #[error("too many secrets: got {actual}, max is {max}")]
    TooManySecrets { actual: usize, max: usize },

    #[error("invalid secret name '{name}': expected regex ^[A-Za-z_][A-Za-z0-9_]*$")]
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

#[cfg(test)]
mod tests {
    use super::{SecretSetSpec, SecretSpec, SecretValueSpec};

    #[test]
    fn deserializes_scalar_secret_syntax() {
        let spec: SecretSetSpec = serde_json::from_value(serde_json::json!({
            "secrets": {
                "API_TOKEN": "secret-value",
            }
        }))
        .unwrap();

        assert_eq!(
            spec,
            SecretSetSpec {
                secrets: [(
                    "API_TOKEN".to_string(),
                    SecretSpec::Literal("secret-value".to_string()),
                )]
                .into_iter()
                .collect(),
            }
        );
    }

    #[test]
    fn deserializes_structured_secret_syntax() {
        let spec: SecretSetSpec = serde_json::from_value(serde_json::json!({
            "secrets": {
                "API_TOKEN": {
                    "value": "secret-value",
                },
            }
        }))
        .unwrap();

        assert_eq!(
            spec,
            SecretSetSpec {
                secrets: [(
                    "API_TOKEN".to_string(),
                    SecretSpec::Value(SecretValueSpec {
                        value: "secret-value".to_string(),
                    }),
                )]
                .into_iter()
                .collect(),
            }
        );
    }

    #[test]
    fn serializes_secret_as_scalar_syntax() {
        let value = serde_json::to_value(SecretSetSpec {
            secrets: [(
                "API_TOKEN".to_string(),
                SecretSpec::Literal("secret-value".to_string()),
            )]
            .into_iter()
            .collect(),
        })
        .unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "secrets": {
                    "API_TOKEN": "secret-value",
                }
            })
        );
    }

    #[test]
    fn serializes_structured_secret_syntax() {
        let value = serde_json::to_value(SecretSetSpec {
            secrets: [(
                "API_TOKEN".to_string(),
                SecretSpec::Value(SecretValueSpec {
                    value: "secret-value".to_string(),
                }),
            )]
            .into_iter()
            .collect(),
        })
        .unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "secrets": {
                    "API_TOKEN": {
                        "value": "secret-value",
                    },
                }
            })
        );
    }

    #[test]
    fn lints_reserved_prefix_warning() {
        use kamu_resources::ResourceLinterSpec;

        let spec = SecretSetSpec {
            secrets: [(
                "KAMU_INTERNAL".to_string(),
                SecretSpec::Literal("value".to_string()),
            )]
            .into_iter()
            .collect(),
        };

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            SecretSetSpec::WARNING_CODE_RESERVED_SECRET_PREFIX
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.secrets.KAMU_INTERNAL".to_string())
        );
    }

    #[test]
    fn lints_lowercase_name_warning() {
        use kamu_resources::ResourceLinterSpec;

        let spec = SecretSetSpec {
            secrets: [(
                "my_secret".to_string(),
                SecretSpec::Literal("value".to_string()),
            )]
            .into_iter()
            .collect(),
        };

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            SecretSetSpec::WARNING_CODE_LOWERCASE_SECRET_NAME
        );
        assert_eq!(warnings[0].path, Some("spec.secrets.my_secret".to_string()));
        assert!(warnings[0].message.contains("MY_SECRET"));
    }

    #[test]
    fn lints_long_value_warning_literal() {
        use kamu_resources::ResourceLinterSpec;

        let long_value = "x".repeat(SecretSetSpec::WARNING_SECRET_VALUE_LEN + 1);
        let spec = SecretSetSpec {
            secrets: [("SECRET_KEY".to_string(), SecretSpec::Literal(long_value))]
                .into_iter()
                .collect(),
        };

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            SecretSetSpec::WARNING_CODE_LONG_SECRET_VALUE
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.secrets.SECRET_KEY".to_string())
        );
    }

    #[test]
    fn lints_long_value_warning_structured() {
        use kamu_resources::ResourceLinterSpec;

        let long_value = "x".repeat(SecretSetSpec::WARNING_SECRET_VALUE_LEN + 1);
        let spec = SecretSetSpec {
            secrets: [(
                "SECRET_KEY".to_string(),
                SecretSpec::Value(SecretValueSpec { value: long_value }),
            )]
            .into_iter()
            .collect(),
        };

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            warnings[0].code,
            SecretSetSpec::WARNING_CODE_LONG_SECRET_VALUE
        );
        assert_eq!(
            warnings[0].path,
            Some("spec.secrets.SECRET_KEY.value".to_string())
        );
    }

    #[test]
    fn lints_multiple_warnings() {
        use kamu_resources::ResourceLinterSpec;

        let long_value = "x".repeat(SecretSetSpec::WARNING_SECRET_VALUE_LEN + 1);
        let spec = SecretSetSpec {
            secrets: [
                (
                    "KAMU_TOKEN".to_string(),
                    SecretSpec::Literal("short".to_string()),
                ),
                ("my_key".to_string(), SecretSpec::Literal(long_value)),
            ]
            .into_iter()
            .collect(),
        };

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 3);
        assert_eq!(
            warnings
                .iter()
                .filter(|w| w.code == SecretSetSpec::WARNING_CODE_RESERVED_SECRET_PREFIX)
                .count(),
            1
        );
        assert_eq!(
            warnings
                .iter()
                .filter(|w| w.code == SecretSetSpec::WARNING_CODE_LOWERCASE_SECRET_NAME)
                .count(),
            1
        );
        assert_eq!(
            warnings
                .iter()
                .filter(|w| w.code == SecretSetSpec::WARNING_CODE_LONG_SECRET_VALUE)
                .count(),
            1
        );
    }

    #[test]
    fn lints_no_warnings_for_valid_secret() {
        use kamu_resources::ResourceLinterSpec;

        let spec = SecretSetSpec {
            secrets: [(
                "API_TOKEN".to_string(),
                SecretSpec::Literal("secret-value".to_string()),
            )]
            .into_iter()
            .collect(),
        };

        let warnings = spec.lint_warnings();
        assert_eq!(warnings.len(), 0);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
