// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crypto_utils::{SecretCryptor, jwe};
use internal_error::InternalError;
use kamu_resources::{ResourceLinterSpec, ResourceValidateSpec, ResourceWarning};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretSetSpec {
    pub secrets: BTreeMap<String, SecretSpec>,
}

kamu_resources::declare_identity_resource_spec_from_input!(SecretSetSpec);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// RFC-18-shaped secret: either a bare string (plaintext shorthand) or an
/// object `{ value, contentEncoding? }`. `contentEncoding` labels how `value`
/// is encoded: absent means plaintext; `"jwe"` is the encrypted form the node
/// produces on apply (an RFC-7516 compact JWE token); `"aes256gcm"` is a
/// legacy read-only form (`hex(nonce ‖ ciphertext)`) emitted only by the
/// env-var backfill migrations.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_encoding: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `contentEncoding` for a compact JWE token — the form written on apply.
pub const CONTENT_ENCODING_JWE: &str = "jwe";

/// `contentEncoding` for the legacy `hex(nonce ‖ ciphertext)` AES-GCM form,
/// produced only by the env-var backfill migrations and read (never written)
/// by the node.
pub const CONTENT_ENCODING_AES256GCM: &str = "aes256gcm";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSpec {
    /// The plaintext string for a non-encrypted secret.
    ///
    /// Panics on an encrypted `Value` — callers must gate on
    /// [`Self::is_encrypted`] first.
    pub fn literal_value(&self) -> &str {
        match self {
            Self::Literal(value) => value,
            Self::Value(value) if value.content_encoding.is_none() => &value.value,
            Self::Value(_) => panic!("literal_value() called on encrypted secret"),
        }
    }

    pub fn is_encrypted(&self) -> bool {
        matches!(
            self,
            Self::Value(SecretValueSpec {
                content_encoding: Some(_),
                ..
            })
        )
    }

    /// The encrypted token, if this is an encrypted `Value` — used by the
    /// sanitizer idempotency check to compare/reuse an existing ciphertext.
    pub fn as_encrypted(&self) -> Option<&str> {
        match self {
            Self::Value(SecretValueSpec {
                value,
                content_encoding: Some(_),
            }) => Some(value.as_str()),
            _ => None,
        }
    }

    /// Decrypt to plaintext bytes, dispatching on `contentEncoding`:
    /// `jwe` → JWE-decrypt; `aes256gcm` → legacy hex form; absent → the value
    /// is already plaintext.
    pub fn decrypt_plaintext_bytes(
        &self,
        cryptor: &SecretCryptor,
    ) -> Result<Vec<u8>, InternalError> {
        match self {
            Self::Literal(value) => Ok(value.as_bytes().to_vec()),
            Self::Value(SecretValueSpec {
                value,
                content_encoding,
            }) => match content_encoding.as_deref() {
                None => Ok(value.as_bytes().to_vec()),
                Some(CONTENT_ENCODING_JWE) => cryptor.decrypt_jwe(value),
                Some(CONTENT_ENCODING_AES256GCM) => cryptor.decrypt_legacy_aes256gcm_hex(value),
                Some(other) => {
                    InternalError::bail(format!("unknown secret contentEncoding '{other}'"))
                }
            },
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

    /// Validate an encrypted `Value` (`content_encoding` is `Some`). Checks the
    /// encoding is one we recognize and that `value` is structurally plausible
    /// for it, without needing the encryption key.
    fn validate_encrypted_secret(
        name: &str,
        value: &str,
        content_encoding: &str,
    ) -> Result<(), SecretSetSpecValidationError> {
        let invalid = |reason: &str| SecretSetSpecValidationError::InvalidEncryptedSecret {
            name: name.to_string(),
            reason: reason.to_string(),
        };

        if value.is_empty() {
            return Err(invalid("encrypted value is empty"));
        }

        match content_encoding {
            CONTENT_ENCODING_JWE => {
                if !jwe::looks_like_compact(value) {
                    return Err(invalid("value is not a compact JWE token"));
                }
            }
            CONTENT_ENCODING_AES256GCM => {
                let bytes =
                    hex::decode(value).map_err(|_| invalid("aes256gcm value is not valid hex"))?;
                if bytes.len() <= SecretCryptor::AES_NONCE_LEN {
                    return Err(invalid(
                        "aes256gcm value is too short to contain a nonce and ciphertext",
                    ));
                }
            }
            other => {
                return Err(SecretSetSpecValidationError::UnknownContentEncoding {
                    name: name.to_string(),
                    content_encoding: other.to_string(),
                });
            }
        }

        Ok(())
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
            if !Self::is_valid_secret_name(name) {
                return Err(SecretSetSpecValidationError::InvalidSecretName { name: name.clone() });
            }

            if let SecretSpec::Value(SecretValueSpec {
                value,
                content_encoding: Some(content_encoding),
            }) = secret
            {
                Self::validate_encrypted_secret(name, value, content_encoding)?;
                continue;
            }

            let value = secret.literal_value();

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
            if name.starts_with(Self::RESERVED_SECRET_PREFIX) {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_RESERVED_SECRET_PREFIX.to_string(),
                    path: Some(format!("spec.secrets.{name}")),
                    message: format!(
                        "Secret '{name}' uses reserved '{prefix}' prefix",
                        prefix = Self::RESERVED_SECRET_PREFIX
                    ),
                });
            }

            if name.chars().any(|c| c.is_ascii_lowercase()) {
                warnings.push(ResourceWarning {
                    code: Self::WARNING_CODE_LOWERCASE_SECRET_NAME.to_string(),
                    path: Some(format!("spec.secrets.{name}")),
                    message: format!(
                        "Secret '{name}' uses lowercase letters; prefer uppercase names like '{}'",
                        name.to_uppercase()
                    ),
                });
            }

            if !secret.is_encrypted() {
                let value = secret.literal_value();

                if value.len() > Self::WARNING_SECRET_VALUE_LEN {
                    warnings.push(ResourceWarning {
                        code: Self::WARNING_CODE_LONG_SECRET_VALUE.to_string(),
                        path: Some(match secret {
                            SecretSpec::Literal(_) => format!("spec.secrets.{name}"),
                            SecretSpec::Value(_) => format!("spec.secrets.{name}.value"),
                        }),
                        message: format!(
                            "Secret '{name}' value is unusually long: got {actual}, warning \
                             threshold is {threshold}",
                            actual = value.len(),
                            threshold = Self::WARNING_SECRET_VALUE_LEN
                        ),
                    });
                }
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

    #[error("secret '{name}' has invalid encrypted value: {reason}")]
    InvalidEncryptedSecret { name: String, reason: String },

    #[error("secret '{name}' has unknown contentEncoding '{content_encoding}'")]
    UnknownContentEncoding {
        name: String,
        content_encoding: String,
    },

    #[error("description is too long: got {actual}, max is {max}")]
    DescriptionTooLong { actual: usize, max: usize },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use crypto_utils::SecretCryptor;

    use super::{
        CONTENT_ENCODING_AES256GCM,
        CONTENT_ENCODING_JWE,
        SecretSetSpec,
        SecretSetSpecValidationError,
        SecretSpec,
        SecretValueSpec,
    };

    // A 32-byte key for building real JWE / aes256gcm test values.
    const TEST_KEY: &str = "0123456789abcdef0123456789abcdef";

    /// A plaintext structured secret (`{ value }`, no encoding).
    fn plaintext_value(value: &str) -> SecretSpec {
        SecretSpec::Value(SecretValueSpec {
            value: value.to_string(),
            content_encoding: None,
        })
    }

    /// A real encrypted JWE secret for `plaintext`.
    fn jwe_secret(plaintext: &str) -> SecretSpec {
        let cryptor = SecretCryptor::try_new(TEST_KEY).unwrap();
        SecretSpec::Value(SecretValueSpec {
            value: cryptor.encrypt_to_jwe(plaintext.as_bytes()).unwrap(),
            content_encoding: Some(CONTENT_ENCODING_JWE.to_string()),
        })
    }

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
                secrets: [("API_TOKEN".to_string(), plaintext_value("secret-value"))]
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
            secrets: [("API_TOKEN".to_string(), plaintext_value("secret-value"))]
                .into_iter()
                .collect(),
        })
        .unwrap();

        // `contentEncoding` is `None`, so it is omitted — the `{ value }` form
        // round-trips exactly as before.
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
            secrets: [("SECRET_KEY".to_string(), plaintext_value(&long_value))]
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

    #[test]
    fn deserializes_encrypted_jwe_secret_syntax() {
        let cryptor = SecretCryptor::try_new(TEST_KEY).unwrap();
        let token = cryptor.encrypt_to_jwe(b"secret-value").unwrap();

        let spec: SecretSetSpec = serde_json::from_value(serde_json::json!({
            "secrets": {
                "API_TOKEN": {
                    "value": token,
                    "contentEncoding": CONTENT_ENCODING_JWE,
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
                        value: token,
                        content_encoding: Some(CONTENT_ENCODING_JWE.to_string()),
                    }),
                )]
                .into_iter()
                .collect(),
            }
        );
    }

    #[test]
    fn serializes_encrypted_jwe_secret_syntax() {
        let cryptor = SecretCryptor::try_new(TEST_KEY).unwrap();
        let token = cryptor.encrypt_to_jwe(b"secret-value").unwrap();

        let value = serde_json::to_value(SecretSetSpec {
            secrets: [(
                "API_TOKEN".to_string(),
                SecretSpec::Value(SecretValueSpec {
                    value: token.clone(),
                    content_encoding: Some(CONTENT_ENCODING_JWE.to_string()),
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
                        "value": token,
                        "contentEncoding": CONTENT_ENCODING_JWE,
                    },
                }
            })
        );
    }

    #[test]
    fn validate_accepts_encrypted_jwe_variant() {
        use kamu_resources::ResourceValidateSpec;

        let spec = SecretSetSpec {
            secrets: [("API_TOKEN".to_string(), jwe_secret("secret-value"))]
                .into_iter()
                .collect(),
        };

        assert!(spec.validate().is_ok());
    }

    #[test]
    fn validate_rejects_malformed_jwe_token() {
        use kamu_resources::ResourceValidateSpec;

        let spec = SecretSetSpec {
            secrets: [(
                "API_TOKEN".to_string(),
                SecretSpec::Value(SecretValueSpec {
                    value: "not-a-jwe-token".to_string(),
                    content_encoding: Some(CONTENT_ENCODING_JWE.to_string()),
                }),
            )]
            .into_iter()
            .collect(),
        };

        assert_matches!(
            spec.validate(),
            Err(SecretSetSpecValidationError::InvalidEncryptedSecret { .. })
        );
    }

    #[test]
    fn validate_rejects_unknown_content_encoding() {
        use kamu_resources::ResourceValidateSpec;

        let spec = SecretSetSpec {
            secrets: [(
                "API_TOKEN".to_string(),
                SecretSpec::Value(SecretValueSpec {
                    value: "whatever".to_string(),
                    content_encoding: Some("rot13".to_string()),
                }),
            )]
            .into_iter()
            .collect(),
        };

        assert_matches!(
            spec.validate(),
            Err(SecretSetSpecValidationError::UnknownContentEncoding { .. })
        );
    }

    #[test]
    fn validate_accepts_legacy_aes256gcm_variant() {
        use kamu_resources::ResourceValidateSpec;

        // Build a legacy aes256gcm value the same way the backfill SQL does:
        // hex(nonce ‖ ciphertext).
        let hex_value = hex::encode(vec![0u8; SecretCryptor::AES_NONCE_LEN + 4]);
        let spec = SecretSetSpec {
            secrets: [(
                "API_TOKEN".to_string(),
                SecretSpec::Value(SecretValueSpec {
                    value: hex_value,
                    content_encoding: Some(CONTENT_ENCODING_AES256GCM.to_string()),
                }),
            )]
            .into_iter()
            .collect(),
        };

        assert!(spec.validate().is_ok());
    }

    #[test]
    fn decrypts_legacy_aes256gcm_encoding() {
        // Produce a real legacy value with the AES encryptor, packed as the
        // backfill SQL would (hex of nonce ‖ ciphertext), and confirm the spec
        // decrypt dispatch reads it back.
        let cryptor = SecretCryptor::try_new(TEST_KEY).unwrap();

        let secret = SecretSpec::Value(SecretValueSpec {
            value: legacy_aes256gcm_hex(b"legacy-secret"),
            content_encoding: Some(CONTENT_ENCODING_AES256GCM.to_string()),
        });

        let decrypted = secret.decrypt_plaintext_bytes(&cryptor).unwrap();
        assert_eq!(decrypted, b"legacy-secret");
    }

    #[test]
    fn lint_skips_value_length_for_encrypted() {
        use kamu_resources::ResourceLinterSpec;

        // A JWE token can exceed WARNING_SECRET_VALUE_LEN; length checks must not
        // apply to encrypted values (the ciphertext is not user-authored text).
        let long_plaintext = "x".repeat(SecretSetSpec::WARNING_SECRET_VALUE_LEN + 1);
        let spec = SecretSetSpec {
            secrets: [("API_TOKEN".to_string(), jwe_secret(&long_plaintext))]
                .into_iter()
                .collect(),
        };

        let warnings = spec.lint_warnings();
        assert!(
            warnings
                .iter()
                .all(|w| w.code != SecretSetSpec::WARNING_CODE_LONG_SECRET_VALUE)
        );
    }

    /// Encrypt with the legacy AES-GCM scheme and pack as `hex(nonce ‖
    /// ciphertext)` — the exact wire form the backfill migrations emit.
    fn legacy_aes256gcm_hex(plaintext: &[u8]) -> String {
        use crypto_utils::{AesGcmEncryptor, Encryptor};
        let aes = AesGcmEncryptor::try_new(TEST_KEY).unwrap();
        let (ciphertext, nonce) = aes.encrypt_bytes(plaintext).unwrap();
        let mut combined = nonce;
        combined.extend_from_slice(&ciphertext);
        hex::encode(combined)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
