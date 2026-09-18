// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::SecretCryptor;
use internal_error::InternalError;
use kamu_configuration::{
    ContentEncoding,
    Secret,
    SecretExt,
    SecretSetResource,
    SecretSetSpec,
    SecretSetSpecInput,
    SecretSetSpecValidationError,
};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{
    ApplyResourceRejection,
    ApplyResourceRejectionCategory,
    ResourceSpecSanitizer,
    SanitizeSpecOutcome,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceSpecSanitizer<SecretSetResource>)]
pub struct SecretSetSpecSanitizer {
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceSpecSanitizer<SecretSetResource> for SecretSetSpecSanitizer {
    async fn sanitize_new_spec(
        &self,
        mut new_spec: SecretSetSpecInput,
        maybe_current_spec: Option<&SecretSetSpec>,
    ) -> Result<SanitizeSpecOutcome<SecretSetResource>, InternalError> {
        let cryptor = self.secrets_encryption_config.new_secret_cryptor()?;

        for (name, new_secret) in &mut new_spec.secrets.entries {
            // An already-`jwe` secret is left as-is, but only after confirming it
            // decrypts under the current key — structural validation has no key
            // and can't catch a wrong-key or tampered token, so trusting the tag
            // alone would persist it and only fail later, in reconciliation or
            // reveal. A failure here is business input, not a technical fault, so
            // it's a rejection rather than an `InternalError`. Anything else
            // (plaintext, or legacy `aes256gcm` from the env-var backfill
            // migrations) is decrypted and re-encrypted to `jwe`.
            if new_secret.content_encoding() == Some(ContentEncoding::Jwe) {
                if let Err(reason) = new_secret.decrypt_plaintext_bytes(&cryptor) {
                    return Ok(SanitizeSpecOutcome::Rejected(rejection_for_invalid_secret(
                        name,
                        &reason.to_string(),
                    )));
                }
                continue;
            }
            let new_plaintext = new_secret.decrypt_plaintext_bytes(&cryptor)?;

            // Reuse the current ciphertext if the plaintext is unchanged, to avoid
            // unnecessary rewrites.
            if let Some(current_secret) =
                maybe_current_spec.and_then(|s| s.secrets.entries.get(name))
                && current_secret.content_encoding() == Some(ContentEncoding::Jwe)
                && Self::matches_current_plaintext(current_secret, &new_plaintext, &cryptor)?
            {
                *new_secret = current_secret.clone();
                continue;
            }

            // The secret value is new, has changed, or needs upgrading from a legacy
            // encoding — encrypt it into a compact JWE token
            let token = cryptor.encrypt_to_jwe(&new_plaintext)?;
            *new_secret = Secret {
                value: token,
                content_encoding: Some(ContentEncoding::Jwe.as_str().to_string()),
            };
        }

        Ok(SanitizeSpecOutcome::Sanitized(new_spec))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetSpecSanitizer {
    /// Decrypts the *current* (already-persisted) secret to compare against
    /// `new_plaintext`. Unlike the new-value verify-before-trust check above,
    /// a failure here means a previously-accepted secret no longer decrypts
    /// (e.g. the key changed underneath us) — a technical fault, not bad user
    /// input, so it stays an `InternalError`.
    fn matches_current_plaintext(
        current_secret: &Secret,
        new_plaintext: &[u8],
        cryptor: &SecretCryptor,
    ) -> Result<bool, InternalError> {
        let decrypted_current = current_secret.decrypt_plaintext_bytes(cryptor)?;
        Ok(decrypted_current == new_plaintext)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn rejection_for_invalid_secret(name: &str, reason: &str) -> ApplyResourceRejection {
    let err = SecretSetSpecValidationError::InvalidEncryptedSecret {
        name: name.to_string(),
        reason: reason.to_string(),
    };
    ApplyResourceRejection {
        category: ApplyResourceRejectionCategory::BusinessValidationFailed,
        message: err.to_string(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
