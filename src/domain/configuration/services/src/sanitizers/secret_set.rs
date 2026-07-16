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
    CONTENT_ENCODING_JWE,
    SecretSetResource,
    SecretSetSpec,
    SecretSpec,
    SecretValueSpec,
};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::ResourceSpecSanitizer;

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
        mut new_spec: SecretSetSpec,
        maybe_current_spec: Option<&SecretSetSpec>,
    ) -> Result<SecretSetSpec, InternalError> {
        let cryptor = self.secrets_encryption_config.new_secret_cryptor()?;

        for (name, new_secret) in &mut new_spec.secrets {
            if new_secret.is_encrypted() {
                continue;
            }
            let new_plaintext = new_secret.literal_value();

            // If the new plaintext matches the current secret (after decryption), keep the
            // current encrypted value to avoid unnecessary changes
            if let Some(current_secret) = maybe_current_spec.and_then(|s| s.secrets.get(name))
                && Self::matches_current_plaintext(current_secret, new_plaintext, &cryptor)?
            {
                *new_secret = current_secret.clone();
                continue;
            }

            // The secret value is new or has changed, encrypt it into a compact JWE token
            let token = cryptor.encrypt_to_jwe(new_plaintext.as_bytes())?;
            *new_secret = SecretSpec::Value(SecretValueSpec {
                value: token,
                content_encoding: Some(CONTENT_ENCODING_JWE.to_string()),
            });
        }

        Ok(new_spec)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetSpecSanitizer {
    fn matches_current_plaintext(
        current_secret: &SecretSpec,
        new_plaintext: &str,
        cryptor: &SecretCryptor,
    ) -> Result<bool, InternalError> {
        if current_secret.as_encrypted().is_none() {
            return Ok(false);
        }

        let decrypted_current = current_secret.decrypt_plaintext_bytes(cryptor)?;
        Ok(decrypted_current == new_plaintext.as_bytes())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
