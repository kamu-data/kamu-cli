// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use base64::Engine;
use crypto_utils::{AesGcmEncryptor, Encryptor};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_configuration::{EncryptedSecretSpec, SecretSetResource, SecretSetSpec, SecretSpec};
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
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

        let encryptor = self.secrets_encryption_config.new_encryptor()?;

        for (name, new_secret) in &mut new_spec.secrets {
            if new_secret.is_encrypted() {
                continue;
            }
            let new_plaintext = new_secret.literal_value();

            // If the new plaintext matches the current secret (after decryption), keep the
            // current encrypted value to avoid unnecessary changes
            if let Some(current_secret) = maybe_current_spec.and_then(|s| s.secrets.get(name))
                && Self::matches_current_plaintext(current_secret, new_plaintext, &encryptor)?
            {
                *new_secret = current_secret.clone();
                continue;
            }

            // The secret value is new or has changed, encrypt it
            let (encrypted_bytes, nonce_bytes) = encryptor
                .encrypt_bytes(new_plaintext.as_bytes())
                .int_err()?;
            *new_secret = SecretSpec::Encrypted(EncryptedSecretSpec {
                encrypted: BASE64_STANDARD.encode(&encrypted_bytes),
                nonce: BASE64_STANDARD.encode(&nonce_bytes),
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
        encryptor: &AesGcmEncryptor,
    ) -> Result<bool, InternalError> {
        let Some(current_encrypted) = current_secret.as_encrypted() else {
            return Ok(false);
        };

        let decrypted_current = current_encrypted.decrypt_plaintext_bytes(encryptor)?;
        Ok(decrypted_current == new_plaintext.as_bytes())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
