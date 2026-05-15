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
use internal_error::ResultIntoInternal;
use kamu_configuration::{EncryptedSecretSpec, SecretSetResource, SecretSetSpec, SecretSpec};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::ResourceSpecSanitizer;
use secrecy::{ExposeSecret, SecretString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceSpecSanitizer<SecretSetResource>)]
pub struct SecretSetSpecSanitizer {
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceSpecSanitizer<SecretSetResource> for SecretSetSpecSanitizer {
    async fn sanitize(
        &self,
        mut spec: SecretSetSpec,
    ) -> Result<SecretSetSpec, internal_error::InternalError> {
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

        let encryption_key = SecretString::from(
            self.secrets_encryption_config
                .encryption_key
                .as_ref()
                .unwrap()
                .clone(),
        );
        let encryptor = AesGcmEncryptor::try_new(encryption_key.expose_secret()).int_err()?;

        for secret in spec.secrets.values_mut() {
            if secret.is_encrypted() {
                continue;
            }
            let plaintext = secret.literal_value().as_bytes();
            let (encrypted_bytes, nonce_bytes) = encryptor.encrypt_bytes(plaintext).int_err()?;
            *secret = SecretSpec::Encrypted(EncryptedSecretSpec {
                encrypted: BASE64_STANDARD.encode(&encrypted_bytes),
                nonce: BASE64_STANDARD.encode(&nonce_bytes),
            });
        }

        Ok(spec)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
