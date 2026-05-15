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
use kamu_configuration::{SecretSetResource, SecretSetSpec, SecretSpec};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{ResourceDescriptor, ResourceDescriptorProvider, ResourceSpecViewDispatcher};
use secrecy::{ExposeSecret, SecretString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceSpecViewDispatcher)]
#[dill::meta(kamu_resources::ResourceDispatcherMeta {
    descriptor: <SecretSetResource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR,
})]
pub struct SecretSetSpecViewDispatcher {
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSpecViewDispatcher for SecretSetSpecViewDispatcher {
    fn descriptor(&self) -> ResourceDescriptor {
        SecretSetResource::DESCRIPTOR
    }

    fn reveal_spec(
        &self,
        spec_json: serde_json::Value,
    ) -> Result<serde_json::Value, InternalError> {
        use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

        let mut spec: SecretSetSpec = serde_json::from_value(spec_json).int_err()?;

        let encryption_key = SecretString::from(
            self.secrets_encryption_config
                .encryption_key
                .as_ref()
                .unwrap()
                .clone(),
        );
        let encryptor = AesGcmEncryptor::try_new(encryption_key.expose_secret()).int_err()?;

        for secret in spec.secrets.values_mut() {
            if let SecretSpec::Encrypted(enc) = secret {
                let encrypted_bytes = BASE64_STANDARD.decode(&enc.encrypted).int_err()?;
                let nonce_bytes = BASE64_STANDARD.decode(&enc.nonce).int_err()?;
                let plaintext = encryptor
                    .decrypt_bytes(&encrypted_bytes, &nonce_bytes)
                    .int_err()?;
                let plaintext_str = String::from_utf8(plaintext).int_err()?;
                *secret = SecretSpec::Literal(plaintext_str);
            }
        }

        serde_json::to_value(spec).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
