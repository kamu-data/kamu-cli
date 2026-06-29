// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_configuration::{SecretSetResource, SecretSetSpec, SecretSpec};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{
    ResourceDescriptor,
    ResourceDescriptorProvider,
    ResourceDispatcherMeta,
    ResourcePresentation,
    ResourceSpecViewDispatcher,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceSpecViewDispatcher)]
#[dill::meta(ResourceDispatcherMeta {
    schema: <SecretSetResource as ResourceDescriptorProvider>::DESCRIPTOR.schema,
    name: <SecretSetResource as ResourcePresentation>::PRESENTATION.resource_name,
    short_names: <SecretSetResource as ResourcePresentation>::PRESENTATION.resource_short_names,
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
        let mut spec: SecretSetSpec = serde_json::from_value(spec_json).int_err()?;

        let encryptor = self.secrets_encryption_config.new_encryptor()?;

        for secret in spec.secrets.values_mut() {
            if let SecretSpec::Encrypted(enc) = secret {
                let decrypted_bytes = enc.decrypt_plaintext_bytes(&encryptor)?;
                let decrypted_string = String::from_utf8(decrypted_bytes).int_err()?;
                *secret = SecretSpec::Literal(decrypted_string);
            }
        }

        serde_json::to_value(spec).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
