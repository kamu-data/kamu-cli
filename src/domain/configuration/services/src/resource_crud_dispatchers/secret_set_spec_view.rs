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
use kamu_configuration::{Secret, SecretExt, SecretSetResource, SecretSetSpec};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{
    ResourceDispatcherMeta,
    ResourceSchemaProvider,
    ResourceSpecViewDispatcher,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ResourceSpecViewDispatcher)]
#[dill::meta(ResourceDispatcherMeta {
    schema: SecretSetResource::SCHEMA_STR,
    canonical_selector: SecretSetResource::CANONICAL_SELECTOR_NAME_STR,
    selector_aliases: SecretSetResource::SELECTOR_ALIAS_STRS,
})]
pub struct SecretSetSpecViewDispatcher {
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSpecViewDispatcher for SecretSetSpecViewDispatcher {
    fn schema(&self) -> &'static TypeUri {
        SecretSetResource::schema()
    }

    fn reveal_spec(
        &self,
        spec_json: serde_json::Value,
    ) -> Result<serde_json::Value, InternalError> {
        let mut spec: SecretSetSpec = serde_json::from_value(spec_json).int_err()?;

        let cryptor = self.secrets_encryption_config.new_secret_cryptor()?;

        for secret in spec.secrets.entries.values_mut() {
            if secret.is_encrypted() {
                let decrypted_bytes = secret.decrypt_plaintext_bytes(&cryptor)?;
                let decrypted_string = String::from_utf8(decrypted_bytes).int_err()?;
                *secret = Secret {
                    value: decrypted_string,
                    content_encoding: None,
                };
            }
        }

        serde_json::to_value(spec).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
