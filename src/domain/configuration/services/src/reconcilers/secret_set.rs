// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::{AesGcmEncryptor, Encryptor};
use internal_error::ResultIntoInternal;
use kamu_configuration::{
    ReplaceProjectionEntriesError,
    SecretSetEntry,
    SecretSetProjectionRepository,
    SecretSetReconcileError,
    SecretSetReconcileSuccess,
    SecretSetResource,
    SecretSetStats,
};
use kamu_datasets::DatasetEnvVarsConfig;
use kamu_resources::{DeclarativeResource, ReconcilableResource, Reconciler};
use secrecy::{ExposeSecret, SecretString};
use time_source::SystemTimeSource;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Reconciler<SecretSetResource>)]
pub struct SecretSetReconcilerImpl {
    secret_set_projection_repository: Arc<dyn SecretSetProjectionRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    dataset_env_var_config: Arc<DatasetEnvVarsConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reconciler<SecretSetResource> for SecretSetReconcilerImpl {
    async fn reconcile(
        &self,
        resource: &SecretSetResource,
    ) -> Result<
        <SecretSetResource as ReconcilableResource>::ReconcileSuccess,
        <SecretSetResource as ReconcilableResource>::ReconcileError,
    > {
        let total = resource.spec().secrets.len();
        let now = self.time_source.now();
        let resource_id = *resource.resource_id();
        let resource_generation = resource.metadata().generation;
        let account_id = resource.metadata().account.clone();

        let encryption_key = SecretString::from(
            self.dataset_env_var_config
                .encryption_key
                .as_ref()
                .unwrap()
                .clone(),
        );
        let encryptor = AesGcmEncryptor::try_new(encryption_key.expose_secret()).int_err()?;

        let mut entries = Vec::with_capacity(total);
        for (key, secret) in &resource.spec().secrets {
            let (value, secret_nonce) =
                encryptor.encrypt_bytes(secret.value.as_bytes()).int_err()?;
            entries.push(SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                key: key.clone(),
                value,
                secret_nonce,
                updated_at: now,
            });
        }

        self.secret_set_projection_repository
            .replace_entries(&resource_id, resource_generation, &entries)
            .await
            .map_err(|e| match e {
                ReplaceProjectionEntriesError::ConcurrentModification(err) => {
                    SecretSetReconcileError::ConcurrentModification(err)
                }
                ReplaceProjectionEntriesError::Internal(err) => {
                    SecretSetReconcileError::Internal(err)
                }
            })?;

        Ok(SecretSetReconcileSuccess {
            stats: SecretSetStats {
                total_secrets: total,
                valid_secrets: total,
                invalid_secrets: 0,
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
