// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
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
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{DeclarativeResource, ReconcilableResource, Reconciler, ResourceID};
use odf::AccountID;
use time_source::SystemTimeSource;
use uuid::Uuid;

use crate::PreviousConfigurationEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Reconciler<SecretSetResource>)]
pub struct SecretSetReconcilerImpl {
    secret_set_projection_repository: Arc<dyn SecretSetProjectionRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
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
        let resource_id = *resource.id();
        let resource_generation = resource.headers().generation;
        let account_id = &resource.headers().account;

        let previous_entries_by_key = self
            .load_previous_entries_by_key(&resource_id, resource_generation)
            .await?;

        let encryptor = self.create_encryptor()?;

        let entries = self.build_secret_entries(
            &encryptor,
            &resource.spec().secrets,
            &previous_entries_by_key,
            account_id,
            now,
        )?;

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

impl SecretSetReconcilerImpl {
    async fn load_previous_entries_by_key(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<HashMap<String, PreviousConfigurationEntry>, SecretSetReconcileError> {
        if resource_generation == 0 {
            return Ok(HashMap::new());
        }

        let entries = self
            .secret_set_projection_repository
            .get_latest_entries_before_generation(resource_id, resource_generation)
            .await
            .map_err(SecretSetReconcileError::Internal)?;

        Ok(entries
            .into_iter()
            .map(|entry| {
                (
                    entry.key,
                    PreviousConfigurationEntry {
                        entry_id: entry.entry_id,
                        created_at: entry.created_at,
                    },
                )
            })
            .collect())
    }

    fn create_encryptor(&self) -> Result<AesGcmEncryptor, SecretSetReconcileError> {
        self.secrets_encryption_config
            .new_encryptor()
            .map_err(SecretSetReconcileError::Internal)
    }

    fn build_secret_entries(
        &self,
        encryptor: &AesGcmEncryptor,
        secrets: &std::collections::BTreeMap<String, kamu_configuration::SecretSpec>,
        previous_entries_by_key: &HashMap<String, PreviousConfigurationEntry>,
        account_id: &AccountID,
        now: DateTime<Utc>,
    ) -> Result<Vec<SecretSetEntry>, SecretSetReconcileError> {
        let mut entries = Vec::with_capacity(secrets.len());

        for (key, secret) in secrets {
            let plaintext = secret
                .decrypt_plaintext_bytes(encryptor)
                .map_err(SecretSetReconcileError::Internal)?;
            let (value, secret_nonce) = encryptor
                .encrypt_bytes(&plaintext)
                .int_err()
                .map_err(SecretSetReconcileError::Internal)?;

            let (entry_id, created_at) = previous_entries_by_key
                .get(key)
                .map(|entry| (entry.entry_id, entry.created_at))
                .unwrap_or_else(|| (Uuid::new_v4(), now));

            entries.push(SecretSetEntry {
                entry_id,
                account_id: account_id.clone(),
                key: key.clone(),
                value,
                secret_nonce,
                created_at,
                updated_at: now,
            });
        }

        Ok(entries)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
