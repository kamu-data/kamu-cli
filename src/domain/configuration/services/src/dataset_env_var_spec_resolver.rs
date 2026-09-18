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

use crypto_utils::SecretCryptor;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_configuration::{
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
    Secret,
    SecretExt,
    SecretSetResource,
    SecretSetSpec,
    VariableSetResource,
    VariableSetSpec,
};
use kamu_datasets::{
    DatasetEntryRepository,
    DatasetEnvVar,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarSpecResolver,
    GetDatasetEntryError,
    GetDatasetEnvVarError,
    SecretsEncryptionConfig,
};
use kamu_resources::{
    GenericResourceQueryService,
    ResourceID,
    ResourceRepository,
    ResourceSchemaProvider,
    ResourceSnapshot,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Spec-backed counterpart of
/// [`DatasetEnvVarResolverImpl`](crate::DatasetEnvVarResolverImpl); see
/// [`DatasetEnvVarSpecResolver`] for why the two sources exist.
///
/// Semantics are deliberately identical to that resolver — owner scoping,
/// precedence, secret shadowing — so the two must be kept in step.
#[dill::component(pub)]
#[dill::interface(dyn DatasetEnvVarSpecResolver)]
pub struct DatasetEnvVarSpecResolverImpl {
    resource_repo: Arc<dyn ResourceRepository>,
    dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetEnvVarSpecResolverImpl {
    /// Owner scoping is a security boundary, not a filter: label values are
    /// unvalidated on write, so an unscoped lookup would let a stranger inject
    /// variables into someone else's ingest.
    async fn find_target_resource_ids(
        &self,
        schema: &TypeUri,
        dataset_id: &odf::DatasetID,
        owner_id: &odf::AccountID,
    ) -> Result<Vec<ResourceID>, InternalError> {
        self.resource_repo
            .find_resource_ids_by_schema_and_label(
                owner_id,
                schema,
                // Registered labels are stored under their canonical URI, not
                // the short name the user authors.
                RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
                &dataset_id.as_did_str().to_string(),
            )
            .await
    }

    async fn find_dataset_owner(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Option<odf::AccountID>, InternalError> {
        match self
            .dataset_entry_repository
            .get_dataset_entry(dataset_id)
            .await
        {
            Ok(entry) => Ok(Some(entry.owner_id)),
            Err(GetDatasetEntryError::NotFound(_)) => Ok(None),
            Err(GetDatasetEntryError::Internal(e)) => Err(e),
        }
    }

    /// Precedence is positional (oldest set wins) but `find_snapshots_by_ids`
    /// guarantees no ordering, so the caller's order is re-imposed here.
    async fn load_snapshots_in_order(
        &self,
        ids: &[ResourceID],
        owner_id: &odf::AccountID,
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let snapshots = self
            .generic_resource_query_service
            .find_snapshots_by_ids(owner_id, ids)
            .await?;

        let mut by_id: HashMap<ResourceID, ResourceSnapshot> =
            snapshots.into_iter().map(|s| (s.id, s)).collect();

        // A missing snapshot means the resource was deleted between the two
        // queries; skipping matches what the projection resolver would do.
        Ok(ids.iter().filter_map(|id| by_id.remove(id)).collect())
    }

    fn cryptor(&self) -> Result<SecretCryptor, InternalError> {
        self.secrets_encryption_config.new_secret_cryptor()
    }

    /// Specs hold `jwe` (or legacy `aes256gcm`) ciphertext while
    /// [`DatasetEnvVar`] expects raw-AES `(value, nonce)`, so this re-encodes
    /// between them the same way `SecretSetReconcilerImpl` does.
    fn secret_to_env_var(
        cryptor: &SecretCryptor,
        key: &str,
        secret: &Secret,
        created_at: chrono::DateTime<chrono::Utc>,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEnvVar, InternalError> {
        let plaintext = secret.decrypt_plaintext_bytes(cryptor)?;
        let (value, secret_nonce) = cryptor.encrypt_bytes(&plaintext).int_err()?;

        Ok(DatasetEnvVar {
            key: key.to_string(),
            value,
            secret_nonce: Some(secret_nonce),
            created_at,
            dataset_id: dataset_id.clone(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarSpecResolver for DatasetEnvVarSpecResolverImpl {
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError> {
        // Resolved once and threaded down: every lookup below is scoped to the
        // dataset owner, and re-fetching the entry per lookup would query the
        // same row repeatedly in one call.
        let Some(owner_id) = self.find_dataset_owner(dataset_id).await? else {
            return Ok(HashMap::new());
        };

        let mut env_map: HashMap<String, DatasetEnvVar> = HashMap::new();

        // Apply labelled variable sets oldest-first; first one wins per key
        let variable_set_ids = self
            .find_target_resource_ids(VariableSetResource::schema(), dataset_id, &owner_id)
            .await?;

        for snapshot in self
            .load_snapshots_in_order(&variable_set_ids, &owner_id)
            .await?
        {
            // `created_at` is per-resource: the spec carries no per-key
            // timestamp (only the projection does, via its stable entry rows).
            let created_at = snapshot.headers.created_at;
            let spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;

            for (key, variable) in spec.into_dto().variables.entries {
                env_map.entry(key.clone()).or_insert_with(|| DatasetEnvVar {
                    key,
                    value: variable.value.into_bytes(),
                    secret_nonce: None,
                    created_at,
                    dataset_id: dataset_id.clone(),
                });
            }
        }

        // Apply labelled secret sets; secrets override all variables on key
        // collision
        let secret_set_ids = self
            .find_target_resource_ids(SecretSetResource::schema(), dataset_id, &owner_id)
            .await?;

        let mut secret_map: HashMap<String, DatasetEnvVar> = HashMap::new();
        if !secret_set_ids.is_empty() {
            let cryptor = self.cryptor()?;

            for snapshot in self
                .load_snapshots_in_order(&secret_set_ids, &owner_id)
                .await?
            {
                let created_at = snapshot.headers.created_at;
                let spec: SecretSetSpec = serde_json::from_value(snapshot.spec).int_err()?;

                for (key, secret) in spec.into_dto().secrets.entries {
                    if secret_map.contains_key(&key) {
                        continue;
                    }

                    secret_map.insert(
                        key.clone(),
                        Self::secret_to_env_var(&cryptor, &key, &secret, created_at, dataset_id)?,
                    );
                }
            }
        }

        // Secrets override variables
        env_map.extend(secret_map);

        Ok(env_map)
    }

    async fn get_env_var_by_entry_key(
        &self,
        dataset_id: &odf::DatasetID,
        entry_key: &str,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        let not_found = || {
            GetDatasetEnvVarError::NotFound(DatasetEnvVarNotFoundError {
                dataset_env_var_key: entry_key.to_string(),
            })
        };

        // Secret sets are searched first: a key carried by both a secret set and
        // a variable set must resolve to the secret, matching the overlay in
        // `resolve_effective_env_vars`. Returning the variable here would hand
        // back plaintext for a key the user believes they have shadowed with a
        // secret.
        let Some(owner_id) = self.find_dataset_owner(dataset_id).await.int_err()? else {
            return Err(not_found());
        };

        let secret_set_ids = self
            .find_target_resource_ids(SecretSetResource::schema(), dataset_id, &owner_id)
            .await
            .int_err()?;

        if !secret_set_ids.is_empty() {
            let cryptor = self.cryptor().int_err()?;

            for snapshot in self
                .load_snapshots_in_order(&secret_set_ids, &owner_id)
                .await
                .int_err()?
            {
                let created_at = snapshot.headers.created_at;
                let spec: SecretSetSpec = serde_json::from_value(snapshot.spec).int_err()?;

                if let Some(secret) = spec.into_dto().secrets.entries.get(entry_key) {
                    return Self::secret_to_env_var(
                        &cryptor, entry_key, secret, created_at, dataset_id,
                    )
                    .map_err(GetDatasetEnvVarError::Internal);
                }
            }
        }

        // Only then variable sets, oldest-first within the kind.
        let variable_set_ids = self
            .find_target_resource_ids(VariableSetResource::schema(), dataset_id, &owner_id)
            .await
            .int_err()?;

        for snapshot in self
            .load_snapshots_in_order(&variable_set_ids, &owner_id)
            .await
            .int_err()?
        {
            let created_at = snapshot.headers.created_at;
            let spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;

            if let Some(variable) = spec.into_dto().variables.entries.get(entry_key) {
                return Ok(DatasetEnvVar {
                    key: entry_key.to_string(),
                    value: variable.value.clone().into_bytes(),
                    secret_nonce: None,
                    created_at,
                    dataset_id: dataset_id.clone(),
                });
            }
        }

        Err(not_found())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
