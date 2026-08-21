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

use internal_error::{InternalError, ResultIntoInternal};
use kamu_configuration::{
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
    SecretSetProjectionRepository,
    SecretSetResource,
    VariableSetProjectionRepository,
    VariableSetResource,
};
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarResolver,
    GetDatasetEnvVarError,
};
use kamu_resources::{ResourceID, ResourceRepository, ResourceSchemaProvider, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DatasetEnvVarResolver)]
pub struct DatasetEnvVarResolverImpl {
    resource_repo: Arc<dyn ResourceRepository>,
    variable_set_projection_repo: Arc<dyn VariableSetProjectionRepository>,
    secret_set_projection_repo: Arc<dyn SecretSetProjectionRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetEnvVarResolverImpl {
    /// Resources of `schema` associated with `dataset_id` by the temporary
    /// `legacy-config-target-dataset` label, oldest first.
    ///
    /// Ordering is by creation time rather than an explicit ordering column,
    /// so editing a set never reshuffles precedence among its peers.
    async fn find_target_resource_ids(
        &self,
        schema: &TypeUri,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<ResourceID>, InternalError> {
        self.resource_repo
            .find_resource_ids_by_schema_and_label(
                schema,
                // Registered labels are stored under their canonical URI, not
                // the short name the user authors.
                RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
                &dataset_id.as_did_str().to_string(),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarResolver for DatasetEnvVarResolverImpl {
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError> {
        let mut env_map: HashMap<String, DatasetEnvVar> = HashMap::new();

        // Apply labelled variable sets oldest-first; first one wins per key
        let variable_set_ids = self
            .find_target_resource_ids(VariableSetResource::schema(), dataset_id)
            .await?;

        for resource_id in &variable_set_ids {
            let entries = self
                .variable_set_projection_repo
                .get_latest_entries(resource_id)
                .await?;

            for entry in entries {
                env_map
                    .entry(entry.key.clone())
                    .or_insert_with(|| DatasetEnvVar {
                        key: entry.key,
                        value: entry.value.into_bytes(),
                        secret_nonce: None,
                        created_at: entry.created_at,
                        dataset_id: dataset_id.clone(),
                    });
            }
        }

        // Apply labelled secret sets; secrets override all variables on key
        // collision
        let secret_set_ids = self
            .find_target_resource_ids(SecretSetResource::schema(), dataset_id)
            .await?;

        let mut secret_map: HashMap<String, DatasetEnvVar> = HashMap::new();
        for resource_id in &secret_set_ids {
            let entries = self
                .secret_set_projection_repo
                .get_latest_entries(resource_id)
                .await?;

            for entry in entries {
                secret_map
                    .entry(entry.key.clone())
                    .or_insert_with(|| DatasetEnvVar {
                        key: entry.key,
                        value: entry.value,
                        secret_nonce: Some(entry.secret_nonce),
                        created_at: entry.created_at,
                        dataset_id: dataset_id.clone(),
                    });
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
        let secret_set_ids = self
            .find_target_resource_ids(SecretSetResource::schema(), dataset_id)
            .await
            .int_err()?;

        for resource_id in &secret_set_ids {
            let entries = self
                .secret_set_projection_repo
                .get_latest_entries(resource_id)
                .await
                .int_err()?;

            if let Some(entry) = entries.into_iter().find(|e| e.key == entry_key) {
                return Ok(DatasetEnvVar {
                    key: entry.key,
                    value: entry.value,
                    secret_nonce: Some(entry.secret_nonce),
                    created_at: entry.created_at,
                    dataset_id: dataset_id.clone(),
                });
            }
        }

        // Only then variable sets, oldest-first within the kind.
        let variable_set_ids = self
            .find_target_resource_ids(VariableSetResource::schema(), dataset_id)
            .await
            .int_err()?;

        for resource_id in &variable_set_ids {
            let entries = self
                .variable_set_projection_repo
                .get_latest_entries(resource_id)
                .await
                .int_err()?;

            if let Some(entry) = entries.into_iter().find(|e| e.key == entry_key) {
                return Ok(DatasetEnvVar {
                    key: entry.key,
                    value: entry.value.into_bytes(),
                    secret_nonce: None,
                    created_at: entry.created_at,
                    dataset_id: dataset_id.clone(),
                });
            }
        }

        Err(not_found())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
