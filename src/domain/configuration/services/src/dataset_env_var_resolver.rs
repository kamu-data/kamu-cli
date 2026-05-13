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
    DatasetSecretSetBindingRepository,
    DatasetVariableSetBindingRepository,
    SecretSetProjectionRepository,
    VariableSetProjectionRepository,
};
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarResolver,
    GetDatasetEnvVarError,
};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DatasetEnvVarResolver)]
pub struct DatasetEnvVarResolverImpl {
    variable_set_binding_repo: Arc<dyn DatasetVariableSetBindingRepository>,
    secret_set_binding_repo: Arc<dyn DatasetSecretSetBindingRepository>,
    variable_set_projection_repo: Arc<dyn VariableSetProjectionRepository>,
    secret_set_projection_repo: Arc<dyn SecretSetProjectionRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarResolver for DatasetEnvVarResolverImpl {
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError> {
        let mut env_map: HashMap<String, DatasetEnvVar> = HashMap::new();

        // Apply variable-set bindings in order; first binding wins per key
        let var_bindings = self
            .variable_set_binding_repo
            .list_bindings(dataset_id)
            .await?;

        for binding in &var_bindings {
            let entries = self
                .variable_set_projection_repo
                .get_latest_entries(&binding.resource_uid)
                .await?;

            for entry in entries {
                env_map
                    .entry(entry.key.clone())
                    .or_insert_with(|| DatasetEnvVar {
                        id: entry.entry_id,
                        key: entry.key,
                        value: entry.value.into_bytes(),
                        secret_nonce: None,
                        created_at: entry.created_at,
                        dataset_id: dataset_id.clone(),
                    });
            }
        }

        // Apply secret-set bindings; secrets override all variables on key collision
        let secret_bindings = self
            .secret_set_binding_repo
            .list_bindings(dataset_id)
            .await?;

        let mut secret_map: HashMap<String, DatasetEnvVar> = HashMap::new();
        for binding in &secret_bindings {
            let entries = self
                .secret_set_projection_repo
                .get_latest_entries(&binding.resource_uid)
                .await?;

            for entry in entries {
                secret_map
                    .entry(entry.key.clone())
                    .or_insert_with(|| DatasetEnvVar {
                        id: entry.entry_id,
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

    async fn get_env_var_by_entry_id(
        &self,
        dataset_id: &odf::DatasetID,
        entry_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        let not_found = || {
            GetDatasetEnvVarError::NotFound(DatasetEnvVarNotFoundError {
                dataset_env_var_key: entry_id.to_string(),
            })
        };

        // Search variable-set resources bound to this dataset
        let var_bindings = self
            .variable_set_binding_repo
            .list_bindings(dataset_id)
            .await
            .int_err()?;

        for binding in &var_bindings {
            let entries = self
                .variable_set_projection_repo
                .get_latest_entries(&binding.resource_uid)
                .await
                .int_err()?;

            if let Some(entry) = entries.into_iter().find(|e| e.entry_id == *entry_id) {
                return Ok(DatasetEnvVar {
                    id: entry.entry_id,
                    key: entry.key,
                    value: entry.value.into_bytes(),
                    secret_nonce: None,
                    created_at: entry.created_at,
                    dataset_id: dataset_id.clone(),
                });
            }
        }

        // Search secret-set resources bound to this dataset
        let secret_bindings = self
            .secret_set_binding_repo
            .list_bindings(dataset_id)
            .await
            .int_err()?;

        for binding in &secret_bindings {
            let entries = self
                .secret_set_projection_repo
                .get_latest_entries(&binding.resource_uid)
                .await
                .int_err()?;

            if let Some(entry) = entries.into_iter().find(|e| e.entry_id == *entry_id) {
                return Ok(DatasetEnvVar {
                    id: entry.entry_id,
                    key: entry.key,
                    value: entry.value,
                    secret_nonce: Some(entry.secret_nonce),
                    created_at: entry.created_at,
                    dataset_id: dataset_id.clone(),
                });
            }
        }

        Err(not_found())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
