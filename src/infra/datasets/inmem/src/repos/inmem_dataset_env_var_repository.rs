// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use database_common::PaginationOpts;
use dill::*;
use internal_error::InternalError;
use opendatafabric::DatasetID;
use uuid::Uuid;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetEnvVarRepository {
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    dataset_env_var_ids_by_dataset_id: HashMap<DatasetID, Vec<Uuid>>,
    dataset_env_var_ids_by_keys: HashMap<String, Uuid>,
    dataset_env_vars_by_ids: HashMap<Uuid, DatasetEnvVar>,
}

impl State {
    fn new() -> Self {
        Self {
            dataset_env_var_ids_by_dataset_id: HashMap::new(),
            dataset_env_var_ids_by_keys: HashMap::new(),
            dataset_env_vars_by_ids: HashMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEnvVarRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetEnvVarRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarRepository for InMemoryDatasetEnvVarRepository {
    async fn upsert_dataset_env_var(
        &self,
        dataset_env_var: &DatasetEnvVar,
    ) -> Result<UpsertDatasetEnvVarResult, InternalError> {
        let mut guard = self.state.lock().unwrap();

        // Modify env var if such exists
        if let Some(existing_dataset_env_var_key_id) = guard
            .dataset_env_var_ids_by_keys
            .get(&dataset_env_var.key)
            .copied()
            && let Some(existing_dataset_env_var) = guard
                .dataset_env_vars_by_ids
                .get_mut(&existing_dataset_env_var_key_id)
            && existing_dataset_env_var.dataset_id == dataset_env_var.dataset_id
        {
            let mut upsert_status = UpsertDatasetEnvVarStatus::UpToDate;
            if existing_dataset_env_var.value != dataset_env_var.value {
                existing_dataset_env_var
                    .value
                    .clone_from(&dataset_env_var.value);
                upsert_status = UpsertDatasetEnvVarStatus::Updated;
            }
            if (existing_dataset_env_var.secret_nonce.is_none()
                && dataset_env_var.secret_nonce.is_some())
                || (existing_dataset_env_var.secret_nonce.is_some()
                    && dataset_env_var.secret_nonce.is_none())
            {
                existing_dataset_env_var
                    .secret_nonce
                    .clone_from(&dataset_env_var.secret_nonce);
                upsert_status = UpsertDatasetEnvVarStatus::Updated;
            }
            existing_dataset_env_var
                .secret_nonce
                .clone_from(&dataset_env_var.secret_nonce);
            return Ok(UpsertDatasetEnvVarResult {
                id: existing_dataset_env_var.id,
                status: upsert_status,
            });
        }

        // Create a new env var
        guard
            .dataset_env_vars_by_ids
            .insert(dataset_env_var.id, dataset_env_var.clone());
        guard
            .dataset_env_var_ids_by_keys
            .insert(dataset_env_var.key.clone(), dataset_env_var.id);
        let dataset_env_vars_entries = match guard
            .dataset_env_var_ids_by_dataset_id
            .entry(dataset_env_var.dataset_id.clone())
        {
            Entry::Occupied(v) => v.into_mut(),
            Entry::Vacant(v) => v.insert(Vec::default()),
        };
        dataset_env_vars_entries.push(dataset_env_var.id);

        return Ok(UpsertDatasetEnvVarResult {
            id: dataset_env_var.id,
            status: UpsertDatasetEnvVarStatus::Created,
        });
    }

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
        pagination: &PaginationOpts,
    ) -> Result<Vec<DatasetEnvVar>, GetDatasetEnvVarError> {
        let guard = self.state.lock().unwrap();
        if let Some(dataset_env_var_ids) = guard.dataset_env_var_ids_by_dataset_id.get(dataset_id) {
            let dataset_env_vars: Vec<_> = dataset_env_var_ids
                .iter()
                .map(|dataset_env_var_id| {
                    guard
                        .dataset_env_vars_by_ids
                        .get(dataset_env_var_id)
                        .unwrap()
                        .clone()
                })
                .skip(pagination.offset)
                .take(pagination.limit)
                .collect();
            return Ok(dataset_env_vars);
        }
        Ok(vec![])
    }

    async fn get_all_dataset_env_vars_count_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<usize, GetDatasetEnvVarError> {
        let guard = self.state.lock().unwrap();

        if let Some(dataset_env_var_ids) = guard.dataset_env_var_ids_by_dataset_id.get(dataset_id) {
            return Ok(dataset_env_var_ids.len());
        }
        Ok(0)
    }

    async fn get_dataset_env_var_by_id(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        let guard = self.state.lock().unwrap();
        if let Some(existing_dataset_env_var) =
            guard.dataset_env_vars_by_ids.get(dataset_env_var_id)
        {
            return Ok(existing_dataset_env_var.clone());
        }
        return Err(GetDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_id.to_string(),
            },
        ));
    }

    async fn get_dataset_env_var_by_key_and_dataset_id(
        &self,
        dataset_env_var_key: &str,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        let guard = self.state.lock().unwrap();
        if let Some(existing_dataset_env_var_key_id) =
            guard.dataset_env_var_ids_by_keys.get(dataset_env_var_key)
            && let Some(existing_dataset_env_var) = guard
                .dataset_env_vars_by_ids
                .get(existing_dataset_env_var_key_id)
            && &existing_dataset_env_var.dataset_id == dataset_id
        {
            return Ok(existing_dataset_env_var.clone());
        }
        return Err(GetDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_key.to_string(),
            },
        ));
    }

    async fn delete_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        let mut guard = self.state.lock().unwrap();
        if let Some((_, existing_dataset_env_var)) = guard
            .dataset_env_vars_by_ids
            .remove_entry(dataset_env_var_id)
        {
            guard
                .dataset_env_var_ids_by_keys
                .remove(&existing_dataset_env_var.key);
            let dataset_env_var_ids = guard
                .dataset_env_var_ids_by_dataset_id
                .get_mut(&existing_dataset_env_var.dataset_id)
                .unwrap();
            dataset_env_var_ids.retain(|env_var_id| env_var_id != dataset_env_var_id);
            if dataset_env_var_ids.is_empty() {
                guard
                    .dataset_env_var_ids_by_dataset_id
                    .remove(&existing_dataset_env_var.dataset_id);
            }
            return Ok(());
        }

        Err(DeleteDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_id.to_string(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetEnvVarRepository {
    async fn on_dataset_entry_removed(&self, dataset_id: &DatasetID) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();

        if let Some(env_var_ids) = guard.dataset_env_var_ids_by_dataset_id.remove(dataset_id) {
            for env_var_id in env_var_ids {
                if let Some(env_var) = guard.dataset_env_vars_by_ids.remove(&env_var_id) {
                    guard.dataset_env_var_ids_by_keys.remove(&env_var.key);
                }
            }
        }

        Ok(())
    }
}
