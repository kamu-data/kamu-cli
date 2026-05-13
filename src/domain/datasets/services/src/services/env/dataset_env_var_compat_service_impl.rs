// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarListing,
    DatasetEnvVarMutationAdapter,
    DatasetEnvVarResolver,
    DatasetEnvVarService,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValue,
    DatasetEnvVarsConfig,
    DeleteDatasetEnvVarError,
    GetDatasetEnvVarError,
};
use secrecy::{ExposeSecret, SecretString};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarCompatServiceImpl {
    resolver: Arc<dyn DatasetEnvVarResolver>,
    mutation_adapter: Arc<dyn DatasetEnvVarMutationAdapter>,
    dataset_env_var_encryption_key: Option<SecretString>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEnvVarService)]
impl DatasetEnvVarCompatServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        resolver: Arc<dyn DatasetEnvVarResolver>,
        mutation_adapter: Arc<dyn DatasetEnvVarMutationAdapter>,
        dataset_env_var_config: Arc<DatasetEnvVarsConfig>,
    ) -> Self {
        Self {
            resolver,
            mutation_adapter,
            dataset_env_var_encryption_key: dataset_env_var_config
                .encryption_key
                .as_ref()
                .map(|k| SecretString::from(k.clone())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarService for DatasetEnvVarCompatServiceImpl {
    async fn upsert_dataset_env_var(
        &self,
        dataset_env_var_key: &str,
        dataset_env_var_value: &DatasetEnvVarValue,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        self.mutation_adapter
            .upsert_env_var(dataset_id, dataset_env_var_key, dataset_env_var_value)
            .await
    }

    async fn get_exposed_value(
        &self,
        dataset_env_var: &DatasetEnvVar,
    ) -> Result<String, InternalError> {
        if let Some(ref key) = self.dataset_env_var_encryption_key {
            dataset_env_var
                .get_exposed_decrypted_value(key.expose_secret())
                .int_err()
        } else {
            dataset_env_var.get_exposed_decrypted_value("").int_err()
        }
    }

    async fn get_dataset_env_var_by_id(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        self.resolver
            .get_env_var_by_entry_id(dataset_id, dataset_env_var_id)
            .await
    }

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        dataset_id: &odf::DatasetID,
        pagination: Option<PaginationOpts>,
    ) -> Result<DatasetEnvVarListing, GetDatasetEnvVarError> {
        let env_map = self
            .resolver
            .resolve_effective_env_vars(dataset_id)
            .await
            .int_err()?;

        let mut list: Vec<DatasetEnvVar> = env_map.into_values().collect();
        list.sort_by(|a, b| a.key.cmp(&b.key));

        let total_count = list.len();

        if let Some(pagination) = pagination {
            let offset = pagination.offset;
            let limit = pagination.limit;
            let end = (offset + limit).min(total_count);
            let page = if offset < total_count {
                list[offset..end].to_vec()
            } else {
                vec![]
            };
            Ok(DatasetEnvVarListing {
                list: page,
                total_count,
            })
        } else {
            Ok(DatasetEnvVarListing { list, total_count })
        }
    }

    async fn delete_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        self.mutation_adapter
            .delete_env_var_by_entry_id(dataset_env_var_id)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
