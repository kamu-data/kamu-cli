// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::DatabasePaginationOpts;
use dill::*;
use internal_error::ResultIntoInternal;
use kamu_core::{ErrorIntoInternal, SystemTimeSource};
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarListing,
    DatasetEnvVarRepository,
    DatasetEnvVarService,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    GetDatasetEnvVarError,
    ModifyDatasetEnvVarError,
    SaveDatasetEnvVarError,
};
use opendatafabric::DatasetID;
use secrecy::Secret;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarServiceImpl {
    dataset_env_var_repository: Arc<dyn DatasetEnvVarRepository>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEnvVarService)]
impl DatasetEnvVarServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        dataset_env_var_repository: Arc<dyn DatasetEnvVarRepository>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            dataset_env_var_repository,
            time_source,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarService for DatasetEnvVarServiceImpl {
    async fn create_dataset_env_var(
        &self,
        dataset_env_var_key: &str,
        dataset_env_var_value: &DatasetEnvVarValue,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVar, SaveDatasetEnvVarError> {
        let dataset_env_var = DatasetEnvVar::new(
            dataset_env_var_key,
            self.time_source.now(),
            dataset_env_var_value,
            dataset_id,
        )
        .map_err(|err| SaveDatasetEnvVarError::Internal(err.int_err()))?;
        self.dataset_env_var_repository
            .save_dataset_env_var(&dataset_env_var)
            .await?;
        Ok(dataset_env_var)
    }

    async fn get_dataset_env_var_value_by_key_and_dataset_id(
        &self,
        dataset_env_var_key: &str,
        dataset_id: &DatasetID,
    ) -> Result<Secret<String>, GetDatasetEnvVarError> {
        let existing_env_var = self
            .dataset_env_var_repository
            .get_dataset_env_var_by_key_and_dataset_id(dataset_env_var_key, dataset_id)
            .await?;

        Ok(Secret::new(existing_env_var.get_exposed_value().map_err(
            |err| GetDatasetEnvVarError::Internal(err.int_err()),
        )?))
    }

    async fn get_dataset_env_var_by_id(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        self.dataset_env_var_repository
            .get_dataset_env_var_by_id(dataset_env_var_id)
            .await
    }

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
        pagination: &DatabasePaginationOpts,
    ) -> Result<DatasetEnvVarListing, GetDatasetEnvVarError> {
        let total_count = self
            .dataset_env_var_repository
            .get_all_dataset_env_vars_count_by_dataset_id(dataset_id)
            .await?;
        if total_count == 0 {
            return Ok(DatasetEnvVarListing {
                total_count,
                list: vec![],
            });
        }

        let dataset_env_var_list = self
            .dataset_env_var_repository
            .get_all_dataset_env_vars_by_dataset_id(dataset_id, pagination)
            .await?;
        Ok(DatasetEnvVarListing {
            list: dataset_env_var_list,
            total_count,
        })
    }

    async fn delete_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        self.dataset_env_var_repository
            .delete_dataset_env_var(dataset_env_var_id)
            .await
    }

    async fn modify_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
        dataset_env_var_new_value: &DatasetEnvVarValue,
    ) -> Result<(), ModifyDatasetEnvVarError> {
        let existing_dataset_env_var = self
            .dataset_env_var_repository
            .get_dataset_env_var_by_id(dataset_env_var_id)
            .await
            .map_err(|err| match err {
                GetDatasetEnvVarError::NotFound(e) => ModifyDatasetEnvVarError::NotFound(e),
                GetDatasetEnvVarError::Internal(e) => ModifyDatasetEnvVarError::Internal(e),
            })?;

        let (new_value, nonce) = existing_dataset_env_var
            .generate_new_value(dataset_env_var_new_value)
            .int_err()
            .map_err(ModifyDatasetEnvVarError::Internal)?;
        self.dataset_env_var_repository
            .modify_dataset_env_var(dataset_env_var_id, new_value, nonce)
            .await
    }
}