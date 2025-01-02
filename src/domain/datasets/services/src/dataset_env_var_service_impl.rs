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
    DatasetEnvVarRepository,
    DatasetEnvVarService,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValue,
    DatasetEnvVarsConfig,
    DeleteDatasetEnvVarError,
    GetDatasetEnvVarError,
};
use opendatafabric::DatasetID;
use secrecy::{ExposeSecret, SecretString};
use time_source::SystemTimeSource;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarServiceImpl {
    dataset_env_var_repository: Arc<dyn DatasetEnvVarRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    dataset_env_var_encryption_key: SecretString,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEnvVarService)]
impl DatasetEnvVarServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        dataset_env_var_repository: Arc<dyn DatasetEnvVarRepository>,
        time_source: Arc<dyn SystemTimeSource>,
        dataset_env_var_config: Arc<DatasetEnvVarsConfig>,
    ) -> Self {
        Self {
            dataset_env_var_repository,
            time_source,
            dataset_env_var_encryption_key: SecretString::from(
                dataset_env_var_config
                    .encryption_key
                    .as_ref()
                    .unwrap()
                    .clone(),
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarService for DatasetEnvVarServiceImpl {
    async fn upsert_dataset_env_var(
        &self,
        dataset_env_var_key: &str,
        dataset_env_var_value: &DatasetEnvVarValue,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let mut dataset_env_var = DatasetEnvVar::new(
            dataset_env_var_key,
            self.time_source.now(),
            dataset_env_var_value,
            dataset_id,
            self.dataset_env_var_encryption_key.expose_secret(),
        )
        .int_err()?;

        let upsert_result = self
            .dataset_env_var_repository
            .upsert_dataset_env_var(&dataset_env_var)
            .await?;
        dataset_env_var.id = upsert_result.id;

        Ok(DatasetEnvVarUpsertResult {
            dataset_env_var,
            status: upsert_result.status,
        })
    }

    async fn get_exposed_value(
        &self,
        dataset_env_var: &DatasetEnvVar,
    ) -> Result<String, InternalError> {
        dataset_env_var
            .get_exposed_decrypted_value(self.dataset_env_var_encryption_key.expose_secret())
            .int_err()
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
        pagination: Option<PaginationOpts>,
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
        let database_pagination = pagination.unwrap_or(PaginationOpts {
            limit: total_count,
            offset: 0,
        });

        let dataset_env_var_list = self
            .dataset_env_var_repository
            .get_all_dataset_env_vars_by_dataset_id(dataset_id, &database_pagination)
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
}
