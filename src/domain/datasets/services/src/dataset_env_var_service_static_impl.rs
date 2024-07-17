// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::DatabasePaginationOpts;
use dill::*;
use kamu_core::{ErrorIntoInternal, InternalError};
use kamu_datasets::{
    DatasetEnvVar,
    DatasetEnvVarListing,
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

pub struct DatasetEnvVarServiceStaticImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetEnvVarService)]
impl DatasetEnvVarServiceStaticImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new() -> Self {
        Self {}
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarService for DatasetEnvVarServiceStaticImpl {
    async fn create_dataset_env_var(
        &self,
        _dataset_env_var_key: &str,
        _dataset_env_var_value: &DatasetEnvVarValue,
        _dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVar, SaveDatasetEnvVarError> {
        unreachable!()
    }

    async fn get_dataset_env_var_value_by_key_and_dataset_id(
        &self,
        dataset_env_var_key: &str,
        _dataset_id: &DatasetID,
    ) -> Result<Secret<String>, GetDatasetEnvVarError> {
        match std::env::var(dataset_env_var_key) {
            Ok(value_string) => Ok(Secret::new(value_string)),
            Err(err) => Err(GetDatasetEnvVarError::Internal(err.int_err())),
        }
    }

    async fn get_dataset_env_var_by_id(
        &self,
        _dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        unreachable!()
    }

    async fn get_exposed_value(
        &self,
        _dataset_env_var: &DatasetEnvVar,
    ) -> Result<String, InternalError> {
        unreachable!()
    }

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        _dataset_id: &DatasetID,
        _pagination: &DatabasePaginationOpts,
    ) -> Result<DatasetEnvVarListing, GetDatasetEnvVarError> {
        unreachable!()
    }

    async fn delete_dataset_env_var(
        &self,
        _dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        unreachable!()
    }

    async fn modify_dataset_env_var(
        &self,
        _dataset_env_var_id: &Uuid,
        _dataset_env_var_new_value: &DatasetEnvVarValue,
    ) -> Result<(), ModifyDatasetEnvVarError> {
        unreachable!()
    }
}
