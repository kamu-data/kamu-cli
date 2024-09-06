// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;
use opendatafabric::DatasetID;
use uuid::Uuid;

use crate::{
    DatasetEnvVar,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    GetDatasetEnvVarError,
    ModifyDatasetEnvVarError,
    SaveDatasetEnvVarError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEnvVarService: Sync + Send {
    async fn create_dataset_env_var(
        &self,
        dataset_env_var_key: &str,
        dataset_env_var_value: &DatasetEnvVarValue,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVar, SaveDatasetEnvVarError>;

    async fn get_dataset_env_var_by_id(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError>;

    async fn get_exposed_value(
        &self,
        dataset_env_var: &DatasetEnvVar,
    ) -> Result<String, InternalError>;

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
        pagination: Option<PaginationOpts>,
    ) -> Result<DatasetEnvVarListing, GetDatasetEnvVarError>;

    async fn delete_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError>;

    async fn modify_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
        dataset_env_var_new_value: &DatasetEnvVarValue,
    ) -> Result<(), ModifyDatasetEnvVarError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarListing {
    pub list: Vec<DatasetEnvVar>,
    pub total_count: usize,
}
