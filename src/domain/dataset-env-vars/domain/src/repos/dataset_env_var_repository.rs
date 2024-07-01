// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::DatasetID;
use thiserror::Error;
use uuid::Uuid;

use crate::DatasetEnvVar;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEnvVarRepository: Send + Sync {
    async fn save_dataset_env_var(
        &self,
        dataset_env_var: &DatasetEnvVar,
    ) -> Result<(), SaveDatasetEnvVarError>;

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
        pagination: &DatasetEnvVarPaginationOpts,
    ) -> Result<Vec<DatasetEnvVar>, GetDatasetEnvVarError>;

    async fn get_dataset_env_var_by_id(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError>;

    async fn get_dataset_env_var_by_key_and_dataset_id(
        &self,
        dataset_env_var_key: &str,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError>;

    async fn delete_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError>;

    async fn modify_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
        new_value: Vec<u8>,
        secret_nonce: Option<Vec<u8>>,
    ) -> Result<(), ModifyDatasetEnvVarError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarPaginationOpts {
    pub limit: i64,
    pub offset: i64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveDatasetEnvVarError {
    #[error(transparent)]
    Internal(InternalError),

    #[error(transparent)]
    Duplicate(SaveDatasetEnvVarErrorDuplicate),
}

#[derive(Error, Debug)]
#[error("Dataset env var not saved, duplicate {dataset_env_var_key} for dataset {dataset_id}")]
pub struct SaveDatasetEnvVarErrorDuplicate {
    pub dataset_env_var_key: String,
    pub dataset_id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetEnvVarError {
    #[error(transparent)]
    NotFound(DatasetEnvVarNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset environment variable not found: '{dataset_env_var_key}'")]
pub struct DatasetEnvVarNotFoundError {
    pub dataset_env_var_key: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetEnvVarError {
    #[error(transparent)]
    NotFound(DatasetEnvVarNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ModifyDatasetEnvVarError {
    #[error(transparent)]
    NotFound(DatasetEnvVarNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}
