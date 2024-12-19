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
use thiserror::Error;
use uuid::Uuid;

use crate::DatasetEnvVar;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEnvVarRepository: Send + Sync {
    async fn upsert_dataset_env_var(
        &self,
        dataset_env_var: &DatasetEnvVar,
    ) -> Result<UpsertDatasetEnvVarResult, InternalError>;

    async fn get_all_dataset_env_vars_count_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<usize, GetDatasetEnvVarError>;

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
        pagination: &PaginationOpts,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetEnvVarError {
    #[error(transparent)]
    NotFound(DatasetEnvVarNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
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
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UpsertDatasetEnvVarResult {
    pub id: Uuid,
    pub status: UpsertDatasetEnvVarStatus,
}

#[derive(Debug, PartialEq, Eq)]
pub enum UpsertDatasetEnvVarStatus {
    Created,
    Updated,
    UpToDate,
}
