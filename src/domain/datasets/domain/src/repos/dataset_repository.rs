// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{AccountID, DatasetAlias, DatasetID};
use thiserror::Error;

use crate::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DatasetRepository: Send + Sync {
    async fn get_dataset(&self, dataset_id: &DatasetID) -> Result<Dataset, GetDatasetError>;

    async fn get_datasets_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<Vec<Dataset>, GetDatasetError>;

    async fn save_dataset(&self, dataset: &Dataset) -> Result<(), SaveDatasetError>;

    async fn update_dataset_alias(
        &self,
        dataset_id: &DatasetID,
        new_alias: DatasetAlias,
    ) -> Result<(), UpdateDatasetAliasError>;

    async fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DeleteDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetError {
    #[error(transparent)]
    NotFound(DatasetNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
pub enum DatasetNotFoundError {
    #[error("Dataset not found by dataset_id: '{0}'")]
    ByDatasetId(DatasetID),

    #[error("Datasets not found by owner_id: '{0}'")]
    ByOwnerId(AccountID),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveDatasetError {
    #[error(transparent)]
    Duplicate(SaveDatasetErrorDuplicate),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset with dataset_id '{dataset_id}' already exists")]
pub struct SaveDatasetErrorDuplicate {
    pub dataset_id: DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateDatasetAliasError {
    #[error(transparent)]
    NotFound(DatasetNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetError {
    #[error(transparent)]
    NotFound(DatasetNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
