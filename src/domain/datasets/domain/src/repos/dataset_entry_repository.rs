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

use crate::DatasetEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEntryRepository: Send + Sync {
    async fn get_dataset(&self, dataset_id: &DatasetID) -> Result<DatasetEntry, GetDatasetError>;

    async fn get_datasets_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<Vec<DatasetEntry>, GetDatasetError>;

    async fn save_dataset(&self, dataset: &DatasetEntry) -> Result<(), SaveDatasetError>;

    async fn update_dataset_alias(
        &self,
        dataset_id: &DatasetID,
        new_alias: &DatasetAlias,
    ) -> Result<(), UpdateDatasetAliasError>;

    async fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DeleteDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

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
    Duplicate(#[from] SaveDatasetErrorDuplicate),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset with dataset_id '{dataset_id}' already exists")]
pub struct SaveDatasetErrorDuplicate {
    pub dataset_id: DatasetID,
}

impl SaveDatasetErrorDuplicate {
    pub fn new(dataset_id: DatasetID) -> Self {
        Self { dataset_id }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateDatasetAliasError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

    #[error(transparent)]
    SameAlias(#[from] DatasetAliasSameError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset with dataset_id '{dataset_id}' same alias '{dataset_alias}' update attempt")]
pub struct DatasetAliasSameError {
    pub dataset_id: DatasetID,
    pub dataset_alias: DatasetAlias,
}

impl DatasetAliasSameError {
    pub fn new(dataset_id: DatasetID, dataset_alias: DatasetAlias) -> Self {
        Self {
            dataset_id,
            dataset_alias,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
