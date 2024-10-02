// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{AccountID, DatasetID, DatasetName};
use thiserror::Error;

use crate::DatasetEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetEntryRepository: Send + Sync {
    async fn dataset_entries_count(&self) -> Result<usize, GetDatasetEntryError>;

    async fn get_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError>;

    async fn get_dataset_entry_by_name(
        &self,
        owner_id: &AccountID,
        name: &DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError>;

    async fn get_dataset_entries_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<Vec<DatasetEntry>, GetDatasetEntriesByOwnerIdError>;

    async fn save_dataset_entry(
        &self,
        dataset_entry: &DatasetEntry,
    ) -> Result<(), SaveDatasetEntryError>;

    async fn update_dataset_entry_name(
        &self,
        dataset_id: &DatasetID,
        new_name: &DatasetName,
    ) -> Result<(), UpdateDatasetEntryNameError>;

    async fn delete_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<(), DeleteEntryDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetEntryError {
    #[error(transparent)]
    NotFound(#[from] DatasetEntryNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset entry with dataset_id '{dataset_id}' not found")]
pub struct DatasetEntryNotFoundError {
    pub dataset_id: DatasetID,
}

impl DatasetEntryNotFoundError {
    pub fn new(dataset_id: DatasetID) -> Self {
        Self { dataset_id }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetEntryByNameError {
    #[error(transparent)]
    NotFound(#[from] DatasetEntryByNameNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset entry with owner_id '{owner_id}' and name '{dataset_name}' not found")]
pub struct DatasetEntryByNameNotFoundError {
    pub owner_id: AccountID,
    pub dataset_name: DatasetName,
}

impl DatasetEntryByNameNotFoundError {
    pub fn new(owner_id: AccountID, dataset_name: DatasetName) -> Self {
        Self {
            owner_id,
            dataset_name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetEntriesByOwnerIdError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveDatasetEntryError {
    #[error(transparent)]
    Duplicate(#[from] SaveDatasetEntryErrorDuplicate),

    #[error(transparent)]
    NameCollision(#[from] DatasetEntryNameCollisionError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset entry with dataset_id '{dataset_id}' already exists")]
pub struct SaveDatasetEntryErrorDuplicate {
    pub dataset_id: DatasetID,
}

impl SaveDatasetEntryErrorDuplicate {
    pub fn new(dataset_id: DatasetID) -> Self {
        Self { dataset_id }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateDatasetEntryNameError {
    #[error(transparent)]
    NotFound(#[from] DatasetEntryNotFoundError),

    #[error(transparent)]
    NameCollision(#[from] DatasetEntryNameCollisionError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset entry with name {dataset_name} for same owner already exists")]
pub struct DatasetEntryNameCollisionError {
    pub dataset_name: DatasetName,
}

impl DatasetEntryNameCollisionError {
    pub fn new(dataset_name: DatasetName) -> Self {
        Self { dataset_name }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteEntryDatasetError {
    #[error(transparent)]
    NotFound(#[from] DatasetEntryNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
