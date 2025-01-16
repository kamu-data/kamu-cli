// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use database_common::PaginationOpts;
use internal_error::InternalError;
use opendatafabric as odf;
use thiserror::Error;

use crate::DatasetEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetEntryRepository: Send + Sync {
    async fn dataset_entries_count(&self) -> Result<usize, DatasetEntriesCountError>;

    async fn dataset_entries_count_by_owner_id(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<usize, InternalError>;

    async fn get_dataset_entries<'a>(
        &'a self,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'a>;

    async fn get_dataset_entries_by_owner_id<'a>(
        &'a self,
        owner_id: &odf::AccountID,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'a>;

    async fn get_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError>;

    async fn get_multiple_dataset_entries(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError>;

    async fn get_dataset_entry_by_owner_and_name(
        &self,
        owner_id: &odf::AccountID,
        name: &odf::DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError>;

    async fn save_dataset_entry(
        &self,
        dataset_entry: &DatasetEntry,
    ) -> Result<(), SaveDatasetEntryError>;

    async fn update_dataset_entry_name(
        &self,
        dataset_id: &odf::DatasetID,
        new_name: &odf::DatasetName,
    ) -> Result<(), UpdateDatasetEntryNameError>;

    async fn delete_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeleteEntryDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetEntryStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<DatasetEntry, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Eq, PartialEq)]
pub struct DatasetEntriesResolution {
    pub resolved_entries: Vec<DatasetEntry>,
    pub unresolved_entries: Vec<odf::DatasetID>,
}

impl DatasetEntriesResolution {
    pub fn resolved_entries_owner_ids(&self) -> HashSet<odf::AccountID> {
        self.resolved_entries
            .iter()
            .fold(HashSet::new(), |mut acc, entry| {
                acc.insert(entry.owner_id.clone());
                acc
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DatasetEntriesCountError {
    #[error(transparent)]
    Internal(#[from] InternalError),
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
pub enum GetMultipleDatasetEntriesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Dataset entry with dataset_id '{dataset_id}' not found")]
pub struct DatasetEntryNotFoundError {
    pub dataset_id: odf::DatasetID,
}

impl DatasetEntryNotFoundError {
    pub fn new(dataset_id: odf::DatasetID) -> Self {
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
    pub owner_id: odf::AccountID,
    pub dataset_name: odf::DatasetName,
}

impl DatasetEntryByNameNotFoundError {
    pub fn new(owner_id: odf::AccountID, dataset_name: odf::DatasetName) -> Self {
        Self {
            owner_id,
            dataset_name,
        }
    }
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
    pub dataset_id: odf::DatasetID,
}

impl SaveDatasetEntryErrorDuplicate {
    pub fn new(dataset_id: odf::DatasetID) -> Self {
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
    pub dataset_name: odf::DatasetName,
}

impl DatasetEntryNameCollisionError {
    pub fn new(dataset_name: odf::DatasetName) -> Self {
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

#[async_trait::async_trait]
pub trait DatasetEntryRemovalListener: Send + Sync {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
