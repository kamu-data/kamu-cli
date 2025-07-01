// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{
    DatasetEntriesResolution,
    DatasetEntry,
    DatasetEntryStream,
    GetDatasetEntryError,
    GetMultipleDatasetEntriesError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait DatasetEntryService: Sync + Send {
    fn all_entries(&self) -> DatasetEntryStream;

    fn entries_owned_by(&self, owner_id: &odf::AccountID) -> DatasetEntryStream;

    async fn get_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError>;

    async fn get_multiple_entries(
        &self,
        dataset_ids: &[&odf::DatasetID],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEntryServiceExt: Sync + Send {
    async fn get_owned_dataset_ids(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<Vec<odf::DatasetID>, GetOwnedDatasetIdsError>;

    async fn is_dataset_owned_by(
        &self,
        dataset_id: &odf::DatasetID,
        account_id: &odf::AccountID,
    ) -> Result<bool, IsDatasetOwnedByError>;
}

#[async_trait::async_trait]
impl<T> DatasetEntryServiceExt for T
where
    T: DatasetEntryService,
    T: ?Sized,
{
    async fn get_owned_dataset_ids(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<Vec<odf::DatasetID>, GetOwnedDatasetIdsError> {
        use futures::TryStreamExt;

        let owned_dataset_ids = self
            .entries_owned_by(owner_id)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|dataset_entry| dataset_entry.id)
            .collect::<Vec<_>>();

        Ok(owned_dataset_ids)
    }

    async fn is_dataset_owned_by(
        &self,
        dataset_id: &odf::DatasetID,
        account_id: &odf::AccountID,
    ) -> Result<bool, IsDatasetOwnedByError> {
        match self.get_entry(dataset_id).await {
            Ok(entry) => Ok(entry.owner_id == *account_id),
            Err(err) => match err {
                GetDatasetEntryError::NotFound(_) => Ok(false),
                unexpected_error => Err(unexpected_error.int_err().into()),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ListDatasetEntriesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum GetOwnedDatasetIdsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum IsDatasetOwnedByError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
