// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EntityPageListing, PaginationOpts};
use internal_error::InternalError;
use opendatafabric as odf;
use thiserror::Error;

use crate::{DatasetEntry, DatasetEntryStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEntryService: Sync + Send {
    // TODO: Private Datasets: tests
    // TODO: Private Datasets: extract to DatasetEntryRegistry?
    fn all_entries(&self) -> DatasetEntryStream;

    fn entries_owned_by(&self, owner_id: &odf::AccountID) -> DatasetEntryStream;

    async fn list_all_entries(
        &self,
        pagination: PaginationOpts,
    ) -> Result<EntityPageListing<DatasetEntry>, ListDatasetEntriesError>;

    async fn list_entries_owned_by(
        &self,
        owner_id: &odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<EntityPageListing<DatasetEntry>, ListDatasetEntriesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEntryServiceExt: Sync + Send {
    async fn get_owned_dataset_ids(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<Vec<odf::DatasetID>, InternalError>;
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
    ) -> Result<Vec<odf::DatasetID>, InternalError> {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ListDatasetEntriesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
