// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{EntityListing, PaginationOpts};
use internal_error::InternalError;
use opendatafabric as odf;
use thiserror::Error;

use crate::{DatasetEntry, DatasetEntryStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEntryService: Sync + Send {
    // TODO: Private Datasets: extract to DatasetEntryRegistry?
    fn all_entries(&self) -> DatasetEntryStream;

    async fn list_all_entries(
        &self,
        pagination: PaginationOpts,
    ) -> Result<EntityListing<DatasetEntry>, ListDatasetEntriesError>;

    async fn list_entries_owned_by(
        &self,
        owner_id: &odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<EntityListing<DatasetEntry>, ListDatasetEntriesError>;
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
