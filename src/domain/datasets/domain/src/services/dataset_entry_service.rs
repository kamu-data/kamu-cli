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
use opendatafabric::AccountID;
use thiserror::Error;

use crate::DatasetEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetEntryService: Sync + Send {
    async fn list_all_entries(
        &self,
        pagination: PaginationOpts,
    ) -> Result<DatasetEntryListing, ListDatasetEntriesError>;

    async fn list_entries_owned_by(
        &self,
        owner_id: AccountID,
        pagination: PaginationOpts,
    ) -> Result<DatasetEntryListing, ListDatasetEntriesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEntryListing {
    pub list: Vec<DatasetEntry>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ListDatasetEntriesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
