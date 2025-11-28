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

use crate::{FileVersion, ReadCheckedDataset, VersionedFileEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ViewVersionedFileHistoryUseCase: Send + Sync {
    async fn execute(
        &self,
        file_dataset: ReadCheckedDataset<'_>,
        max_version: Option<FileVersion>,
        pagination: Option<PaginationOpts>,
    ) -> Result<VersionedFileHistoryPage, ViewVersionedFileHistoryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VersionedFileHistoryPage = EntityPageListing<VersionedFileEntry>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ViewVersionedFileHistoryError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
