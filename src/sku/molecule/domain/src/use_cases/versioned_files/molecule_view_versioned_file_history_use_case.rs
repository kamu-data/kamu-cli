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

use crate::MoleculeVersionedFileEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeViewVersionedFileHistoryUseCase: Send + Sync {
    /// Versions of the file in reverse chronological order, up to
    /// `max_version` (inclusive) when it is set
    async fn execute(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        max_version: Option<kamu_datasets::FileVersion>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeVersionedFileHistoryPage, MoleculeViewVersionedFileHistoryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeVersionedFileHistoryPage = EntityPageListing<MoleculeVersionedFileEntry>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeViewVersionedFileHistoryError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
