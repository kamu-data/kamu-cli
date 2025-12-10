// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_datasets::{ContentArgs, WriteCheckedDataset};

use crate::{
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryDetailedInfo,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeUploadVersionedFileVersionUseCase: Send + Sync {
    async fn execute(
        &self,
        versioned_file_dataset_ref: MoleculeUploadVersionedFileDatasetRef<'_>,
        source_event_time: Option<DateTime<Utc>>,
        content_args: ContentArgs,
        basic_info: MoleculeVersionedFileEntryBasicInfo,
        detailed_info: MoleculeVersionedFileEntryDetailedInfo,
    ) -> Result<MoleculeVersionedFileEntry, MoleculeUploadVersionedFileVersionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum MoleculeUploadVersionedFileDatasetRef<'a> {
    Id(&'a odf::DatasetID),
    WriteChecked(WriteCheckedDataset<'a>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeUploadVersionedFileVersionError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
