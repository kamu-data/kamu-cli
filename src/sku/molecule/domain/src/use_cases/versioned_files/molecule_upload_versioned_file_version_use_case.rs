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
use kamu_datasets::{ContentArgs, ResolvedDataset};

use crate::{MoleculeEncryptionMetadata, MoleculeVersionedFileEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeUploadVersionedFileVersionUseCase: Send + Sync {
    async fn execute(
        &self,
        versioned_file_dataset: ResolvedDataset,
        source_event_time: Option<DateTime<Utc>>,
        content_args: ContentArgs,
        access_level: String,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadata>,
    ) -> Result<MoleculeVersionedFileEntry, MoleculeUploadVersionedFileVersionError>;
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
