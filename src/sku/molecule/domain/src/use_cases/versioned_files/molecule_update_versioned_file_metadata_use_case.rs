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

use crate::{MoleculeEncryptionMetadata, MoleculeVersionedFileEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeUpdateVersionedFileMetadataUseCase: Send + Sync {
    async fn execute(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        existing_versioned_file_entry: MoleculeVersionedFileEntry,
        source_event_time: Option<DateTime<Utc>>,
        access_level: String,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadata>,
    ) -> Result<MoleculeVersionedFileEntry, MoleculeUpdateVersionedFileMetadataError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeUpdateVersionedFileMetadataError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
