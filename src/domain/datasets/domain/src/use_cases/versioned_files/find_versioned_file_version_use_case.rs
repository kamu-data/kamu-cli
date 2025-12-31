// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{FileVersion, ReadCheckedDataset, VersionedFileEntry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FindVersionedFileVersionUseCase: Send + Sync {
    async fn execute(
        &self,
        file_dataset: ReadCheckedDataset<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<odf::Multihash>,
    ) -> Result<Option<VersionedFileEntry>, FindVersionedFileVersionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum FindVersionedFileVersionError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
