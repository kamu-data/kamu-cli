// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::MoleculeVersionedFileEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeReadVersionedFileEntryUseCase: Send + Sync {
    async fn execute(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        as_of_version: Option<kamu_datasets::FileVersion>,
        as_of_head: Option<odf::Multihash>,
    ) -> Result<Option<MoleculeVersionedFileEntry>, MoleculeReadVersionedFileEntryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeReadVersionedFileEntryError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
