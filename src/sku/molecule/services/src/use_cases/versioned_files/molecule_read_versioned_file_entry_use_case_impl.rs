// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ErrorIntoInternal;
use kamu_molecule_domain::{
    MoleculeReadVersionedFileEntryError,
    MoleculeReadVersionedFileEntryUseCase,
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileReadError,
    MoleculeVersionedFileService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeReadVersionedFileEntryUseCase)]
pub struct MoleculeReadVersionedFileEntryUseCaseImpl {
    versioned_file_service: Arc<dyn MoleculeVersionedFileService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeReadVersionedFileEntryUseCase for MoleculeReadVersionedFileEntryUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeReadVersionedFileEntryUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        file_dataset_id: &odf::DatasetID,
        as_of_version: Option<kamu_datasets::FileVersion>,
        as_of_block_hash: Option<odf::Multihash>,
    ) -> Result<Option<MoleculeVersionedFileEntry>, MoleculeReadVersionedFileEntryError> {
        let maybe_versioned_file_entry = self
            .versioned_file_service
            .find_versioned_file_entry(file_dataset_id, as_of_version, as_of_block_hash)
            .await
            .map_err(|e| match e {
                MoleculeVersionedFileReadError::VersionedFileNotFound(e) => e.int_err().into(),
                MoleculeVersionedFileReadError::Access(e) => {
                    MoleculeReadVersionedFileEntryError::Access(e)
                }
                MoleculeVersionedFileReadError::Internal(e) => {
                    MoleculeReadVersionedFileEntryError::Internal(e)
                }
            })?;

        Ok(maybe_versioned_file_entry
            .map(MoleculeVersionedFileEntry::from_raw_versioned_file_entry))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
