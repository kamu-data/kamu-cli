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
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::{FindVersionedFileVersionUseCase, ReadCheckedDataset, ResolvedDataset};
use kamu_molecule_domain::{
    MoleculeReadVersionedFileEntryError,
    MoleculeReadVersionedFileEntryUseCase,
    MoleculeVersionedFileEntry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeReadVersionedFileEntryUseCase)]
pub struct MoleculeReadVersionedFileEntryUseCaseImpl {
    find_versioned_file_version_uc: Arc<dyn FindVersionedFileVersionUseCase>,
    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl MoleculeReadVersionedFileEntryUseCaseImpl {
    async fn readable_versioned_file_dataset(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
    ) -> Result<ReadCheckedDataset<'_>, MoleculeReadVersionedFileEntryError> {
        let readable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &versioned_file_dataset_id.as_local_ref(),
                kamu_core::auth::DatasetAction::Read,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => e.int_err().into(),
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeReadVersionedFileEntryError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(ReadCheckedDataset::from_owned(readable_dataset))
    }
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
        versioned_file_dataset_id: &odf::DatasetID,
        as_of_version: Option<kamu_datasets::FileVersion>,
        as_of_head: Option<odf::Multihash>,
    ) -> Result<
        (Option<MoleculeVersionedFileEntry>, ResolvedDataset),
        MoleculeReadVersionedFileEntryError,
    > {
        let read_checked_versioned_file_dataset = self
            .readable_versioned_file_dataset(versioned_file_dataset_id)
            .await?;

        let maybe_versioned_file_entry = self
            .find_versioned_file_version_uc
            .execute(
                read_checked_versioned_file_dataset.clone(),
                as_of_version,
                as_of_head,
            )
            .await
            .map_err(|e| match e {
                e @ kamu_datasets::FindVersionedFileVersionError::Internal(_) => {
                    MoleculeReadVersionedFileEntryError::Internal(e.int_err())
                }
            })?;

        Ok((
            (maybe_versioned_file_entry
                .map(MoleculeVersionedFileEntry::from_raw_versioned_file_entry)),
            read_checked_versioned_file_dataset.into_inner(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
