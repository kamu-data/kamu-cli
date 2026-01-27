// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::{
    UpdateVersionFileUseCaseError,
    UpdateVersionedFileUseCase,
    WriteCheckedDataset,
};
use kamu_molecule_domain::{
    MoleculeUpdateVersionedFileMetadataError,
    MoleculeUpdateVersionedFileMetadataUseCase,
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryDetailedInfo,
    MoleculeVersionedFileEntryExtraData,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeUpdateVersionedFileMetadataUseCase)]
pub struct MoleculeUpdateVersionedFileMetadataUseCaseImpl {
    update_versioned_file_uc: Arc<dyn UpdateVersionedFileUseCase>,
    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeUpdateVersionedFileMetadataUseCaseImpl {
    async fn writable_versioned_file_dataset(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
    ) -> Result<WriteCheckedDataset<'_>, MoleculeUpdateVersionedFileMetadataError> {
        let resolved_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &versioned_file_dataset_id.as_local_ref(),
                kamu_core::auth::DatasetAction::Write,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => e.int_err().into(),
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeUpdateVersionedFileMetadataError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(WriteCheckedDataset::from_owned(resolved_dataset))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeUpdateVersionedFileMetadataUseCase for MoleculeUpdateVersionedFileMetadataUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeUpdateVersionedFileMetadataUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        existing_versioned_file_entry: MoleculeVersionedFileEntry,
        source_event_time: Option<DateTime<Utc>>,
        expected_head: Option<odf::Multihash>,
        basic_info: MoleculeVersionedFileEntryBasicInfo,
        detailed_info: MoleculeVersionedFileEntryDetailedInfo,
    ) -> Result<MoleculeVersionedFileEntry, MoleculeUpdateVersionedFileMetadataError> {
        let write_checked_versioned_file_dataset = self
            .writable_versioned_file_dataset(versioned_file_dataset_id)
            .await?;

        let entry_extra_data = MoleculeVersionedFileEntryExtraData {
            basic_info: Cow::Borrowed(&basic_info),
            detailed_info: Cow::Borrowed(&detailed_info),
        };

        let update_version_result = self
            .update_versioned_file_uc
            .execute(
                write_checked_versioned_file_dataset,
                source_event_time,
                None,
                expected_head,
                Some(entry_extra_data.to_fields()),
            )
            .await
            .map_err(|e| match e {
                UpdateVersionFileUseCaseError::Access(e) => {
                    MoleculeUpdateVersionedFileMetadataError::Access(e)
                }
                UpdateVersionFileUseCaseError::RefCASFailed(e) => {
                    MoleculeUpdateVersionedFileMetadataError::RefCASFailed(e)
                }
                UpdateVersionFileUseCaseError::QuotaExceeded(e) => {
                    MoleculeUpdateVersionedFileMetadataError::QuotaExceeded(e)
                }
                UpdateVersionFileUseCaseError::Internal(e) => {
                    MoleculeUpdateVersionedFileMetadataError::Internal(e)
                }
            })?;

        assert_eq!(
            existing_versioned_file_entry.content_hash,
            update_version_result.content_hash,
        );

        Ok(MoleculeVersionedFileEntry {
            system_time: update_version_result.system_time,
            event_time: source_event_time.unwrap_or(update_version_result.system_time),
            version: update_version_result.new_version,
            content_type: existing_versioned_file_entry.content_type,
            content_length: existing_versioned_file_entry.content_length,
            content_hash: existing_versioned_file_entry.content_hash,
            basic_info,
            detailed_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
