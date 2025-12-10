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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::{ResolvedDataset, UpdateVersionedFileUseCase, WriteCheckedDataset};
use kamu_molecule_domain::{
    MoleculeEncryptionMetadata,
    MoleculeUpdateVersionedFileMetadataError,
    MoleculeUpdateVersionedFileMetadataUseCase,
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryDetailedInfo,
    MoleculeVersionedFileExtraData,
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
    ) -> Result<ResolvedDataset, MoleculeUpdateVersionedFileMetadataError> {
        let writable_dataset = self
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

        Ok(writable_dataset)
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
        access_level: String,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadata>,
    ) -> Result<MoleculeVersionedFileEntry, MoleculeUpdateVersionedFileMetadataError> {
        let versioned_file_dataset = self
            .writable_versioned_file_dataset(versioned_file_dataset_id)
            .await?;

        let versioned_file_basic_info = MoleculeVersionedFileEntryBasicInfo {
            access_level,
            change_by,
            description,
            categories: categories.unwrap_or_default(),
            tags: tags.unwrap_or_default(),
        };

        let versioned_file_detailed_info = MoleculeVersionedFileEntryDetailedInfo {
            content_text,
            encryption_metadata: encryption_metadata.map(MoleculeEncryptionMetadata::into_record),
        };

        let versioned_file_extra_data = MoleculeVersionedFileExtraData {
            basic_info: Cow::Owned(versioned_file_basic_info.clone()),
            detailed_info: Cow::Owned(versioned_file_detailed_info.clone()),
        };

        let update_version_result = self
            .update_versioned_file_uc
            .execute(
                WriteCheckedDataset(&versioned_file_dataset),
                source_event_time,
                None,
                None,
                Some(versioned_file_extra_data.to_extra_data_fields()),
            )
            .await
            .int_err()?;

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
            basic_info: versioned_file_basic_info,
            detailed_info: versioned_file_detailed_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
