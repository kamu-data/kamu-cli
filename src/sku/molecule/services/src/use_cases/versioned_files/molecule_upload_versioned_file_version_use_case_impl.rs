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
use file_utils::MediaType;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::{ContentArgs, UpdateVersionedFileUseCase, WriteCheckedDataset};
use kamu_molecule_domain::{
    MoleculeUploadVersionedFileDatasetRef,
    MoleculeUploadVersionedFileVersionError,
    MoleculeUploadVersionedFileVersionUseCase,
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryDetailedInfo,
    MoleculeVersionedFileEntryExtraData,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeUploadVersionedFileVersionUseCase)]
pub struct MoleculeUploadVersionedFileVersionUseCaseImpl {
    update_versioned_file_uc: Arc<dyn UpdateVersionedFileUseCase>,
    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeUploadVersionedFileVersionUseCaseImpl {
    async fn write_checked_dataset<'a>(
        &self,
        versioned_file_dataset_ref: MoleculeUploadVersionedFileDatasetRef<'a>,
    ) -> Result<WriteCheckedDataset<'a>, MoleculeUploadVersionedFileVersionError> {
        match versioned_file_dataset_ref {
            MoleculeUploadVersionedFileDatasetRef::Id(versioned_file_dataset_id) => {
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
                            MoleculeUploadVersionedFileVersionError::Access(e)
                        }
                        e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
                    })?;

                Ok(WriteCheckedDataset::from_owned(resolved_dataset))
            }
            MoleculeUploadVersionedFileDatasetRef::WriteChecked(write_checked) => Ok(write_checked),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeUploadVersionedFileVersionUseCase for MoleculeUploadVersionedFileVersionUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeUploadVersionedFileVersionUseCaseImpl_execute,
        skip_all,
    )]
    async fn execute(
        &self,
        versioned_file_dataset_ref: MoleculeUploadVersionedFileDatasetRef<'_>,
        source_event_time: Option<DateTime<Utc>>,
        content_args: ContentArgs,
        basic_info: MoleculeVersionedFileEntryBasicInfo,
        detailed_info: MoleculeVersionedFileEntryDetailedInfo,
    ) -> Result<MoleculeVersionedFileEntry, MoleculeUploadVersionedFileVersionError> {
        let write_checked_versioned_file_dataset = self
            .write_checked_dataset(versioned_file_dataset_ref)
            .await?;

        let content_type = content_args.content_type.clone();
        let content_length = content_args.content_length;

        let entry_extra_data = MoleculeVersionedFileEntryExtraData {
            basic_info: Cow::Borrowed(&basic_info),
            detailed_info: Cow::Borrowed(&detailed_info),
        };

        let update_version_result = self
            .update_versioned_file_uc
            .execute(
                write_checked_versioned_file_dataset,
                source_event_time,
                Some(content_args),
                None,
                Some(entry_extra_data.to_fields()),
            )
            .await
            .int_err()?;

        Ok(MoleculeVersionedFileEntry {
            system_time: update_version_result.system_time,
            event_time: source_event_time.unwrap_or(update_version_result.system_time),
            version: update_version_result.new_version,
            content_type: content_type.unwrap_or_else(|| MediaType::OCTET_STREAM.to_owned()),
            content_length,
            content_hash: update_version_result.content_hash,
            basic_info,
            detailed_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
