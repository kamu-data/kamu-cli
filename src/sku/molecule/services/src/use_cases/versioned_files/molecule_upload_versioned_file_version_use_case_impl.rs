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
use internal_error::ResultIntoInternal;
use kamu_datasets::{
    ContentArgs,
    ResolvedDataset,
    UpdateVersionedFileUseCase,
    WriteCheckedDataset,
};
use kamu_molecule_domain::{
    MoleculeEncryptionMetadata,
    MoleculeUploadVersionedFileVersionError,
    MoleculeUploadVersionedFileVersionUseCase,
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryDetailedInfo,
    MoleculeVersionedFileExtraData,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeUploadVersionedFileVersionUseCase)]
pub struct MoleculeUploadVersionedFileVersionUseCaseImpl {
    update_versioned_file_uc: Arc<dyn UpdateVersionedFileUseCase>,
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
    ) -> Result<MoleculeVersionedFileEntry, MoleculeUploadVersionedFileVersionError> {
        let content_type = content_args.content_type.clone();
        let content_length = content_args.content_length;

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
                Some(content_args),
                None,
                Some(versioned_file_extra_data.to_extra_data_fields()),
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
            basic_info: versioned_file_basic_info,
            detailed_info: versioned_file_detailed_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
