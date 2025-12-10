// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use file_utils::MediaType;
use kamu::domain;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::ContentArgs;
use kamu_molecule_domain::{
    MoleculeAppendDataRoomActivityError,
    MoleculeAppendGlobalDataRoomActivityUseCase,
    MoleculeCreateDataRoomEntryError,
    MoleculeCreateDataRoomEntryUseCase,
    MoleculeCreateVersionedFileDatasetError,
    MoleculeCreateVersionedFileDatasetUseCase,
    MoleculeDataRoomActivityEntity,
    MoleculeDataRoomFileActivityType,
    MoleculeMoveDataRoomEntryError,
    MoleculeMoveDataRoomEntryUseCase,
    MoleculeReadVersionedFileEntryError,
    MoleculeReadVersionedFileEntryUseCase,
    MoleculeRemoveDataRoomEntryError,
    MoleculeRemoveDataRoomEntryUseCase,
    MoleculeUpdateDataRoomEntryError,
    MoleculeUpdateDataRoomEntryResult,
    MoleculeUpdateDataRoomEntryUseCase,
    MoleculeUpdateVersionedFileMetadataError,
    MoleculeUpdateVersionedFileMetadataUseCase,
    MoleculeUploadVersionedFileVersionError,
    MoleculeUploadVersionedFileVersionUseCase,
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryDetailedInfo,
};
use time_source::SystemTimeSource;

use crate::molecule::molecule_subject;
use crate::mutations::{
    StartUploadVersionErrorTooLarge,
    StartUploadVersionResult,
    StartUploadVersionSuccess,
    UpdateVersionErrorCasFailed,
    UpdateVersionErrorInvalidExtraData,
    map_get_content_args_error,
};
use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeAccessLevel,
    MoleculeCategory,
    MoleculeDataRoomEntry,
    MoleculeEncryptionMetadataInput,
    MoleculeProjectV2,
    MoleculeTag,
};
use crate::utils::{ContentSource, get_content_args};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomMutV2 {
    project: Arc<MoleculeProjectV2>,
}

impl MoleculeDataRoomMutV2 {
    pub fn new(project: Arc<MoleculeProjectV2>) -> Self {
        Self { project }
    }

    // TODO: Specialize error handling
    async fn finish_upload_file_new_file(
        &self,
        ctx: &Context<'_>,
        content_args: ContentArgs,
        path: kamu_datasets::CollectionPathV2,
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<MoleculeCategory>>,
        tags: Option<Vec<MoleculeTag>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadataInput>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let (
            time_source,
            create_versioned_file_dataset_uc,
            upload_versioned_file_version_uc,
            create_data_room_entry_uc,
            append_global_data_room_activity_uc,
        ) = from_catalog_n!(
            ctx,
            dyn SystemTimeSource,
            dyn MoleculeCreateVersionedFileDatasetUseCase,
            dyn MoleculeUploadVersionedFileVersionUseCase,
            dyn MoleculeCreateDataRoomEntryUseCase,
            dyn MoleculeAppendGlobalDataRoomActivityUseCase
        );

        let event_time = time_source.now();

        // 1. Create an empty versioned dataset.
        let versioned_file_dataset_id = create_versioned_file_dataset_uc
            .execute(&molecule_subject, &self.project.entity, path.clone())
            .await
            .map_err(|e| {
                use MoleculeCreateVersionedFileDatasetError as E;

                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        // 2. Upload the first version to just created dataset.
        // NOTE: Version and content hash get updated to correct values below
        let versioned_file_entry = upload_versioned_file_version_uc
            .execute(
                &versioned_file_dataset_id,
                Some(event_time),
                content_args,
                MoleculeVersionedFileEntryBasicInfo {
                    access_level,
                    change_by,
                    description,
                    categories: categories.unwrap_or_default(),
                    tags: tags.unwrap_or_default(),
                },
                MoleculeVersionedFileEntryDetailedInfo {
                    content_text,
                    encryption_metadata: encryption_metadata.map(Into::into),
                },
            )
            .await
            .map_err(|e| {
                use MoleculeUploadVersionedFileVersionError as E;

                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        // 3. Add the file to the data room.

        let data_room_entry = create_data_room_entry_uc
            .execute(
                &molecule_subject,
                &self.project.entity,
                Some(event_time),
                path.clone().into_v1(),
                versioned_file_dataset_id.clone(),
                versioned_file_entry.to_denormalized(),
            )
            .await
            .map_err(|e| match e {
                MoleculeCreateDataRoomEntryError::Access(e) => e.into(),
                MoleculeCreateDataRoomEntryError::RefCASFailed(_) => {
                    GqlError::gql("Data room linking: CAS failed")
                }
                e @ MoleculeCreateDataRoomEntryError::Internal(_) => e.int_err().into(),
            })?;

        // 4. Log the activity.
        // TODO: asynchronous write of activity log
        {
            let data_room_activity = MoleculeDataRoomActivityEntity {
                system_time: event_time, // TODO: take from ingest result
                event_time,
                activity_type: MoleculeDataRoomFileActivityType::Added,
                ipnft_uid: self.project.entity.ipnft_uid.clone(),
                path: path.into_v1(),
                r#ref: versioned_file_dataset_id,
                version: versioned_file_entry.version,
                change_by: versioned_file_entry.basic_info.change_by,
                access_level: versioned_file_entry.basic_info.access_level,
                content_type: Some(versioned_file_entry.content_type),
                content_length: versioned_file_entry.content_length,
                content_hash: versioned_file_entry.content_hash,
                description: versioned_file_entry.basic_info.description,
                categories: versioned_file_entry.basic_info.categories,
                tags: versioned_file_entry.basic_info.tags,
            };

            append_global_data_room_activity_uc
                .execute(&molecule_subject, Some(event_time), data_room_activity)
                .await
                .map_err(|e| -> GqlError {
                    use MoleculeAppendDataRoomActivityError as E;

                    match e {
                        E::Access(e) => e.into(),
                        e @ E::Internal(_) => e.int_err().into(),
                    }
                })?;
        }

        Ok(MoleculeDataRoomFinishUploadFileResultSuccess {
            entry: MoleculeDataRoomEntry::new_from_data_room_entry(&self.project, data_room_entry),
        }
        .into())
    }

    // TODO: Specialize error handling
    async fn finish_upload_file_new_file_version(
        &self,
        ctx: &Context<'_>,
        content_args: ContentArgs,
        reference: DatasetID<'static>,
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<MoleculeCategory>>,
        tags: Option<Vec<MoleculeTag>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadataInput>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let (
            time_source,
            upload_versioned_file_version_uc,
            update_data_room_entry_uc,
            append_global_data_room_activity_uc,
        ) = from_catalog_n!(
            ctx,
            dyn SystemTimeSource,
            dyn MoleculeUploadVersionedFileVersionUseCase,
            dyn MoleculeUpdateDataRoomEntryUseCase,
            dyn MoleculeAppendGlobalDataRoomActivityUseCase
        );

        let event_time = time_source.now();

        // 1. Get the existing versioned dataset entry -- we need to know `path`;
        let Some(existing_data_room_entry) = self
            .get_latest_data_room_entry(ctx, reference.as_ref())
            .await?
        else {
            todo!();
        };

        // 2. Upload the next version to the specified dataset.

        // NOTE: Access rights will be checked inside the use case.
        let versioned_file_entry = upload_versioned_file_version_uc
            .execute(
                &existing_data_room_entry.reference,
                Some(event_time),
                content_args,
                MoleculeVersionedFileEntryBasicInfo {
                    access_level,
                    change_by,
                    description,
                    categories: categories.unwrap_or_default(),
                    tags: tags.unwrap_or_default(),
                },
                MoleculeVersionedFileEntryDetailedInfo {
                    content_text,
                    encryption_metadata: encryption_metadata.map(Into::into),
                },
            )
            .await
            .int_err()?;

        // 3. Update the file state in the data room.

        let updated_data_room_entry = update_data_room_entry_uc
            .execute(
                &molecule_subject,
                &self.project.entity,
                Some(event_time),
                existing_data_room_entry.path.clone(),
                existing_data_room_entry.reference.clone(),
                versioned_file_entry.to_denormalized(),
            )
            .await
            .map_err(|e| match e {
                MoleculeUpdateDataRoomEntryError::Access(e) => e.into(),
                MoleculeUpdateDataRoomEntryError::RefCASFailed(_) => {
                    GqlError::gql("Data room linking: CAS failed")
                }
                e @ MoleculeUpdateDataRoomEntryError::Internal(_) => e.int_err().into(),
            })?;

        // 4. Log the activity.
        // TODO: asynchronous write of activity log
        {
            let data_room_activity = MoleculeDataRoomActivityEntity {
                system_time: event_time, // TODO: take from ingest result
                event_time,
                activity_type: MoleculeDataRoomFileActivityType::Updated,
                ipnft_uid: self.project.entity.ipnft_uid.clone(),
                path: updated_data_room_entry.path.clone(),
                r#ref: existing_data_room_entry.reference,
                version: versioned_file_entry.version,
                change_by: versioned_file_entry.basic_info.change_by,
                access_level: versioned_file_entry.basic_info.access_level,
                content_type: Some(versioned_file_entry.content_type),
                content_length: versioned_file_entry.content_length,
                content_hash: versioned_file_entry.content_hash,
                description: versioned_file_entry.basic_info.description,
                categories: versioned_file_entry.basic_info.categories,
                tags: versioned_file_entry.basic_info.tags,
            };

            append_global_data_room_activity_uc
                .execute(&molecule_subject, Some(event_time), data_room_activity)
                .await
                .map_err(|e| -> GqlError {
                    use MoleculeAppendDataRoomActivityError as E;

                    match e {
                        E::Access(e) => e.into(),
                        e @ E::Internal(_) => e.int_err().into(),
                    }
                })?;
        }

        Ok(MoleculeDataRoomFinishUploadFileResultSuccess {
            entry: MoleculeDataRoomEntry::new_from_data_room_entry(
                &self.project,
                updated_data_room_entry,
            ),
        }
        .into())
    }

    async fn get_latest_data_room_entry(
        &self,
        ctx: &Context<'_>,
        reference: &odf::DatasetID,
    ) -> Result<Option<kamu_molecule_domain::MoleculeDataRoomEntry>> {
        let find_data_room_entry_uc = from_catalog_n!(
            ctx,
            dyn kamu_molecule_domain::MoleculeFindDataRoomEntryUseCase
        );

        let maybe_data_room_entry = find_data_room_entry_uc
            .execute_find_by_ref(&self.project.entity, None /* latest */, reference)
            .await
            .map_err(|e| -> GqlError {
                use kamu_molecule_domain::MoleculeFindDataRoomEntryError as E;
                match e {
                    E::Access(e) => e.into(),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        Ok(maybe_data_room_entry)
    }

    async fn append_global_data_room_activity(
        &self,
        ctx: &Context<'_>,
        event_time: DateTime<Utc>,
        inserted_records: Vec<(
            odf::metadata::OperationType,
            kamu_datasets::CollectionEntryRecord,
        )>,
        activity_type: MoleculeDataRoomFileActivityType,
    ) -> Result<()> {
        match activity_type {
            // Update happens in two records
            MoleculeDataRoomFileActivityType::Updated if inserted_records.len() == 2 => {}
            MoleculeDataRoomFileActivityType::Removed if inserted_records.len() == 1 => {}
            _ => unreachable!(),
        }

        let molecule_subject = molecule_subject(ctx)?;

        let append_global_data_room_activity_uc =
            from_catalog_n!(ctx, dyn MoleculeAppendGlobalDataRoomActivityUseCase);

        let (_op, collection_entry_record) = inserted_records.into_iter().next_back().unwrap();

        let denormalized_latest_file_info =
            kamu_molecule_domain::MoleculeDenormalizeFileToDataRoom::try_from_extra_data_fields(
                collection_entry_record.extra_data,
            )
            .int_err()?;

        let data_room_activity = MoleculeDataRoomActivityEntity {
            system_time: event_time, // TODO: take from ingest result
            event_time,
            activity_type,
            ipnft_uid: self.project.entity.ipnft_uid.clone(),
            path: collection_entry_record.path,
            r#ref: collection_entry_record.reference,
            version: denormalized_latest_file_info.version,
            change_by: denormalized_latest_file_info.change_by,
            access_level: denormalized_latest_file_info.access_level,
            content_type: {
                let s = denormalized_latest_file_info.content_type;
                if !s.0.is_empty() { Some(s) } else { None }
            },
            content_length: denormalized_latest_file_info.content_length,
            content_hash: denormalized_latest_file_info.content_hash,
            description: denormalized_latest_file_info.description,
            categories: denormalized_latest_file_info.categories,
            tags: denormalized_latest_file_info.tags,
        };

        // TODO: asynchronous write of activity log
        append_global_data_room_activity_uc
            .execute(&molecule_subject, Some(event_time), data_room_activity)
            .await
            .map_err(|e| -> GqlError {
                use MoleculeAppendDataRoomActivityError as E;

                match e {
                    E::Access(e) => e.into(),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomMutV2 {
    /// Allows creating a file, upload a new version, and link a file into the
    /// data room via a single transaction. Uploads a new version of content
    /// in-band, so should be used only for very small files.
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_finish_upload_file, skip_all)]
    async fn upload_file(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Base64-encoded file content (url-safe, no padding)")] content: Base64Usnp,
        #[graphql(name = "ref")] reference: Option<DatasetID<'static>>,
        path: Option<CollectionPathV2<'static>>,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<MoleculeCategory>>,
        tags: Option<Vec<MoleculeTag>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadataInput>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
        let content_args = get_content_args(
            ctx,
            ContentSource::Bytes(&content),
            content_type.map(Into::into),
        )
        .await
        .map_err(map_get_content_args_error)?;

        match (path, reference) {
            (Some(path), None) => {
                self.finish_upload_file_new_file(
                    ctx,
                    content_args,
                    path.into(),
                    access_level,
                    change_by,
                    description,
                    categories,
                    tags,
                    content_text,
                    encryption_metadata,
                )
                .await
            }
            (None, Some(reference)) => {
                self.finish_upload_file_new_file_version(
                    ctx,
                    content_args,
                    reference,
                    access_level,
                    change_by,
                    description,
                    categories,
                    tags,
                    content_text,
                    encryption_metadata,
                )
                .await
            }
            _ => Err(GqlError::gql("Either `path` or `ref` must be specified")),
        }
    }

    /// Starts the process of uploading a file to the data room.
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_start_upload_file, skip_all)]
    async fn start_upload_file(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Size of the file being uploaded")] content_length: usize,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
    ) -> Result<StartUploadVersionResult> {
        let (subject, upload_svc, limits) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn domain::UploadService,
            domain::FileUploadLimitConfig
        );

        let upload_context = match upload_svc
            .make_upload_context(
                subject.account_id(),
                uuid::Uuid::new_v4().to_string(),
                content_type.map(MediaType),
                content_length,
            )
            .await
        {
            Ok(ctx) => ctx,
            Err(domain::MakeUploadContextError::TooLarge(_)) => {
                return Ok(StartUploadVersionResult::TooLarge(
                    StartUploadVersionErrorTooLarge {
                        upload_size: content_length,
                        upload_limit: limits.max_file_size_in_bytes(),
                    },
                ));
            }
            Err(e) => return Err(e.int_err().into()),
        };

        Ok(StartUploadVersionResult::Success(
            StartUploadVersionSuccess {
                upload_context: upload_context.into(),
            },
        ))
    }

    /// Allows creating a file, upload a new version, and link a file into the
    /// data room via a single transaction.
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_finish_upload_file, skip_all)]
    async fn finish_upload_file(
        &self,
        ctx: &Context<'_>,
        upload_token: String,
        #[graphql(name = "ref")] reference: Option<DatasetID<'static>>,
        path: Option<CollectionPathV2<'static>>,
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<MoleculeCategory>>,
        tags: Option<Vec<MoleculeTag>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadataInput>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
        // IMPORTANT: If after file creation or version update an error occurs,
        //            all DB will be cleared (transaction rollback). Dataset data
        //            (e.g., on S3) will need later cleanup (garbage collection).

        let content_args = get_content_args(ctx, ContentSource::Token(upload_token), None)
            .await
            .map_err(map_get_content_args_error)?;

        match (path, reference) {
            (Some(path), None) => {
                self.finish_upload_file_new_file(
                    ctx,
                    content_args,
                    path.into(),
                    access_level,
                    change_by,
                    description,
                    categories,
                    tags,
                    content_text,
                    encryption_metadata,
                )
                .await
            }
            (None, Some(reference)) => {
                self.finish_upload_file_new_file_version(
                    ctx,
                    content_args,
                    reference,
                    access_level,
                    change_by,
                    description,
                    categories,
                    tags,
                    content_text,
                    encryption_metadata,
                )
                .await
            }
            _ => Err(GqlError::gql("Either `path` or `ref` must be specified")),
        }
    }

    /// Moves an entry in the data room.
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_move_entry, skip_all)]
    async fn move_entry(
        &self,
        ctx: &Context<'_>,
        from_path: CollectionPathV2<'static>,
        to_path: CollectionPathV2<'static>,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<MoleculeDataRoomMoveEntryResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let (time_source, move_data_room_entry_uc) = from_catalog_n!(
            ctx,
            dyn SystemTimeSource,
            dyn MoleculeMoveDataRoomEntryUseCase
        );

        let event_time = time_source.now();

        match move_data_room_entry_uc
            .execute(
                &molecule_subject,
                &self.project.entity,
                Some(event_time),
                from_path.into_v1_scalar().into(),
                to_path.into_v1_scalar().into(),
                expected_head.map(Into::into),
            )
            .await
        {
            Ok(MoleculeUpdateDataRoomEntryResult::Success(r)) => {
                // TODO: asynchronous write of activity log
                self.append_global_data_room_activity(
                    ctx,
                    event_time,
                    r.inserted_records,
                    MoleculeDataRoomFileActivityType::Updated,
                )
                .await?;

                Ok(MoleculeDataRoomUpdateSuccess {
                    old_head: r.old_head.into(),
                    new_head: r.new_head.into(),
                }
                .into())
            }
            Ok(MoleculeUpdateDataRoomEntryResult::UpToDate) => {
                Ok(MoleculeDataRoomUpdateUpToDate.into())
            }
            Ok(MoleculeUpdateDataRoomEntryResult::EntryNotFound(path)) => {
                Ok(MoleculeDataRoomUpdateEntryNotFound { path: path.into() }.into())
            }
            Err(MoleculeMoveDataRoomEntryError::Access(e)) => Err(e.into()),
            Err(MoleculeMoveDataRoomEntryError::RefCASFailed(_)) => {
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Err(e @ MoleculeMoveDataRoomEntryError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Removes an entry from the data room.
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_remove_entry, skip_all)]
    async fn remove_entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPathV2<'static>,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<MoleculeDataRoomRemoveEntryResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let (time_sourcem, remove_data_room_entry_uc) = from_catalog_n!(
            ctx,
            dyn SystemTimeSource,
            dyn MoleculeRemoveDataRoomEntryUseCase
        );

        let event_time = time_sourcem.now();
        let path = path.into_v1_scalar();

        match remove_data_room_entry_uc
            .execute(
                &molecule_subject,
                &self.project.entity,
                Some(event_time),
                path.clone().into(),
                expected_head.map(Into::into),
            )
            .await
        {
            Ok(MoleculeUpdateDataRoomEntryResult::Success(r)) => {
                // TODO: asynchronous write of activity log
                self.append_global_data_room_activity(
                    ctx,
                    event_time,
                    r.inserted_records,
                    MoleculeDataRoomFileActivityType::Removed,
                )
                .await?;

                Ok(MoleculeDataRoomUpdateSuccess {
                    old_head: r.old_head.into(),
                    new_head: r.new_head.into(),
                }
                .into())
            }
            Ok(MoleculeUpdateDataRoomEntryResult::UpToDate) => {
                Ok(MoleculeDataRoomUpdateEntryNotFound { path }.into())
            }
            Ok(MoleculeUpdateDataRoomEntryResult::EntryNotFound(_)) => {
                unreachable!(
                    "Removals are idempotent, so UpToDate is returned instead of EntryNotFound"
                )
            }
            Err(MoleculeRemoveDataRoomEntryError::Access(e)) => Err(e.into()),
            Err(MoleculeRemoveDataRoomEntryError::RefCASFailed(_)) => {
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Err(e @ MoleculeRemoveDataRoomEntryError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Updates the metadata of a file in the data room.
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_update_file_metadata, skip_all)]
    async fn update_file_metadata(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "ref")] reference: DatasetID<'static>,
        // TODO: not optional?
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<String>>,
        tags: Option<Vec<String>>,
        content_text: Option<String>,
        encryption_metadata: Option<MoleculeEncryptionMetadataInput>,
    ) -> Result<MoleculeDataRoomUpdateFileMetadataResult> {
        let molecule_subject = molecule_subject(ctx)?;

        let (
            time_source,
            read_versioned_file_entry_uc,
            update_versioned_file_metadata_uc,
            update_data_room_entry_uc,
            append_global_data_room_activity_uc,
        ) = from_catalog_n!(
            ctx,
            dyn SystemTimeSource,
            dyn MoleculeReadVersionedFileEntryUseCase,
            dyn MoleculeUpdateVersionedFileMetadataUseCase,
            dyn MoleculeUpdateDataRoomEntryUseCase,
            dyn MoleculeAppendGlobalDataRoomActivityUseCase
        );

        let event_time = time_source.now();

        // 0. Get existing data room entry record and latest versioned file data

        let Some(existing_data_room_entry) = self
            .get_latest_data_room_entry(ctx, reference.as_ref())
            .await?
        else {
            return Ok(MoleculeDataRoomUpdateFileMetadataResult::EntryNotFound(
                MoleculeDataRoomUpdateFileMetadataResultEntryNotFound { r#ref: reference },
            ));
        };

        let Some(existing_file_entry) = read_versioned_file_entry_uc
            .execute(reference.as_ref(), None, None)
            .await
            .map_err(|e| {
                use MoleculeReadVersionedFileEntryError as E;

                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?
        else {
            return Ok(MoleculeDataRoomUpdateFileMetadataResult::FileNotFound(
                MoleculeDataRoomUpdateFileMetadataResultFileNotFound { r#ref: reference },
            ));
        };

        // 1. Update the versioned dataset.
        let updated_versioned_file_entry = update_versioned_file_metadata_uc
            .execute(
                reference.as_ref(),
                existing_file_entry,
                Some(event_time),
                MoleculeVersionedFileEntryBasicInfo {
                    access_level,
                    change_by,
                    description,
                    categories: categories.unwrap_or_default(),
                    tags: tags.unwrap_or_default(),
                },
                MoleculeVersionedFileEntryDetailedInfo {
                    content_text,
                    encryption_metadata: encryption_metadata.map(Into::into),
                },
            )
            .await
            .map_err(|e| {
                use MoleculeUpdateVersionedFileMetadataError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        let new_denormalized_file_info = updated_versioned_file_entry.to_denormalized();

        // 2. Update the file state in the data room.

        let updated_data_room_entry = update_data_room_entry_uc
            .execute(
                &molecule_subject,
                &self.project.entity,
                Some(event_time),
                existing_data_room_entry.path.clone(),
                reference.into(),
                new_denormalized_file_info.clone(),
            )
            .await
            .map_err(|e| match e {
                MoleculeUpdateDataRoomEntryError::Access(e) => e.into(),
                MoleculeUpdateDataRoomEntryError::RefCASFailed(_) => {
                    GqlError::gql("Data room linking: CAS failed")
                }
                e @ MoleculeUpdateDataRoomEntryError::Internal(_) => e.int_err().into(),
            })?;

        // 3. Log the activity.
        // TODO: asynchronous write of activity log
        {
            let data_room_activity = MoleculeDataRoomActivityEntity {
                system_time: event_time, // TODO: take from ingest result
                event_time,
                activity_type: MoleculeDataRoomFileActivityType::Updated,
                ipnft_uid: self.project.entity.ipnft_uid.clone(),
                path: updated_data_room_entry.path.clone(),
                r#ref: updated_data_room_entry.reference.clone(),
                version: updated_versioned_file_entry.version,
                change_by: new_denormalized_file_info.change_by,
                access_level: new_denormalized_file_info.access_level,
                content_type: Some(new_denormalized_file_info.content_type),
                content_length: new_denormalized_file_info.content_length,
                content_hash: new_denormalized_file_info.content_hash,
                description: new_denormalized_file_info.description,
                categories: new_denormalized_file_info.categories,
                tags: new_denormalized_file_info.tags,
            };

            append_global_data_room_activity_uc
                .execute(&molecule_subject, Some(event_time), data_room_activity)
                .await
                .map_err(|e| -> GqlError {
                    use MoleculeAppendDataRoomActivityError as E;

                    match e {
                        E::Access(e) => e.into(),
                        e @ E::Internal(_) => e.int_err().into(),
                    }
                })?;
        }

        Ok(MoleculeDataRoomUpdateFileMetadataResultSuccess {
            entry: MoleculeDataRoomEntry::new_from_data_room_entry(
                &self.project,
                updated_data_room_entry,
            ),
        }
        .into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Should be a mix from UpdateVersionResult & CollectionUpdateResult?
#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum MoleculeDataRoomFinishUploadFileResult {
    Success(MoleculeDataRoomFinishUploadFileResultSuccess),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeDataRoomFinishUploadFileResultSuccess {
    pub entry: MoleculeDataRoomEntry,
}

#[ComplexObject]
impl MoleculeDataRoomFinishUploadFileResultSuccess {
    pub async fn is_success(&self) -> bool {
        true
    }

    pub async fn message(&self) -> String {
        String::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum MoleculeDataRoomMoveEntryResult {
    Success(MoleculeDataRoomUpdateSuccess),
    EntryNotFound(MoleculeDataRoomUpdateEntryNotFound),
    UpToDate(MoleculeDataRoomUpdateUpToDate),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeDataRoomUpdateSuccess {
    pub old_head: Multihash<'static>,
    pub new_head: Multihash<'static>,
}

#[ComplexObject]
impl MoleculeDataRoomUpdateSuccess {
    pub async fn is_success(&self) -> bool {
        true
    }

    pub async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeDataRoomUpdateEntryNotFound {
    pub path: CollectionPath<'static>,
}
#[ComplexObject]
impl MoleculeDataRoomUpdateEntryNotFound {
    pub async fn is_success(&self) -> bool {
        false
    }

    pub async fn message(&self) -> String {
        "Data room entry not found".into()
    }
}

pub struct MoleculeDataRoomUpdateUpToDate;

#[Object]
impl MoleculeDataRoomUpdateUpToDate {
    async fn is_success(&self) -> bool {
        true
    }
    async fn message(&self) -> String {
        String::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum MoleculeDataRoomRemoveEntryResult {
    Success(MoleculeDataRoomUpdateSuccess),
    EntryNotFound(MoleculeDataRoomUpdateEntryNotFound),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Should be a mix from UpdateVersionResult & CollectionUpdateResult?
#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum MoleculeDataRoomUpdateFileMetadataResult {
    Success(MoleculeDataRoomUpdateFileMetadataResultSuccess),
    EntryNotFound(MoleculeDataRoomUpdateFileMetadataResultEntryNotFound),
    FileNotFound(MoleculeDataRoomUpdateFileMetadataResultFileNotFound),
    CasFailed(UpdateVersionErrorCasFailed),
    InvalidExtraData(UpdateVersionErrorInvalidExtraData),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeDataRoomUpdateFileMetadataResultSuccess {
    pub entry: MoleculeDataRoomEntry,
}

#[ComplexObject]
impl MoleculeDataRoomUpdateFileMetadataResultSuccess {
    pub async fn is_success(&self) -> bool {
        true
    }

    pub async fn message(&self) -> String {
        String::new()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeDataRoomUpdateFileMetadataResultEntryNotFound {
    pub r#ref: DatasetID<'static>,
}
#[ComplexObject]
impl MoleculeDataRoomUpdateFileMetadataResultEntryNotFound {
    pub async fn is_success(&self) -> bool {
        false
    }

    pub async fn message(&self) -> String {
        "Data room entry not found".to_string()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeDataRoomUpdateFileMetadataResultFileNotFound {
    pub r#ref: DatasetID<'static>,
}
#[ComplexObject]
impl MoleculeDataRoomUpdateFileMetadataResultFileNotFound {
    pub async fn is_success(&self) -> bool {
        false
    }

    pub async fn message(&self) -> String {
        "File dataset not found".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
