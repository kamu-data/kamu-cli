// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::logical_expr::{col, lit};
use file_utils::MediaType;
use kamu::domain;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{
    CollectionUpdateOperation,
    ContentArgs,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCaseOptions,
    DatasetRegistry,
    DatasetRegistryExt,
    ExtraDataFields,
    ResolvedDataset,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
    UpdateVersionedFileUseCase,
    WriteCheckedDataset,
};
use kamu_molecule_domain::MoleculeDatasetSnapshots;
use odf::utils::data::DataFrameExt;

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
    EncryptionMetadata,
    MoleculeAccessLevel,
    MoleculeCategory,
    MoleculeDataRoomEntry,
    MoleculeProjectV2,
    MoleculeTag,
    MoleculeVersionedFileEntry,
    MoleculeVersionedFileEntryBasicInfo,
    MoleculeVersionedFileEntryDetailedInfo,
    MoleculeVersionedFilePrefetch,
};
use crate::queries::{Account, CollectionEntry, DatasetRequestState, VersionedFileEntry};
use crate::utils::{ContentSource, get_content_args};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomMutV2 {
    project: Arc<MoleculeProjectV2>,
    data_room_writable_state: DatasetRequestState,
}

impl MoleculeDataRoomMutV2 {
    pub fn new(
        project: Arc<MoleculeProjectV2>,
        data_room_writable_state: DatasetRequestState,
    ) -> Self {
        Self {
            project,
            data_room_writable_state,
        }
    }

    // TODO: Specialize error handling
    async fn finish_upload_file_new_file(
        &self,
        ctx: &Context<'_>,
        content_args: ContentArgs,
        path: CollectionPath<'static>,
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<MoleculeCategory>>,
        tags: Option<Vec<MoleculeTag>>,
        content_text: Option<String>,
        encryption_metadata: Option<EncryptionMetadata>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
        // TODO: Align timestamps with ingest
        let now = chrono::Utc::now();

        let (create_dataset_from_snapshot, update_versioned_file, update_collection_entries) = from_catalog_n!(
            ctx,
            dyn CreateDatasetFromSnapshotUseCase,
            dyn UpdateVersionedFileUseCase,
            dyn UpdateCollectionEntriesUseCase
        );

        // 1. Create an empty versioned dataset.
        let alias = self.build_new_file_dataset_alias(ctx, &path).await;
        let versioned_file_snapshot = MoleculeDatasetSnapshots::versioned_file_v2(alias);

        let versioned_file_dataset = ResolvedDataset::from_created(
            &create_dataset_from_snapshot
                .execute(
                    versioned_file_snapshot,
                    CreateDatasetUseCaseOptions::default(),
                )
                .await
                .int_err()?,
        );

        // 2. Upload the first version to just created dataset.
        // NOTE: Version and content hash get updated to correct values below
        let mut versioned_file_entry = MoleculeVersionedFileEntry {
            entry: VersionedFileEntry {
                file_dataset: versioned_file_dataset.clone(),
                entity: kamu_datasets::VersionedFileEntry {
                    system_time: now,
                    event_time: now,
                    version: 0,
                    content_type: content_args
                        .content_type
                        .as_ref()
                        .map(|ct| ct.0.clone())
                        .unwrap_or_default(),
                    content_length: content_args.content_length,
                    content_hash: odf::Multihash::from_digest_sha3_256(b""),
                    extra_data: kamu_datasets::ExtraDataFields::default(),
                },
            },
            basic_info: MoleculeVersionedFileEntryBasicInfo {
                access_level,
                change_by,
                description,
                categories: categories.unwrap_or_default(),
                tags: tags.unwrap_or_default(),
            },
            detailed_info: tokio::sync::OnceCell::new_with(Some(
                MoleculeVersionedFileEntryDetailedInfo {
                    content_text,
                    encryption_metadata,
                },
            )),
        };

        let update_version_result = update_versioned_file
            .execute(
                WriteCheckedDataset(&versioned_file_dataset),
                Some(content_args),
                None,
                Some(versioned_file_entry.to_versioned_file_extra_data()),
            )
            .await
            .int_err()?;

        versioned_file_entry.entry.entity.version = update_version_result.new_version;
        versioned_file_entry.entry.entity.content_hash = update_version_result.content_hash;

        // 3. Add the file to the data room.

        let data_room_entry = MoleculeDataRoomEntry {
            entry: CollectionEntry {
                entity: kamu_datasets::CollectionEntry {
                    system_time: now,
                    event_time: now,
                    path: path.into(),
                    reference: versioned_file_dataset.get_id().clone(),
                    extra_data: kamu_datasets::ExtraDataFields::default(),
                },
            },
            project: self.project.clone(),
            denormalized_latest_file_info: versioned_file_entry.to_denormalized(),
        };

        let data_room_writable_dataset =
            self.data_room_writable_state.resolved_dataset(ctx).await?;

        match update_collection_entries
            .execute(
                WriteCheckedDataset(data_room_writable_dataset),
                vec![CollectionUpdateOperation::add(
                    data_room_entry.entry.entity.path.clone(),
                    data_room_entry.entry.entity.reference.clone(),
                    ExtraDataFields::new(data_room_entry.to_collection_extra_data().into()),
                )],
                None,
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(_)) => {
                Ok(MoleculeDataRoomFinishUploadFileResultSuccess {
                    entry: data_room_entry,
                }
                .into())
            }
            Ok(
                UpdateCollectionEntriesResult::UpToDate
                | UpdateCollectionEntriesResult::NotFound(_),
            ) => {
                unreachable!()
            }
            Err(UpdateCollectionEntriesUseCaseError::Access(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(_)) => {
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Err(e @ UpdateCollectionEntriesUseCaseError::Internal(_)) => Err(e.int_err().into()),
        }
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
        encryption_metadata: Option<EncryptionMetadata>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
        // TODO: Align timestamps with ingest
        let now = chrono::Utc::now();

        let (update_versioned_file, dataset_registry, update_collection_entries) = from_catalog_n!(
            ctx,
            dyn UpdateVersionedFileUseCase,
            dyn DatasetRegistry,
            dyn UpdateCollectionEntriesUseCase
        );

        // 1. Get the existing versioned dataset entry -- we need to know `path`;
        let Some(collection_entry) = self.get_data_room_entry(ctx, reference.as_ref()).await?
        else {
            todo!();
        };

        // 2. Upload the next version to the specified dataset.

        // NOTE: Access rights will be checked inside the use case.
        let file_dataset = dataset_registry
            .get_dataset_by_ref(&reference.as_ref().as_local_ref())
            .await
            .int_err()?;

        // NOTE: Version and content hash get updated to correct values below
        let mut versioned_file_entry = MoleculeVersionedFileEntry {
            entry: VersionedFileEntry {
                file_dataset: file_dataset.clone(),
                entity: kamu_datasets::VersionedFileEntry {
                    system_time: now,
                    event_time: now,
                    version: 0,
                    content_type: content_args
                        .content_type
                        .as_ref()
                        .map(|ct| ct.0.clone())
                        .unwrap_or_default(),
                    content_length: content_args.content_length,
                    content_hash: odf::Multihash::from_digest_sha3_256(b""),
                    extra_data: kamu_datasets::ExtraDataFields::default(),
                },
            },
            basic_info: MoleculeVersionedFileEntryBasicInfo {
                access_level,
                change_by,
                description,
                categories: categories.unwrap_or_default(),
                tags: tags.unwrap_or_default(),
            },
            detailed_info: tokio::sync::OnceCell::new_with(Some(
                MoleculeVersionedFileEntryDetailedInfo {
                    content_text,
                    encryption_metadata,
                },
            )),
        };

        let update_version_result = update_versioned_file
            .execute(
                WriteCheckedDataset(&file_dataset),
                Some(content_args),
                None,
                Some(versioned_file_entry.to_versioned_file_extra_data()),
            )
            .await
            .int_err()?;

        versioned_file_entry.entry.entity.version = update_version_result.new_version;
        versioned_file_entry.entry.entity.content_hash = update_version_result.content_hash;
        // 3. Update the file state in the data room.

        let data_room_entry = MoleculeDataRoomEntry {
            entry: collection_entry,
            project: self.project.clone(),
            denormalized_latest_file_info: versioned_file_entry.to_denormalized(),
        };

        let writable_data_room_dataset =
            self.data_room_writable_state.resolved_dataset(ctx).await?;

        match update_collection_entries
            .execute(
                WriteCheckedDataset(writable_data_room_dataset),
                vec![CollectionUpdateOperation::add(
                    data_room_entry.entry.entity.path.clone(),
                    data_room_entry.entry.entity.reference.clone(),
                    ExtraDataFields::new(data_room_entry.to_collection_extra_data().into()),
                )],
                None,
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(_)) => {
                Ok(MoleculeDataRoomFinishUploadFileResultSuccess {
                    entry: data_room_entry,
                }
                .into())
            }
            Ok(
                UpdateCollectionEntriesResult::UpToDate
                | UpdateCollectionEntriesResult::NotFound(_),
            ) => {
                unreachable!()
            }
            Err(UpdateCollectionEntriesUseCaseError::Access(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(_)) => {
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Err(e @ UpdateCollectionEntriesUseCaseError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    async fn get_data_room_projection(
        &self,
        ctx: &Context<'_>,
    ) -> Result<(ResolvedDataset, Option<DataFrameExt>)> {
        let query_service = from_catalog_n!(ctx, dyn domain::QueryService);

        let resolved_dataset = self.data_room_writable_state.resolved_dataset(ctx).await?;

        let res = query_service
            .get_changelog_projection(
                resolved_dataset.clone(),
                domain::GetChangelogProjectionOptions {
                    block_hash: None,
                    // TODO: Maybe we don't need hints here. Added for performance reasons.
                    hints: domain::ChangelogProjectionHints {
                        // TODO: Extract "path" to constant
                        primary_key: Some(vec!["path".to_string()]),
                        dataset_vocabulary: Some(odf::metadata::DatasetVocabulary::default()),
                    },
                },
            )
            .await
            .map_err(|e| -> GqlError {
                use domain::QueryError as E;
                match e {
                    E::Access(e) => e.into(),
                    _ => e.int_err().into(),
                }
            })?;

        Ok((res.source, res.df))
    }

    // TODO: return typed collection entry
    async fn get_data_room_entry(
        &self,
        ctx: &Context<'_>,
        reference: &odf::DatasetID,
    ) -> Result<Option<CollectionEntry>> {
        // ) -> Result<Option<MoleculeDataRoomEntry>> {
        let (_, maybe_projection_df) = self.get_data_room_projection(ctx).await?;

        let Some(df) = maybe_projection_df else {
            return Ok(None);
        };

        let df = df
            .filter(col("ref").eq(lit(reference.to_string())))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);

        let entity =
            kamu_datasets::CollectionEntry::from_json(records.into_iter().next().unwrap())?;
        let entry = CollectionEntry::new(entity);

        Ok(Some(entry))
    }

    // TODO: Test with different paths
    async fn build_new_file_dataset_alias(
        &self,
        ctx: &Context<'_>,
        file_path: &CollectionPath<'static>,
    ) -> odf::DatasetAlias {
        // TODO: PERF: Add AccountRequestState similar to DatasetRequestState and reuse
        //             possibly resolved account?
        let project_account = Account::from_account_id(ctx, self.project.entity.account_id.clone())
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to load project account [{}]: {e}",
                    self.project.entity.account_id
                )
            });
        let project_account_name = project_account.account_name_internal().clone();

        let new_file_name = {
            use std::borrow::Borrow;

            // NOTE: We assume that `file_path` has already validated via `CollectionPath`
            //       scalar.
            let file_path_as_str: &String = file_path.borrow();
            let filename_encoded = file_path_as_str.rsplit('/').next().unwrap();
            odf::DatasetName::new_unchecked(filename_encoded)
        };

        odf::DatasetAlias::new(Some(project_account_name), new_file_name)
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
        path: Option<CollectionPath<'static>>,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<MoleculeCategory>>,
        tags: Option<Vec<MoleculeTag>>,
        content_text: Option<String>,
        encryption_metadata: Option<EncryptionMetadata>,
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
                    path,
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
            _ => return Err(GqlError::gql("Either `path` or `ref` must be specified")),
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
        path: Option<CollectionPath<'static>>,
        access_level: MoleculeAccessLevel,
        change_by: String,
        description: Option<String>,
        categories: Option<Vec<MoleculeCategory>>,
        tags: Option<Vec<MoleculeTag>>,
        content_text: Option<String>,
        encryption_metadata: Option<EncryptionMetadata>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
        // Warning: here be dragons.

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
                    path,
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
        from_path: CollectionPath<'static>,
        to_path: CollectionPath<'static>,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<MoleculeDataRoomMoveEntryResult> {
        let update_collection_entries = from_catalog_n!(ctx, dyn UpdateCollectionEntriesUseCase);

        let data_room_writable_dataset =
            self.data_room_writable_state.resolved_dataset(ctx).await?;

        match update_collection_entries
            .execute(
                WriteCheckedDataset(data_room_writable_dataset),
                vec![CollectionUpdateOperation::r#move(
                    from_path.clone().into(),
                    to_path.into(),
                    None,
                )],
                expected_head.map(Into::into),
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(r)) => Ok(MoleculeDataRoomUpdateSuccess {
                old_head: r.old_head.into(),
                new_head: r.new_head.into(),
            }
            .into()),
            Ok(UpdateCollectionEntriesResult::UpToDate) => {
                Ok(MoleculeDataRoomUpdateUpToDate.into())
            }
            Ok(UpdateCollectionEntriesResult::NotFound(_)) => {
                Ok(MoleculeDataRoomUpdateEntryNotFound { path: from_path }.into())
            }
            Err(UpdateCollectionEntriesUseCaseError::Access(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(_)) => {
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Err(e @ UpdateCollectionEntriesUseCaseError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Removes an entry from the data room.
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_remove_entry, skip_all)]
    async fn remove_entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPath<'static>,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<MoleculeDataRoomRemoveEntryResult> {
        let update_collection_entries = from_catalog_n!(ctx, dyn UpdateCollectionEntriesUseCase);

        let data_room_writable_dataset =
            self.data_room_writable_state.resolved_dataset(ctx).await?;

        match update_collection_entries
            .execute(
                WriteCheckedDataset(data_room_writable_dataset),
                vec![CollectionUpdateOperation::remove(path.clone().into())],
                expected_head.map(Into::into),
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(r)) => Ok(MoleculeDataRoomUpdateSuccess {
                old_head: r.old_head.into(),
                new_head: r.new_head.into(),
            }
            .into()),
            Ok(UpdateCollectionEntriesResult::UpToDate) => {
                // No action was performed - there was nothing to act upon
                Ok(MoleculeDataRoomUpdateEntryNotFound { path }.into())
            }
            Ok(UpdateCollectionEntriesResult::NotFound(_)) => {
                // For moving operation
                unreachable!()
            }
            Err(UpdateCollectionEntriesUseCaseError::Access(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(_)) => {
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Err(e @ UpdateCollectionEntriesUseCaseError::Internal(_)) => Err(e.int_err().into()),
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
        encryption_metadata: Option<EncryptionMetadata>,
    ) -> Result<MoleculeDataRoomUpdateFileMetadataResult> {
        let (update_versioned_file, dataset_registry, update_collection_entries) = from_catalog_n!(
            ctx,
            dyn UpdateVersionedFileUseCase,
            dyn DatasetRegistry,
            dyn UpdateCollectionEntriesUseCase
        );

        // NOTE: Access rights will be checked inside the use case.
        let file_dataset = {
            use odf::DatasetRefUnresolvedError as E;
            match dataset_registry
                .get_dataset_by_ref(&reference.as_ref().as_local_ref())
                .await
            {
                // TODO: Check if the versioned dataset is in data room at all?
                Ok(hdl) => hdl,
                Err(E::NotFound(_)) => {
                    return Ok(MoleculeDataRoomUpdateFileMetadataResult::EntryNotFound(
                        MoleculeDataRoomUpdateFileMetadataResultEntryNotFound { r#ref: reference },
                    ));
                }
                Err(e @ E::Internal(_)) => return Err(e.int_err().into()),
            }
        };

        let Some(collection_entry) = self.get_data_room_entry(ctx, reference.as_ref()).await?
        else {
            // TODO: Should we differentiate between 'file not found'
            //       and 'file not linked to data room'?
            return Ok(MoleculeDataRoomUpdateFileMetadataResult::EntryNotFound(
                MoleculeDataRoomUpdateFileMetadataResultEntryNotFound { r#ref: reference },
            ));
        };

        // 1. Update the versioned dataset.

        let mut data_room_entry =
            MoleculeDataRoomEntry::new_from_collection_entry(&self.project, collection_entry)?;

        data_room_entry.denormalized_latest_file_info.access_level = access_level;

        data_room_entry.denormalized_latest_file_info.change_by = change_by;

        if let Some(description) = description {
            data_room_entry.denormalized_latest_file_info.description = Some(description);
        }
        if let Some(categories) = categories {
            data_room_entry.denormalized_latest_file_info.categories = categories;
        }
        if let Some(tags) = tags {
            data_room_entry.denormalized_latest_file_info.tags = tags;
        }

        let prefetch = MoleculeVersionedFilePrefetch::new_from_data_room_entry(&data_room_entry);
        let mut file_entry =
            MoleculeVersionedFileEntry::new_from_prefetched(file_dataset.clone(), prefetch);

        {
            // Read the current values: content_text & encryption_metadata
            let _ = file_entry.detailed_info(ctx).await?;

            // Safety: we just initialized the value
            let detailed_info = file_entry.detailed_info.get_mut().unwrap();

            if let Some(content_text) = content_text {
                detailed_info.content_text = Some(content_text);
            }
            if let Some(encryption_metadata) = encryption_metadata {
                detailed_info.encryption_metadata = Some(encryption_metadata);
            }
        }

        // TODO: we need to do a retraction if any errors...
        let new_version = update_versioned_file
            .execute(
                WriteCheckedDataset(&file_dataset),
                None,
                None,
                Some(file_entry.to_versioned_file_extra_data()),
            )
            .await
            .int_err()?
            .new_version;

        // 2. Update the file state in the data room.

        data_room_entry.denormalized_latest_file_info.version = new_version;
        let data_room_writable_dataset =
            self.data_room_writable_state.resolved_dataset(ctx).await?;

        match update_collection_entries
            .execute(
                WriteCheckedDataset(data_room_writable_dataset),
                vec![CollectionUpdateOperation::add(
                    data_room_entry.entry.entity.path.clone(),
                    reference.into(),
                    ExtraDataFields::new(data_room_entry.to_collection_extra_data().into()),
                )],
                None,
            )
            .await
        {
            Ok(UpdateCollectionEntriesResult::Success(_)) => {
                Ok(MoleculeDataRoomUpdateFileMetadataResultSuccess {
                    entry: data_room_entry,
                }
                .into())
            }
            Ok(
                UpdateCollectionEntriesResult::UpToDate
                | UpdateCollectionEntriesResult::NotFound(_),
            ) => {
                unreachable!()
            }
            Err(UpdateCollectionEntriesUseCaseError::Access(e)) => Err(e.into()),
            Err(UpdateCollectionEntriesUseCaseError::RefCASFailed(_)) => {
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Err(e @ UpdateCollectionEntriesUseCaseError::Internal(_)) => Err(e.int_err().into()),
        }
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
