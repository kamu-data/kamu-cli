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
    ContentArgs,
    CreateDatasetUseCaseOptions,
    ExtraDataFields,
    UpdateVersionFileUseCase,
};
use kamu_datasets_services::utils::{ContentSource, UpdateVersionFileUseCaseHelper};
use odf::utils::data::DataFrameExt;

use crate::mutations::{
    CollectionEntryInput,
    CollectionMut,
    CollectionUpdateResult,
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
    MoleculeAccessLevelV2,
    MoleculeCategoryV2,
    MoleculeDataRoomEntryV2,
    MoleculeTagV2,
    MoleculeVersionedFileV2,
};
use crate::queries::{Account, CollectionEntry, DatasetRequestState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomMutV2<'a> {
    project_account_id: &'a odf::AccountID,
    data_room_writable_state: DatasetRequestState,
}

impl<'a> MoleculeDataRoomMutV2<'a> {
    pub fn new(
        project_account_id: &'a odf::AccountID,
        data_room_writable_state: DatasetRequestState,
    ) -> Self {
        Self {
            project_account_id,
            data_room_writable_state,
        }
    }

    // TODO: Specialize error handling
    async fn finish_upload_file_new_file(
        &self,
        ctx: &Context<'_>,
        content_args: ContentArgs,
        path: CollectionPath,
        access_level: MoleculeAccessLevelV2,
        change_by: String,
        description: String,
        categories: Vec<MoleculeCategoryV2>,
        tags: Vec<MoleculeTagV2>,
        content_text: String,
        encryption_metadata: Option<Json<EncryptionMetadata>>,
    ) -> Result<()> {
        let (create_dataset_from_snapshot_use_case, update_version_file_use_case) = from_catalog_n!(
            ctx,
            dyn kamu_datasets::CreateDatasetFromSnapshotUseCase,
            dyn UpdateVersionFileUseCase
        );

        // 1. Create an empty versioned dataset.

        let alias = self.build_new_file_dataset_alias(ctx, &path).await;
        let versioned_file_snapshot = MoleculeVersionedFileV2::dataset_snapshot(alias);

        let create_versioned_file_res = create_dataset_from_snapshot_use_case
            .execute(
                versioned_file_snapshot,
                CreateDatasetUseCaseOptions::default(),
            )
            .await
            .int_err()?;

        // 2. Upload the first version to just created dataset.

        let versioned_file_extra_data = MoleculeVersionedFileV2::build_extra_data_json_map(
            &access_level,
            &change_by,
            &description,
            &categories,
            &tags,
            &content_text,
            encryption_metadata.as_ref(),
        );

        let content_type = content_args
            .content_type
            .as_ref()
            .map(|ct| ct.0.clone())
            // TODO: is it OK to have no content type?
            .unwrap_or_default();
        let content_length = content_args.content_length;

        let new_version = update_version_file_use_case
            .execute(
                &create_versioned_file_res.dataset_handle,
                Some(content_args),
                None,
                Some(ExtraDataFields::new(versioned_file_extra_data)),
            )
            .await
            .int_err()?
            .new_version;

        // 3. Add the file to the data room.

        // TODO: Revisit after rebase (Roman's retry changes).
        //       Temporary hacky path -- should use domain
        //       instead of reusing the adapter.
        let data_room_collection = CollectionMut::new(&self.data_room_writable_state);

        let data_room_entry_extra_data = MoleculeDataRoomEntryV2::build_extra_data_json_map(
            &access_level,
            &change_by,
            &content_type,
            content_length,
            &categories,
            &tags,
            new_version,
        );
        let new_data_room_entry_input = CollectionEntryInput {
            path,
            reference: create_versioned_file_res.dataset_handle.id.into(),
            extra_data: Some(ExtraData::new(data_room_entry_extra_data)),
        };

        match data_room_collection
            .add_entry(ctx, new_data_room_entry_input, None)
            .await
        {
            Ok(CollectionUpdateResult::Success(_)) => Ok(()),
            Ok(CollectionUpdateResult::CasFailed(_)) => {
                // TODO: Propagate a nice error
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Ok(CollectionUpdateResult::UpToDate(_) | CollectionUpdateResult::NotFound(_)) => {
                unreachable!()
            }
            Err(e) => Err(e),
        }
    }

    // TODO: Specialize error handling
    async fn finish_upload_file_new_file_version(
        &self,
        ctx: &Context<'_>,
        content_args: ContentArgs,
        reference: DatasetID<'static>,
        access_level: MoleculeAccessLevelV2,
        change_by: String,
        description: String,
        categories: Vec<MoleculeCategoryV2>,
        tags: Vec<MoleculeTagV2>,
        content_text: String,
        encryption_metadata: Option<Json<EncryptionMetadata>>,
    ) -> Result<()> {
        let (update_version_file_use_case, dataset_registry) = from_catalog_n!(
            ctx,
            dyn UpdateVersionFileUseCase,
            Arc<dyn kamu_core::DatasetRegistry>
        );

        // 1. Get the existing versioned dataset entry -- we need to know `path`;
        let Some(collection_entry) = self.get_data_room_entry(ctx, reference.as_ref()).await?
        else {
            unreachable!();
        };

        // 2. Upload the next version to the specified dataset.

        // NOTE: Access rights will be checked inside the use case.
        let dataset_handle = dataset_registry
            .resolve_dataset_handle_by_ref(&reference.as_ref().as_local_ref())
            .await
            .int_err()?;

        let versioned_file_extra_data = MoleculeVersionedFileV2::build_extra_data_json_map(
            &access_level,
            &change_by,
            &description,
            &categories,
            &tags,
            &content_text,
            encryption_metadata.as_ref(),
        );

        let content_type = content_args
            .content_type
            .as_ref()
            .map(|ct| ct.0.clone())
            // TODO: is it OK to have no content type?
            .unwrap_or_default();
        let content_length = content_args.content_length;

        let new_version = update_version_file_use_case
            .execute(
                &dataset_handle,
                Some(content_args),
                None,
                Some(ExtraDataFields::new(versioned_file_extra_data)),
            )
            .await
            .int_err()?
            .new_version;

        // 3. Update the file state in the data room.

        // TODO: Revisit after rebase (Roman's retry changes).
        //       Temporary hacky path -- should use domain
        //       instead of reusing the adapter.
        let data_room_collection = CollectionMut::new(&self.data_room_writable_state);

        let data_room_entry_extra_data = MoleculeDataRoomEntryV2::build_extra_data_json_map(
            &access_level,
            &change_by,
            &content_type,
            content_length,
            &categories,
            &tags,
            new_version,
        );

        let new_data_room_entry_input = CollectionEntryInput {
            path: collection_entry.path,
            reference,
            extra_data: Some(ExtraData::new(data_room_entry_extra_data)),
        };

        match data_room_collection
            .add_entry(ctx, new_data_room_entry_input, None)
            .await
        {
            Ok(CollectionUpdateResult::Success(_)) => Ok(()),
            Ok(CollectionUpdateResult::CasFailed(_)) => {
                // TODO: Propagate a nice error
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Ok(CollectionUpdateResult::UpToDate(_) | CollectionUpdateResult::NotFound(_)) => {
                unreachable!()
            }
            Err(e) => Err(e),
        }
    }

    async fn get_data_room_projection(
        &self,
        ctx: &Context<'_>,
    ) -> Result<(domain::ResolvedDataset, Option<DataFrameExt>)> {
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

        let entry = CollectionEntry::from_json(records.into_iter().next().unwrap())?;

        Ok(Some(entry))
    }

    // TODO: Test with different paths
    async fn build_new_file_dataset_alias(
        &self,
        ctx: &Context<'_>,
        file_path: &CollectionPath,
    ) -> odf::DatasetAlias {
        // TODO: PERF: Add AccountRequestState similar to DatasetRequestState and reuse
        //             possibly resolved account?
        let project_account = Account::from_account_id(ctx, self.project_account_id.clone())
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to load project account [{}]: {e}",
                    self.project_account_id
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
impl MoleculeDataRoomMutV2<'_> {
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
        path: Option<CollectionPath>,
        #[graphql(desc = "Media type of content (e.g. application/pdf)")] content_type: Option<
            String,
        >,
        access_level: MoleculeAccessLevelV2,
        change_by: String,
        description: String,
        categories: Vec<MoleculeCategoryV2>,
        tags: Vec<MoleculeTagV2>,
        content_text: String,
        encryption_metadata: Option<Json<EncryptionMetadata>>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResultV2> {
        let update_version_file_use_case_helper =
            from_catalog_n!(ctx, UpdateVersionFileUseCaseHelper);

        let content_args = update_version_file_use_case_helper
            .get_content_args(ContentSource::Bytes(&content), None)
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
                .await?;
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
                .await?;
            }
            _ => return Err(GqlError::gql("Either `path` or `ref` must be specified")),
        }

        Ok(MoleculeDataRoomFinishUploadFileResultSuccessV2::default().into())
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
        path: Option<CollectionPath>,
        access_level: MoleculeAccessLevelV2,
        change_by: String,
        description: String,
        categories: Vec<MoleculeCategoryV2>,
        tags: Vec<MoleculeTagV2>,
        content_text: String,
        encryption_metadata: Option<Json<EncryptionMetadata>>,
    ) -> Result<MoleculeDataRoomFinishUploadFileResultV2> {
        // Warning: here be dragons.

        // IMPORTANT: If after file creation or version update an error occurs,
        //            all DB will be cleared (transaction rollback). Dataset data
        //            (e.g., on S3) will need later cleanup (garbage collection).

        let update_version_file_use_case_helper =
            from_catalog_n!(ctx, UpdateVersionFileUseCaseHelper);

        let content_args = update_version_file_use_case_helper
            .get_content_args(ContentSource::Token(upload_token), None)
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
                .await?;
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
                .await?;
            }
            _ => return Err(GqlError::gql("Either `path` or `ref` must be specified")),
        }

        Ok(MoleculeDataRoomFinishUploadFileResultSuccessV2::default().into())
    }

    /// Moves an entry in the data room.
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_move_entry, skip_all)]
    async fn move_entry(
        &self,
        ctx: &Context<'_>,
        from_path: CollectionPath,
        to_path: CollectionPath,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<CollectionUpdateResult> {
        // TODO: Revisit after rebase (Roman's retry changes)
        //       Temporary hacky path -- should use domain
        //       instead of reusing the adapter.
        let data_room_collection = CollectionMut::new(&self.data_room_writable_state);

        data_room_collection
            .move_entry(ctx, from_path, to_path, None, expected_head)
            .await
    }

    /// Removes an entry from the data room.
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_remove_entry, skip_all)]
    async fn remove_entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPath,
        expected_head: Option<Multihash<'static>>,
    ) -> Result<CollectionUpdateResult> {
        // TODO: delete file dataset?

        // TODO: Revisit after rebase (Roman's retry changes)
        //       Temporary hacky path -- should use domain
        //       instead of reusing the adapter.
        let data_room_collection = CollectionMut::new(&self.data_room_writable_state);

        data_room_collection
            .remove_entry(ctx, path, expected_head)
            .await
    }

    /// Updates the metadata of a file in the data room.
    #[tracing::instrument(level = "info", name = MoleculeDataRoomMutV2_update_file_metadata, skip_all)]
    async fn update_file_metadata(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "ref")] reference: DatasetID<'static>,
        // TODO: use update object w/ optional fields instead?
        access_level: MoleculeAccessLevelV2,
        change_by: String,
        description: String,
        categories: Vec<String>,
        tags: Vec<String>,
        content_text: String,
        encryption_metadata: Option<Json<EncryptionMetadata>>,
    ) -> Result<MoleculeDataRoomUpdateFileMetadataResultV2> {
        let (update_version_file_use_case, dataset_registry) = from_catalog_n!(
            ctx,
            dyn UpdateVersionFileUseCase,
            Arc<dyn kamu_core::DatasetRegistry>
        );

        // 1. Update the versioned dataset.

        // NOTE: Access rights will be checked inside the use case.
        let entry_dataset_handle = {
            use odf::DatasetRefUnresolvedError as E;
            match dataset_registry
                .resolve_dataset_handle_by_ref(&reference.as_ref().as_local_ref())
                .await
            {
                // TODO: Check if the versioned dataset is in data room at all?
                Ok(hdl) => hdl,
                Err(E::NotFound(_)) => {
                    return Ok(MoleculeDataRoomUpdateFileMetadataResultV2::EntryNotFound(
                        MoleculeDataRoomUpdateFileMetadataResultEntryNotFoundV2 {
                            r#ref: reference,
                        },
                    ));
                }
                Err(e @ E::Internal(_)) => return Err(e.int_err().into()),
            }
        };

        let versioned_file_extra_data = MoleculeVersionedFileV2::build_extra_data_json_map(
            &access_level,
            &change_by,
            &description,
            &categories,
            &tags,
            &content_text,
            encryption_metadata.as_ref(),
        );

        // TODO: we need to do a retraction if any errors...
        let _new_version = update_version_file_use_case
            .execute(
                &entry_dataset_handle,
                None,
                None,
                Some(ExtraDataFields::new(versioned_file_extra_data)),
            )
            .await
            .int_err()?
            .new_version;

        Ok(MoleculeDataRoomUpdateFileMetadataResultSuccessV2::default().into())

        // TODO: Can be unlocked by "!!! Read from the last entry..." step

        /*// 2. Update the file state in the data room.

        // TODO: Revisit after rebase (Roman's retry changes).
        //       Temporary hacky path -- should use domain
        //       instead of reusing the adapter.
        let data_room_collection = CollectionMut::new(&self.data_room_writable_state);

        // TODO: !!! Read from the last entry...
        //       Return from UpdateVersionFileUseCase?
        //       -->
        let content_type = String::new();
        let content_length = 0;
        let path = String::new().into();
        //       <--

        let data_room_entry_extra_data = MoleculeDataRoomEntryV2::build_extra_data_json_map(
            &access_level,
            &change_by,
            &content_type,
            content_length,
            &categories,
            &tags,
            new_version,
        );
        let new_data_room_entry_input = CollectionEntryInput {
            path,
            reference: entry_dataset_handle.id.into(),
            extra_data: Some(ExtraData::new(data_room_entry_extra_data)),
        };

        match data_room_collection
            .add_entry(ctx, new_data_room_entry_input, None)
            .await
        {
            Ok(CollectionUpdateResult::Success(_)) => {
                Ok(MoleculeDataRoomUpdateFileMetadataResultSuccessV2::default().into())
            }
            Ok(CollectionUpdateResult::CasFailed(_)) => {
                // TODO: Propagate a nice error
                Err(GqlError::gql("Data room linking: CAS failed"))
            }
            Ok(CollectionUpdateResult::UpToDate(_) | CollectionUpdateResult::NotFound(_)) => {
                unreachable!()
            }
            Err(e) => Err(e),
        }*/
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Should be a mix from UpdateVersionResult & CollectionUpdateResult?
#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum MoleculeDataRoomFinishUploadFileResultV2 {
    Success(MoleculeDataRoomFinishUploadFileResultSuccessV2),
}

#[derive(SimpleObject, Default)]
#[graphql(complex)]
pub struct MoleculeDataRoomFinishUploadFileResultSuccessV2 {
    pub message: String,
}

#[ComplexObject]
impl MoleculeDataRoomFinishUploadFileResultSuccessV2 {
    pub async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Should be a mix from UpdateVersionResult & CollectionUpdateResult?
#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum MoleculeDataRoomUpdateFileMetadataResultV2 {
    Success(MoleculeDataRoomUpdateFileMetadataResultSuccessV2),
    EntryNotFound(MoleculeDataRoomUpdateFileMetadataResultEntryNotFoundV2),
    CasFailed(UpdateVersionErrorCasFailed),
    InvalidExtraData(UpdateVersionErrorInvalidExtraData),
}

#[derive(SimpleObject, Default)]
#[graphql(complex)]
pub struct MoleculeDataRoomUpdateFileMetadataResultSuccessV2 {
    pub message: String,
}

#[ComplexObject]
impl MoleculeDataRoomUpdateFileMetadataResultSuccessV2 {
    pub async fn is_success(&self) -> bool {
        false
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct MoleculeDataRoomUpdateFileMetadataResultEntryNotFoundV2 {
    pub r#ref: DatasetID<'static>,
}
#[ComplexObject]
impl MoleculeDataRoomUpdateFileMetadataResultEntryNotFoundV2 {
    pub async fn is_success(&self) -> bool {
        false
    }

    pub async fn message(&self) -> String {
        "Data room entry not found".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
