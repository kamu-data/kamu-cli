// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use file_utils::MediaType;
use kamu::domain;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{UpdateVersionFileUseCase, UpdateVersionFileUseCaseError};
use kamu_datasets_services::utils::{ContentSource, UpdateVersionFileUseCaseHelper};

use crate::mutations::{
    StartUploadVersionErrorTooLarge,
    StartUploadVersionResult,
    StartUploadVersionSuccess,
    UpdateVersionErrorCasFailed,
    UpdateVersionResult,
    UpdateVersionSuccess,
    map_get_content_args_error,
};
use crate::prelude::*;
use crate::queries::molecule::v2::{MoleculeAccessLevelV2, MoleculeCategoryV2, MoleculeTagV2};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomMutV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomMutV2 {
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

    #[expect(clippy::unused_async)]
    /// Finishes the process of uploading a file to the data room.
    async fn finish_upload_file(
        &self,
        _ctx: &Context<'_>,
        upload_token: String,
        #[graphql(name = "ref")] reference: DatasetID<'static>,
        path: CollectionPath,
        access_level: MoleculeAccessLevelV2,
        change_by: AccountID<'static>,
        description: String,
        categories: Vec<MoleculeCategoryV2>,
        tags: Vec<MoleculeTagV2>,
        content_text: String,
    ) -> Result<MoleculeDataRoomFinishUploadFileResultV2> {
        let _ = upload_token;
        let _ = reference;
        let _ = path;
        let _ = access_level;
        let _ = change_by;
        let _ = description;
        let _ = categories;
        let _ = tags;
        let _ = content_text;
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Moves an entry in the data room.
    async fn move_entry(
        &self,
        _ctx: &Context<'_>,
        from_path: CollectionPath,
        to_path: CollectionPath,
        expected_head: Multihash<'static>,
    ) -> Result<MoleculeDataRoomMoveEntryResultV2> {
        let _ = from_path;
        let _ = to_path;
        let _ = expected_head;
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Removes an entry from the data room.
    async fn remove_entry(
        &self,
        _ctx: &Context<'_>,
        path: CollectionPath,
        expected_head: Multihash<'static>,
    ) -> Result<MoleculeDataRoomRemoveEntryResultV2> {
        let _ = path;
        let _ = expected_head;
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Updates the metadata of a file in the data room.
    async fn update_file_metadata(
        &self,
        _ctx: &Context<'_>,
        #[graphql(name = "ref")] reference: DatasetID<'static>,
        // TODO: use update object w/ optional fields instead
        access_level: MoleculeAccessLevelV2,
        change_by: AccountID<'static>,
        description: String,
        categories: Vec<String>,
        tags: Vec<String>,
        content_text: String,
        expected_head: Multihash<'static>,
    ) -> Result<MoleculeDataRoomUpdateFileMetadataResultV2> {
        let _ = reference;
        let _ = access_level;
        let _ = change_by;
        let _ = description;
        let _ = categories;
        let _ = tags;
        let _ = content_text;
        let _ = expected_head;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomUploadFileResultV2 {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomFinishUploadFileResultV2 {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomMoveEntryResultV2 {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomRemoveEntryResultV2 {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomUpdateFileMetadataResultV2 {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
