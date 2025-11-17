// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::{CreateAnnouncementResult, CreateProjectResult, MoleculeMutV1};
use crate::prelude::*;
use crate::queries::MoleculeAccessLevel;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct MoleculeMutV2 {
    v1: MoleculeMutV1,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeMutV2 {
    #[graphql(guard = "LoggedInGuard")]
    #[tracing::instrument(level = "info", name = MoleculeMutV2_create_project, skip_all, fields(?ipnft_symbol, ?ipnft_uid))]
    async fn create_project(
        &self,
        ctx: &Context<'_>,
        ipnft_symbol: String,
        ipnft_uid: String,
        ipnft_address: String,
        ipnft_token_id: U256,
    ) -> Result<CreateProjectResult> {
        self.v1
            .create_project(ctx, ipnft_symbol, ipnft_uid, ipnft_address, ipnft_token_id)
            .await
    }

    /// Retracts a project from the `projects` dataset.
    /// History of this project existing will be preserved,
    /// its symbol will remain reserved, data will remain intact,
    /// but the project will no longer appear in the listing.
    #[tracing::instrument(level = "info", name = MoleculeMutV2_remove_project, skip_all, fields(?ipnft_uid))]
    async fn remove_project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<MoleculeRemoveProjectResult> {
        let _ = ipnft_uid;
        todo!()
    }

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeMutV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectMutV2>> {
        let _ = ipnft_uid;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeProjectMutV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectMutV2 {
    /// Strongly typed data room mutator
    async fn data_room(&self, _ctx: &Context<'_>) -> Result<MoleculeDataRoomMutV2> {
        todo!()
    }

    /// Strongly typed announcements mutator
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementsDatasetMutV2> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomMutV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomMutV2 {
    /// Starts the process of uploading a file to the data room.
    async fn start_upload_file(
        &self,
        _ctx: &Context<'_>,
        content_size: usize,
    ) -> Result<MoleculeDataRoomUploadFileResult> {
        let _ = content_size;
        todo!()
    }

    /// Finishes the process of uploading a file to the data room.
    async fn finish_upload_file(
        &self,
        _ctx: &Context<'_>,
        upload_token: String,
        #[graphql(name = "ref")] reference: DatasetID<'static>,
        path: CollectionPath,
        access_level: MoleculeAccessLevel,
        change_by: AccountID<'static>,
        description: String,
        categories: Vec<String>,
        tags: Vec<String>,
        content_text: String,
    ) -> Result<MoleculeDataRoomFinishUploadFileResult> {
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

    /// Moves an entry in the data room.
    async fn move_entry(
        &self,
        _ctx: &Context<'_>,
        from_path: CollectionPath,
        to_path: CollectionPath,
        expected_head: Multihash<'static>,
    ) -> Result<MoleculeDataRoomMoveEntryResult> {
        let _ = from_path;
        let _ = to_path;
        let _ = expected_head;
        todo!()
    }

    /// Removes an entry from the data room.
    async fn remove_entry(
        &self,
        _ctx: &Context<'_>,
        path: CollectionPath,
        expected_head: Multihash<'static>,
    ) -> Result<MoleculeDataRoomRemoveEntryResult> {
        let _ = path;
        let _ = expected_head;
        todo!()
    }

    /// Updates the metadata of a file in the data room.
    async fn update_file_metadata(
        &self,
        _ctx: &Context<'_>,
        #[graphql(name = "ref")] reference: DatasetID<'static>,
        // TODO: use update object w/ optional fields instead
        access_level: MoleculeAccessLevel,
        change_by: AccountID<'static>,
        description: String,
        categories: Vec<String>,
        tags: Vec<String>,
        content_text: String,
        expected_head: Multihash<'static>,
    ) -> Result<MoleculeDataRoomUpdateFileMetadataResult> {
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

pub struct MoleculeAnnouncementsDatasetMutV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementsDatasetMutV2 {
    /// Creates an announcement record for the project.
    #[tracing::instrument(level = "info", name = MoleculeAnnouncementsDatasetMutV2_create, skip_all)]
    async fn create(
        &self,
        _ctx: &Context<'_>,
        headline: String,
        body: String,
        #[graphql(desc = "List of dataset DIDs to link")] attachments: Option<Vec<String>>,
        molecule_access_level: MoleculeAccessLevel,
        molecule_change_by: String,
        categories: Vec<String>,
        tags: Vec<String>,
    ) -> Result<CreateAnnouncementResult> {
        let _ = headline;
        let _ = body;
        let _ = attachments;
        let _ = molecule_access_level;
        let _ = molecule_change_by;
        let _ = categories;
        let _ = tags;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeRemoveProjectResult {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomUploadFileResult {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomFinishUploadFileResult {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomMoveEntryResult {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomRemoveEntryResult {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeDataRoomUpdateFileMetadataResult {
    pub dummy: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
