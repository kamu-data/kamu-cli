// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: breakdown to smaller files after API freeze stage.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use url::Url;

use super::{FileVersion, MoleculeProjectConnection, MoleculeProjectEventConnection};
use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeV2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeV2 {
    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        _ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectV2>> {
        let _ = ipnft_uid;
        todo!()
    }

    /// List the registered projects
    #[tracing::instrument(level = "info", name = MoleculeV2_projects, skip_all)]
    async fn projects(
        &self,
        _ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectConnection> {
        let _ = page;
        let _ = per_page;
        todo!()
    }

    /// Latest activity events across all projects in reverse chronological
    /// order
    #[tracing::instrument(level = "info", name = MoleculeV2_activity, skip_all)]
    async fn activity(
        &self,
        _ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectEventConnection> {
        let _ = page;
        let _ = per_page;
        todo!()
    }

    /// Performs a semantic search
    /// Using `filters` parameters, a search can be narrowed down to:
    /// - a specific set of projects
    /// - specific categories and tags
    /// - only returning files or announcements
    #[tracing::instrument(level = "info", name = MoleculeV2_search, skip_all)]
    async fn search(
        &self,
        _ctx: &Context<'_>,
        // TODO: update types
        prompt: String,
        filters: String,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<FoundSearchEntryConnection> {
        let _ = prompt;
        let _ = filters;
        let _ = page;
        let _ = per_page;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: revisit after IPNFT-less projects changes.
#[derive(SimpleObject, Clone)]
#[graphql(complex)]
pub struct MoleculeProjectV2 {
    #[graphql(skip)]
    pub account_id: odf::AccountID,

    /// System time when this version was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this version was created/updated
    pub event_time: DateTime<Utc>,

    /// Symbolic name of the project
    pub ipnft_symbol: String,

    /// Unique ID of the IPNFT as `{ipnftAddress}_{ipnftTokenId}`
    pub ipnft_uid: String,

    /// Address of the IPNFT contract
    pub ipnft_address: String,

    // NOTE: For backward compatibility (and existing projects),
    //       we continue using BigInt type, which is wider than needed U256.
    /// Token ID withing the IPNFT contract
    pub ipnft_token_id: BigInt,

    #[graphql(skip)]
    pub data_room_dataset_id: odf::DatasetID,

    #[graphql(skip)]
    pub announcements_dataset_id: odf::DatasetID,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[ComplexObject]
impl MoleculeProjectV2 {
    /// Project's organizational account
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_account, skip_all)]
    async fn account(&self, _ctx: &Context<'_>) -> Result<Account> {
        todo!()
    }

    /// Project's data room dataset
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_data_room, skip_all)]
    async fn data_room(&self, _ctx: &Context<'_>) -> Result<MoleculeDataRoomDataset> {
        todo!()
    }

    /// Project's announcements dataset
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_announcements, skip_all)]
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    /// Project's activity events in reverse chronological order
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_activity, skip_all)]
    async fn activity(
        &self,
        _ctx: &Context<'_>,
        _page: Option<usize>,
        _per_page: Option<usize>,
    ) -> Result<MoleculeProjectEventConnection> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomDataset;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomDataset {
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    async fn entries(
        &self,
        _ctx: &Context<'_>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeDataRoomEntryConnection> {
        let _ = path_prefix;
        let _ = max_depth;
        let _ = page;
        let _ = per_page;

        todo!()
    }

    async fn entry(
        &self,
        _ctx: &Context<'_>,
        path: CollectionPath,
    ) -> Result<MoleculeDataRoomEntry> {
        let _ = path;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomEntry;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomEntry {
    async fn project(&self, _ctx: &Context<'_>) -> Result<MoleculeProjectV2> {
        todo!()
    }

    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    async fn path(&self, _ctx: &Context<'_>) -> Result<CollectionPath> {
        todo!()
    }

    #[graphql(name = "ref")]
    async fn reference(&self, _ctx: &Context<'_>) -> Result<DatasetID<'static>> {
        todo!()
    }

    /// Access the linked core Dataset
    async fn as_dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    /// Strongly typed [`MoleculeVersionedFile`] object
    async fn as_versioned_file(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFile> {
        todo!()
    }
}

page_based_connection!(
    MoleculeDataRoomEntry,
    MoleculeDataRoomEntryConnection,
    MoleculeDataRoomEntryEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFile;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFile {
    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    async fn version(&self, _ctx: &Context<'_>) -> Result<FileVersion> {
        todo!()
    }

    async fn content_hash(&self, _ctx: &Context<'_>) -> Result<Multihash<'static>> {
        todo!()
    }

    async fn content_length(&self, _ctx: &Context<'_>) -> Result<usize> {
        todo!()
    }

    // TODO: typing
    async fn content_type(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    async fn access_level(&self, _ctx: &Context<'_>) -> Result<MoleculeAccessLevel> {
        todo!()
    }

    // TODO: typing
    async fn categories(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    // TODO: typing
    async fn tags(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    async fn content_url(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileContentUrl> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeVersionedFileContentUrl {
    pub url: Url,
    pub headers: HashMap<String, String>,
    // TODO: typing
    pub method: String,
    pub expires_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FoundSearchEntry {
    pub dummy: String,
}

page_based_connection!(
    FoundSearchEntry,
    FoundSearchEntryConnection,
    FoundSearchEntryEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: use enum instead of string?
// #[derive(Enum)]
// pub enum MoleculeAccessLevel {
//     Public,
//     Admin,
//     Admin2,
//     Holder,
// }

pub type MoleculeAccessLevel = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
