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

use super::{
    FileVersion,
    FlowProcessTypeFilterInput,
    InitiatorFilterInput,
    MoleculeProjectConnection,
    MoleculeProjectEventConnection,
};
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

    /// Strongly typed data room accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_data_room, skip_all)]
    async fn data_room(&self, _ctx: &Context<'_>) -> Result<MoleculeDataRoomDatasetV2> {
        todo!()
    }

    /// Strongly typed announcements accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_announcements, skip_all)]
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementsDatasetV2> {
        todo!()
    }

    /// Project's activity events in reverse chronological order
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_activity, skip_all)]
    async fn activity(
        &self,
        _ctx: &Context<'_>,
        _page: Option<usize>,
        _per_page: Option<usize>,
        _filters: Option<MoleculeActivityFiltersV2>,
    ) -> Result<MoleculeProjectEventConnection> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomDatasetV2 {
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
    ) -> Result<MoleculeDataRoomEntryV2Connection> {
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
    ) -> Result<MoleculeDataRoomEntryV2> {
        let _ = path;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomEntryV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomEntryV2 {
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

    /// Strongly typed [`MoleculeVersionedFileV2`] object
    async fn as_versioned_file(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileV2> {
        todo!()
    }
}

page_based_connection!(
    MoleculeDataRoomEntryV2,
    MoleculeDataRoomEntryV2Connection,
    MoleculeDataRoomEntryV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFileV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFileV2 {
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

    async fn content_url(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileContentUrlV2> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementsDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementsDatasetV2 {
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    async fn by_id(
        &self,
        _ctx: &Context<'_>,
        id: MoleculeAnnouncementID,
    ) -> Result<MoleculeAnnouncementEntryV2> {
        let _ = id;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementEntryV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementEntryV2 {
    async fn project(&self, _ctx: &Context<'_>) -> Result<MoleculeProjectV2> {
        todo!()
    }

    async fn id(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementID> {
        todo!()
    }

    async fn headline(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    async fn body(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    async fn attachments(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    async fn access_level(&self, _ctx: &Context<'_>) -> Result<MoleculeAccessLevel> {
        todo!()
    }

    async fn change_by(&self, _ctx: &Context<'_>) -> Result<AccountID<'static>> {
        todo!()
    }

    async fn categories(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    async fn tags(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }
}

page_based_connection!(
    MoleculeAnnouncementEntryV2,
    MoleculeAnnouncementEntryV2Connection,
    MoleculeAnnouncementEntryV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct MoleculeVersionedFileContentUrlV2 {
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
// pub enum MoleculeAccessLevelV2 {
//     Public,
//     Admin,
//     Admin2,
//     Holder,
// }

pub type MoleculeAccessLevel = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: scalar?
pub type MoleculeAnnouncementID = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeActivityFiltersV2 {
    // TODO: replace w/ real filters.
    /// This filter is provided as an example.
    by_ipnft_uids: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum MoleculeActivityEventV2 {
    ActivityFileAdded(MoleculeActivityFileAddedV2),
    ActivityFileRemoved(MoleculeActivityFileRemovedV2),
    ActivityFileUpdated(MoleculeActivityFileUpdatedV2),
    ActivityAnnouncement(MoleculeActivityAnnouncementV2),
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileAddedV2 {
    pub entry: MoleculeDataRoomEntryV2,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileRemovedV2 {
    pub entry: MoleculeDataRoomEntryV2,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityFileUpdatedV2 {
    pub entry: MoleculeDataRoomEntryV2,
}

#[derive(SimpleObject)]
pub struct MoleculeActivityAnnouncementV2 {
    pub announcement: MoleculeAnnouncementEntryV2,
}

page_based_stream_connection!(
    MoleculeActivityEventV2,
    MoleculeActivityEventV2Connection,
    MoleculeActivityEventV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
