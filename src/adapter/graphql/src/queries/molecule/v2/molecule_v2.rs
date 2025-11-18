// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use url::Url;

use crate::prelude::*;
use crate::queries::molecule::v2::{MoleculeProjectV2, MoleculeProjectV2Connection};
use crate::queries::{Dataset, FileVersion};

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
    ) -> Result<MoleculeProjectV2Connection> {
        let _ = page;
        let _ = per_page;
        // TODO: implement
        Ok(MoleculeProjectV2Connection::new(Vec::new(), 0, 0, 0))
    }

    /// Latest activity events across all projects in reverse chronological
    /// order
    #[tracing::instrument(level = "info", name = MoleculeV2_activity, skip_all)]
    async fn activity(
        &self,
        _ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeActivityEventV2Connection> {
        let _ = page;
        let _ = per_page;
        // TODO: implement
        Ok(MoleculeActivityEventV2Connection::new(Vec::new(), 0, 0))
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
        filters: Option<MoleculeSemanticSearchFiltersV2>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeSemanticSearchFoundItemV2Connection> {
        let _ = prompt;
        let _ = filters;
        let _ = page;
        let _ = per_page;
        // TODO: implement
        Ok(MoleculeSemanticSearchFoundItemV2Connection::new(
            vec![],
            0,
            0,
            0,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomDatasetV2 {
    #[expect(clippy::unused_async)]
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
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

        // TODO: implement
        Ok(MoleculeDataRoomEntryV2Connection::new(vec![], 0, 0, 0))
    }

    #[expect(clippy::unused_async)]
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
    #[expect(clippy::unused_async)]
    async fn project(&self, _ctx: &Context<'_>) -> Result<MoleculeProjectV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn path(&self, _ctx: &Context<'_>) -> Result<CollectionPath> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    #[graphql(name = "ref")]
    async fn reference(&self, _ctx: &Context<'_>) -> Result<DatasetID<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    /// Access the linked core Dataset
    async fn as_dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
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
    #[expect(clippy::unused_async)]
    async fn system_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn event_time(&self, _ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn version(&self, _ctx: &Context<'_>) -> Result<FileVersion> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_hash(&self, _ctx: &Context<'_>) -> Result<Multihash<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_length(&self, _ctx: &Context<'_>) -> Result<usize> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    // TODO: typing
    async fn content_type(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn access_level(&self, _ctx: &Context<'_>) -> Result<MoleculeAccessLevelV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    // TODO: typing
    async fn categories(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    // TODO: typing
    async fn tags(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn content_url(&self, _ctx: &Context<'_>) -> Result<MoleculeVersionedFileContentUrlV2> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementsDatasetV2;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementsDatasetV2 {
    #[expect(clippy::unused_async)]
    /// Access the underlying core Dataset
    async fn dataset(&self, _ctx: &Context<'_>) -> Result<Dataset> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn tail(
        &self,
        _ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeAnnouncementEntryV2Connection> {
        let _ = page;
        let _ = per_page;
        Ok(MoleculeAnnouncementEntryV2Connection::new(vec![], 0, 0, 0))
    }

    #[expect(clippy::unused_async)]
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
    #[expect(clippy::unused_async)]
    async fn project(&self, _ctx: &Context<'_>) -> Result<MoleculeProjectV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn id(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncementID> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn headline(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn body(&self, _ctx: &Context<'_>) -> Result<String> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn attachments(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn access_level(&self, _ctx: &Context<'_>) -> Result<MoleculeAccessLevelV2> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn change_by(&self, _ctx: &Context<'_>) -> Result<AccountID<'static>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
    async fn categories(&self, _ctx: &Context<'_>) -> Result<Vec<String>> {
        todo!()
    }

    #[expect(clippy::unused_async)]
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

#[derive(InputObject)]
pub struct MoleculeSemanticSearchFiltersV2 {
    // TODO: replace w/ real filters.
    // These filters are provided as an example.
    by_ipnft_uids: Option<Vec<String>>,
    by_tags: Option<Vec<String>>,
    by_categories: Option<Vec<String>>,
}

#[derive(Union)]
pub enum MoleculeSemanticSearchFoundItemV2 {
    File(MoleculeSemanticSearchFoundFileV2),
    Announcement(MoleculeSemanticSearchFoundAnnouncementV2),
}

#[derive(SimpleObject)]
pub struct MoleculeSemanticSearchFoundFileV2 {
    pub entry: MoleculeVersionedFileV2,
}

#[derive(SimpleObject)]
pub struct MoleculeSemanticSearchFoundAnnouncementV2 {
    pub entry: MoleculeAnnouncementEntryV2,
}

page_based_connection!(
    MoleculeSemanticSearchFoundItemV2,
    MoleculeSemanticSearchFoundItemV2Connection,
    MoleculeSemanticSearchFoundItemV2Edge
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

pub type MoleculeAccessLevelV2 = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: scalar?
pub type MoleculeAnnouncementID = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeActivityFiltersV2 {
    // TODO: replace w/ real filters.
    /// This filter is provided as an example.
    by_ipnft_uids: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum MoleculeActivityEventV2 {
    FileAdded(MoleculeActivityFileAddedV2),
    FileRemoved(MoleculeActivityFileRemovedV2),
    FileUpdated(MoleculeActivityFileUpdatedV2),
    Announcement(MoleculeActivityAnnouncementV2),
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
