// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use super::{MoleculeProjectConnection, MoleculeProjectEventConnection};
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
    async fn data_room(&self, _ctx: &Context<'_>) -> Result<Dataset> {
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
