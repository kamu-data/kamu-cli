// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeActivityEventV2Connection,
    MoleculeAnnouncementEntryV2,
    MoleculeProjectV2,
    MoleculeProjectV2Connection,
    MoleculeVersionedFileV2,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeV2;

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
        // TODO: filters?
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
