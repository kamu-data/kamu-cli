// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_molecule_domain::{
    FindMoleculeProjectError,
    FindMoleculeProjectUseCase,
    MoleculeProjectListing,
    ViewMoleculeProjectsError,
    ViewMoleculeProjectsUseCase,
};

use crate::molecule::molecule_subject;
use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeActivityEventV2Connection,
    MoleculeAnnouncementEntry,
    MoleculeCategory,
    MoleculeProjectActivityFilters,
    MoleculeProjectV2,
    MoleculeProjectV2Connection,
    MoleculeTag,
    MoleculeVersionedFile,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeV2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeV2 {
    async fn get_molecule_projects_listing(
        &self,
        ctx: &Context<'_>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectListing> {
        let molecule_subject = molecule_subject(ctx)?;

        let view_molecule_projects = from_catalog_n!(ctx, dyn ViewMoleculeProjectsUseCase);
        let listing = view_molecule_projects
            .execute(&molecule_subject, pagination)
            .await
            .map_err(|e| match e {
                ViewMoleculeProjectsError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                ViewMoleculeProjectsError::Access(e) => GqlError::Access(e),
                ViewMoleculeProjectsError::Internal(e) => GqlError::Gql(e.into()),
            })?;

        Ok(listing)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeV2 {
    const DEFAULT_PROJECTS_PER_PAGE: usize = 15;
    // const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectV2>> {
        let molecule_subject = molecule_subject(ctx)?;

        let find_molecule_project = from_catalog_n!(ctx, dyn FindMoleculeProjectUseCase);
        let maybe_project_json = find_molecule_project
            .execute(&molecule_subject, ipnft_uid)
            .await
            .map_err(|e| match e {
                FindMoleculeProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                FindMoleculeProjectError::Access(e) => GqlError::Access(e),
                FindMoleculeProjectError::Internal(e) => GqlError::Gql(e.into()),
            })?;

        let maybe_project_v2 = maybe_project_json
            .map(MoleculeProjectV2::from_json)
            .transpose()?;
        Ok(maybe_project_v2)
    }

    /// List the registered projects
    #[tracing::instrument(level = "info", name = MoleculeV2_projects, skip_all)]
    async fn projects(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeProjectV2Connection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PROJECTS_PER_PAGE);

        let listing = self
            .get_molecule_projects_listing(
                ctx,
                Some(PaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                }),
            )
            .await?;

        let nodes = listing
            .records
            .into_iter()
            .map(MoleculeProjectV2::from_json)
            .collect::<Result<Vec<MoleculeProjectV2>, _>>()?;

        Ok(MoleculeProjectV2Connection::new(
            nodes,
            page,
            per_page,
            listing.total_count,
        ))
    }

    /// Latest activity events across all projects in reverse chronological
    /// order
    #[tracing::instrument(level = "info", name = MoleculeV2_activity, skip_all)]
    async fn activity(
        &self,
        _ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeProjectActivityFilters>,
    ) -> Result<MoleculeActivityEventV2Connection> {
        let _ = page;
        let _ = per_page;
        let _ = filters;
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
        filters: Option<MoleculeSemanticSearchFilters>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeSemanticSearchFoundItemConnection> {
        let _ = prompt;
        let _ = filters;
        let _ = page;
        let _ = per_page;
        // TODO: implement
        Ok(MoleculeSemanticSearchFoundItemConnection::new(
            vec![],
            0,
            0,
            0,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeSemanticSearchFilters {
    // TODO: replace w/ real filters.
    // These filters are provided as an example.
    by_ipnft_uids: Option<Vec<String>>,
    by_categories: Option<Vec<MoleculeCategory>>,
    by_tags: Option<Vec<MoleculeTag>>,
}

#[derive(Union)]
pub enum MoleculeSemanticSearchFoundItem {
    File(MoleculeSemanticSearchFoundFile),
    Announcement(MoleculeSemanticSearchFoundAnnouncement),
}

#[derive(SimpleObject)]
pub struct MoleculeSemanticSearchFoundFile {
    pub entry: MoleculeVersionedFile,
}

#[derive(SimpleObject)]
pub struct MoleculeSemanticSearchFoundAnnouncement {
    pub entry: MoleculeAnnouncementEntry,
}

page_based_connection!(
    MoleculeSemanticSearchFoundItem,
    MoleculeSemanticSearchFoundItemConnection,
    MoleculeSemanticSearchFoundItemEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
