// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::PaginationOpts;
use kamu_molecule_domain::{
    MoleculeDataRoomFileActivityType,
    MoleculeFindProjectError,
    MoleculeFindProjectUseCase,
    MoleculeProjectListing,
    MoleculeViewDataRoomActivitiesError,
    MoleculeViewDataRoomActivitiesUseCase,
    MoleculeViewProjectsError,
    MoleculeViewProjectsUseCase,
};

use crate::molecule::molecule_subject;
use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeActivityEventV2,
    MoleculeActivityEventV2Connection,
    MoleculeAnnouncementEntry,
    MoleculeCategory,
    MoleculeDataRoomEntry,
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

        let molecule_view_projects = from_catalog_n!(ctx, dyn MoleculeViewProjectsUseCase);
        let listing = molecule_view_projects
            .execute(&molecule_subject, pagination)
            .await
            .map_err(|e| match e {
                MoleculeViewProjectsError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                MoleculeViewProjectsError::Access(e) => GqlError::Access(e),
                e @ MoleculeViewProjectsError::Internal(_) => e.int_err().into(),
            })?;

        Ok(listing)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeV2 {
    const DEFAULT_PROJECTS_PER_PAGE: usize = 15;
    const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectV2>> {
        let molecule_subject = molecule_subject(ctx)?;

        let molecule_find_project = from_catalog_n!(ctx, dyn MoleculeFindProjectUseCase);
        let maybe_project_entity = molecule_find_project
            .execute(&molecule_subject, ipnft_uid)
            .await
            .map_err(|e| match e {
                MoleculeFindProjectError::NoProjectsDataset(e) => GqlError::Gql(e.into()),
                MoleculeFindProjectError::Access(e) => GqlError::Access(e),
                e @ MoleculeFindProjectError::Internal(_) => e.int_err().into(),
            })?;

        Ok(maybe_project_entity.map(MoleculeProjectV2::new))
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
            .projects
            .into_iter()
            .map(MoleculeProjectV2::new)
            .collect();

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
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeProjectActivityFilters>,
    ) -> Result<MoleculeActivityEventV2Connection> {
        // TODO: filters
        assert!(filters.is_none());

        let molecule_subject = molecule_subject(ctx)?;

        let (view_data_room_activities_use_case, molecule_view_projects_use_case) = from_catalog_n!(
            ctx,
            dyn MoleculeViewDataRoomActivitiesUseCase,
            dyn MoleculeViewProjectsUseCase
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        // TODO: announcements

        let listing = view_data_room_activities_use_case
            .execute(
                &molecule_subject,
                Some(PaginationOpts::from_page(page, per_page)),
            )
            .await
            .map_err(|e| -> GqlError {
                use MoleculeViewDataRoomActivitiesError as E;
                match e {
                    E::Access(e) => e.into(),
                    E::Internal(_) => e.int_err().into(),
                }
            })?;

        let projects_mapping: HashMap<_, _> = {
            let listing = molecule_view_projects_use_case
                .execute(&molecule_subject, None)
                .await
                .map_err(|e| -> GqlError {
                    use MoleculeViewProjectsError as E;
                    match e {
                        E::Access(e) => e.into(),
                        E::NoProjectsDataset(_) | E::Internal(_) => e.int_err().into(),
                    }
                })?;

            listing
                .projects
                .into_iter()
                .map(|project| {
                    (
                        project.ipnft_uid.clone(),
                        Arc::new(MoleculeProjectV2::new(project)),
                    )
                })
                .collect()
        };
        let mut nodes = Vec::with_capacity(listing.list.len());

        for activity in listing.list {
            let Some(project) = projects_mapping.get(&activity.ipnft_uid) else {
                return Err(GqlError::gql(format!(
                    "Project [{}] unexpectedly not found",
                    activity.ipnft_uid
                )));
            };

            let activity_type = activity.activity_type;
            let entry =
                MoleculeDataRoomEntry::new_from_data_room_activity_entity(project, activity);

            use MoleculeDataRoomFileActivityType as Type;

            let activity_event = match activity_type {
                Type::Added => MoleculeActivityEventV2::file_added(entry),
                Type::Updated => MoleculeActivityEventV2::file_updated(entry),
                Type::Removed => MoleculeActivityEventV2::file_removed(entry),
            };

            nodes.push(activity_event);
        }

        Ok(MoleculeActivityEventV2Connection::new(
            nodes, page, per_page,
        ))
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
