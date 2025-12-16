// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use database_common::PaginationOpts;
use kamu_molecule_domain::{
    MoleculeDataRoomFileActivityType,
    MoleculeFindProjectError,
    MoleculeFindProjectUseCase,
    MoleculeGlobalActivity,
    MoleculeProjectListing,
    MoleculeSearchError,
    MoleculeSearchFoundItem,
    MoleculeSearchUseCase,
    MoleculeViewGlobalActivitiesError,
    MoleculeViewGlobalActivitiesUseCase,
    MoleculeViewProjectsError,
    MoleculeViewProjectsUseCase,
};

use crate::molecule::molecule_subject;
use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeActivityEventV2,
    MoleculeActivityEventV2Connection,
    MoleculeAnnouncementEntry,
    MoleculeDataRoomEntry,
    MoleculeProjectActivityFilters,
    MoleculeProjectV2,
    MoleculeProjectV2Connection,
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

        let view_projects_uc = from_catalog_n!(ctx, dyn MoleculeViewProjectsUseCase);

        let listing = view_projects_uc
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
    const DEFAULT_SEARCH_RESULTS_PER_PAGE: usize = 15;

    /// Looks up the project
    #[tracing::instrument(level = "info", name = MoleculeV2_project, skip_all, fields(?ipnft_uid))]
    async fn project(
        &self,
        ctx: &Context<'_>,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectV2>> {
        let molecule_subject = molecule_subject(ctx)?;

        let find_project_uc = from_catalog_n!(ctx, dyn MoleculeFindProjectUseCase);

        let maybe_project_entity = find_project_uc
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
            .list
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
        let molecule_subject = molecule_subject(ctx)?;

        let (view_global_activities_uc, molecule_view_projects_uc) = from_catalog_n!(
            ctx,
            dyn MoleculeViewGlobalActivitiesUseCase,
            dyn MoleculeViewProjectsUseCase
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        let listing = view_global_activities_uc
            .execute(
                &molecule_subject,
                filters.map(Into::into),
                Some(PaginationOpts::from_page(page, per_page)),
            )
            .await
            .map_err(|e| -> GqlError {
                use MoleculeViewGlobalActivitiesError as E;
                match e {
                    E::Access(e) => e.into(),
                    E::Internal(_) => e.int_err().into(),
                }
            })?;

        let projects_mapping: HashMap<_, _> = {
            let listing = molecule_view_projects_uc
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
                .list
                .into_iter()
                .map(|project| {
                    (
                        project.ipnft_uid.clone(),
                        Arc::new(MoleculeProjectV2::new(project)),
                    )
                })
                .collect()
        };

        let nodes = listing
            .list
            .into_iter()
            .map(|activity| {
                let ipnft_uid = activity.ipnft_uid();
                let Some(project) = projects_mapping.get(ipnft_uid) else {
                    return Err(GqlError::gql(format!(
                        "Project [{ipnft_uid}] unexpectedly not found",
                    )));
                };

                match activity {
                    MoleculeGlobalActivity::DataRoomActivity(data_room_activity_entity) => {
                        let activity_type = data_room_activity_entity.activity_type;
                        let entry = MoleculeDataRoomEntry::new_from_data_room_activity_entity(
                            project,
                            data_room_activity_entity,
                        );

                        use MoleculeDataRoomFileActivityType as Type;

                        let activity_event = match activity_type {
                            Type::Added => MoleculeActivityEventV2::file_added(entry),
                            Type::Updated => MoleculeActivityEventV2::file_updated(entry),
                            Type::Removed => MoleculeActivityEventV2::file_removed(entry),
                        };

                        Ok(activity_event)
                    }
                    MoleculeGlobalActivity::Announcement(announcement_activity_entity) => {
                        let entry = MoleculeAnnouncementEntry::new_from_global_announcement(
                            project,
                            announcement_activity_entity,
                        );
                        Ok(MoleculeActivityEventV2::announcement(entry))
                    }
                }
            })
            .collect::<Result<_, _>>()?;

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
        ctx: &Context<'_>,
        prompt: String,
        filters: Option<MoleculeSemanticSearchFilters>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<MoleculeSemanticSearchFoundItemConnection> {
        let molecule_subject = molecule_subject(ctx)?;

        let (search_uc, molecule_view_projects_uc) = from_catalog_n!(
            ctx,
            dyn MoleculeSearchUseCase,
            dyn MoleculeViewProjectsUseCase
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_SEARCH_RESULTS_PER_PAGE);

        let listing = search_uc
            .execute(
                &molecule_subject,
                prompt.as_str(),
                filters.map(Into::into),
                Some(PaginationOpts::from_page(page, per_page)),
            )
            .await
            .map_err(|e| -> GqlError {
                use MoleculeSearchError as E;
                match e {
                    E::Access(e) => e.into(),
                    E::Internal(_) => e.int_err().into(),
                }
            })?;

        let projects_mapping: HashMap<_, _> = {
            let listing = molecule_view_projects_uc
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
                .list
                .into_iter()
                .map(|project| {
                    (
                        project.ipnft_uid.clone(),
                        Arc::new(MoleculeProjectV2::new(project)),
                    )
                })
                .collect()
        };

        let nodes = listing
            .list
            .into_iter()
            .map(|found_item| {
                let ipnft_uid = found_item.ipnft_uid();
                let Some(project) = projects_mapping.get(ipnft_uid) else {
                    return Err(GqlError::gql(format!(
                        "Project [{ipnft_uid}] unexpectedly not found",
                    )));
                };

                match found_item {
                    MoleculeSearchFoundItem::DataRoomActivity(data_room_activity) => {
                        let data_room_entry =
                            MoleculeDataRoomEntry::new_from_data_room_activity_entity(
                                project,
                                data_room_activity,
                            );
                        // NOTE: We return data from the projection, therefore,
                        //       it is always the latest.
                        let is_latest = true;
                        let entry = MoleculeVersionedFile::new(
                            Cow::Owned(data_room_entry.entity),
                            is_latest,
                        );

                        Ok(MoleculeSemanticSearchFoundItem::file(entry))
                    }
                    MoleculeSearchFoundItem::Announcement(announcement) => {
                        let entry = MoleculeAnnouncementEntry::new_from_global_announcement(
                            project,
                            announcement,
                        );
                        Ok(MoleculeSemanticSearchFoundItem::announcement(entry))
                    }
                }
            })
            .collect::<Result<_, _>>()?;

        Ok(MoleculeSemanticSearchFoundItemConnection::new(
            nodes,
            page,
            per_page,
            listing.total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeSemanticSearchFilters {
    by_ipnft_uids: Option<Vec<String>>,
    by_tags: Option<Vec<String>>,
    by_categories: Option<Vec<String>>,
    by_access_levels: Option<Vec<String>>,
    by_type: Option<MoleculeSearchTypeInput>,
}

impl From<MoleculeSemanticSearchFilters> for kamu_molecule_domain::MoleculeSearchFilters {
    fn from(value: MoleculeSemanticSearchFilters) -> Self {
        kamu_molecule_domain::MoleculeSearchFilters {
            by_ipnft_uids: value.by_ipnft_uids,
            by_tags: value.by_tags,
            by_categories: value.by_categories,
            by_access_levels: value.by_access_levels,
            by_type: value.by_type.map(Into::into),
        }
    }
}

#[derive(OneofObject, Debug)]
pub enum MoleculeSearchTypeInput {
    OnlyFiles(MoleculeSearchTypeInputInner),
    OnlyAnnouncements(MoleculeSearchTypeInputInner),
    FilesAndAnnouncements(MoleculeSearchTypeInputInner),
}

#[derive(InputObject, Debug)]
pub struct MoleculeSearchTypeInputInner {
    /// Not required to be filled out and is only needed for the declaration.
    _dummy: Option<String>,
}

impl From<MoleculeSearchTypeInput> for kamu_molecule_domain::MoleculeSearchType {
    fn from(value: MoleculeSearchTypeInput) -> Self {
        use MoleculeSearchTypeInput as Gql;
        use kamu_molecule_domain::MoleculeSearchType as Domain;

        match value {
            Gql::OnlyFiles(..) => Domain::OnlyDataRoomActivities,
            Gql::OnlyAnnouncements(..) => Domain::OnlyAnnouncements,
            Gql::FilesAndAnnouncements(..) => Domain::DataRoomActivitiesAndAnnouncements,
        }
    }
}

#[derive(Union)]
pub enum MoleculeSemanticSearchFoundItem {
    File(MoleculeSemanticSearchFoundFile),
    Announcement(MoleculeSemanticSearchFoundAnnouncement),
}

impl MoleculeSemanticSearchFoundItem {
    pub fn announcement(entry: MoleculeAnnouncementEntry) -> Self {
        Self::Announcement(MoleculeSemanticSearchFoundAnnouncement { entry })
    }

    pub fn file(entry: MoleculeVersionedFile<'static>) -> Self {
        Self::File(MoleculeSemanticSearchFoundFile { entry })
    }
}

#[derive(SimpleObject)]
pub struct MoleculeSemanticSearchFoundFile {
    pub entry: MoleculeVersionedFile<'static>,
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
