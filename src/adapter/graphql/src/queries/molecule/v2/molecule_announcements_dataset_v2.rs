// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use kamu_molecule_domain::{
    MoleculeFindProjectAnnouncementError,
    MoleculeFindProjectAnnouncementUseCase,
    MoleculeGlobalAnnouncementRecordExt,
    MoleculeViewProjectAnnouncementsUseCase,
};

use crate::prelude::*;
use crate::queries::Dataset;
use crate::queries::molecule::v2::{
    MoleculeAccessLevel,
    MoleculeAnnouncementId,
    MoleculeCategory,
    MoleculeProjectV2,
    MoleculeTag,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncements {
    pub dataset: Dataset,
    pub project: Arc<MoleculeProjectV2>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncements {
    const DEFAULT_ENTRIES_PER_PAGE: usize = 100;

    /// Access the underlying core Dataset
    async fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    #[tracing::instrument(level = "info", name = MoleculeAnnouncements_tail, skip_all, fields(?page, ?per_page))]
    async fn tail(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeAnnouncementsFilters>,
    ) -> Result<MoleculeAnnouncementEntryConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ENTRIES_PER_PAGE);

        let view_project_announcements_uc =
            from_catalog_n!(ctx, dyn MoleculeViewProjectAnnouncementsUseCase);

        let listing = view_project_announcements_uc
            .execute(
                &self.project.entity,
                filters.map(Into::into),
                Some(PaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                }),
            )
            .await
            .map_err(|e| {
                use kamu_molecule_domain::MoleculeViewProjectAnnouncementsError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    E::Internal(_) => e.int_err().into(),
                }
            })?;

        let nodes = listing
            .list
            .into_iter()
            .map(|record| {
                MoleculeAnnouncementEntry::new_from_project_announcement_record(
                    &self.project,
                    record,
                )
            })
            .collect::<Vec<_>>();

        Ok(MoleculeAnnouncementEntryConnection::new(
            nodes,
            page,
            per_page,
            listing.total_count,
        ))
    }

    #[tracing::instrument(level = "info", name = MoleculeAnnouncements_tail, skip_all, fields(id))]
    async fn by_id(
        &self,
        ctx: &Context<'_>,
        id: MoleculeAnnouncementId,
    ) -> Result<Option<MoleculeAnnouncementEntry>> {
        let find_project_announcement_uc =
            from_catalog_n!(ctx, dyn MoleculeFindProjectAnnouncementUseCase);

        // TODO: scalar validation
        let id = uuid::Uuid::parse_str(&id).int_err()?;

        let maybe_announcement = find_project_announcement_uc
            .execute(&self.project.entity, id)
            .await
            .map_err(|e| {
                use MoleculeFindProjectAnnouncementError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    E::Internal(_) => e.int_err().into(),
                }
            })?
            .map(|record| {
                MoleculeAnnouncementEntry::new_from_project_announcement_record(
                    &self.project,
                    record,
                )
            });

        Ok(maybe_announcement)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementEntry {
    pub entity: kamu_molecule_domain::MoleculeProjectAnnouncementRecord,
    pub project: Arc<MoleculeProjectV2>,
}

impl MoleculeAnnouncementEntry {
    pub fn new_from_global_announcement_record(
        project: &Arc<MoleculeProjectV2>,
        entity: kamu_molecule_domain::MoleculeGlobalAnnouncementRecord,
    ) -> Self {
        Self {
            entity: entity.into_project_announcement_record(),
            project: project.clone(),
        }
    }

    pub fn new_from_project_announcement_record(
        project: &Arc<MoleculeProjectV2>,
        entity: kamu_molecule_domain::MoleculeProjectAnnouncementRecord,
    ) -> Self {
        Self {
            entity,
            project: project.clone(),
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncementEntry {
    async fn project(&self) -> &MoleculeProjectV2 {
        self.project.as_ref()
    }

    async fn system_time(&self) -> DateTime<Utc> {
        self.entity.system_columns.system_time
    }

    async fn event_time(&self) -> DateTime<Utc> {
        self.entity.system_columns.event_time
    }

    async fn id(&self) -> MoleculeAnnouncementId {
        let id = self.entity.record.announcement_id.as_ref().unwrap();
        id.to_string()
    }

    async fn headline(&self) -> &String {
        &self.entity.record.headline
    }

    async fn body(&self) -> &String {
        &self.entity.record.body
    }

    async fn attachments<'a>(&'a self) -> Vec<DatasetID<'a>> {
        self.entity
            .record
            .attachments
            .iter()
            .map(Into::into)
            .collect()
    }

    async fn access_level(&self) -> &MoleculeAccessLevel {
        &self.entity.record.access_level
    }

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    async fn change_by(&self) -> &String {
        &self.entity.record.change_by
    }

    async fn categories(&self) -> &Vec<MoleculeCategory> {
        &self.entity.record.categories
    }

    async fn tags(&self) -> &Vec<MoleculeTag> {
        &self.entity.record.tags
    }
}

page_based_connection!(
    MoleculeAnnouncementEntry,
    MoleculeAnnouncementEntryConnection,
    MoleculeAnnouncementEntryEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeAnnouncementsFilters {
    by_tags: Option<Vec<MoleculeTag>>,
    by_categories: Option<Vec<MoleculeCategory>>,
    by_access_levels: Option<Vec<MoleculeAccessLevel>>,
}

impl From<MoleculeAnnouncementsFilters> for kamu_molecule_domain::MoleculeAnnouncementsFilters {
    fn from(value: MoleculeAnnouncementsFilters) -> Self {
        Self {
            by_tags: value.by_tags,
            by_categories: value.by_categories,
            by_access_levels: value.by_access_levels,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
