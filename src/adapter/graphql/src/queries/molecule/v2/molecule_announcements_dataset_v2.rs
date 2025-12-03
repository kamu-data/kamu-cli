// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

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

pub struct MoleculeAnnouncements;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeAnnouncements {
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
        filters: Option<MoleculeAnnouncementsFilters>,
    ) -> Result<MoleculeAnnouncementEntryConnection> {
        let _ = page;
        let _ = per_page;
        let _ = filters;
        Ok(MoleculeAnnouncementEntryConnection::new(vec![], 0, 0, 0))
    }

    #[expect(clippy::unused_async)]
    async fn by_id(
        &self,
        _ctx: &Context<'_>,
        id: MoleculeAnnouncementId,
    ) -> Result<MoleculeAnnouncementEntry> {
        let _ = id;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeAnnouncementEntry {
    pub entity: kamu_molecule_domain::MoleculeGlobalAnnouncementRecord,
    pub project: Arc<MoleculeProjectV2>,
}

impl MoleculeAnnouncementEntry {
    pub fn new_from_global_announcement_record(
        project: &Arc<MoleculeProjectV2>,
        entity: kamu_molecule_domain::MoleculeGlobalAnnouncementRecord,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
