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
    MoleculeDataRoomFileActivityType,
    MoleculeProjectActivity,
    MoleculeViewDataRoomActivitiesError,
    MoleculeViewProjectActivitiesMode,
    MoleculeViewProjectActivitiesUseCase,
};

use crate::prelude::*;
use crate::queries::molecule::v2::{
    MoleculeAccessLevelRuleInput,
    MoleculeActivityEventV2,
    MoleculeActivityEventV2Connection,
    MoleculeAnnouncementEntry,
    MoleculeAnnouncements,
    MoleculeDataRoom,
    MoleculeDataRoomEntry,
};
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: revisit after IPNFT-less projects changes.
#[derive(Clone)]
pub struct MoleculeProjectV2 {
    pub(crate) entity: kamu_molecule_domain::MoleculeProject,
}

impl MoleculeProjectV2 {
    pub fn new(entity: kamu_molecule_domain::MoleculeProject) -> Self {
        Self { entity }
    }

    fn as_arc(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeProjectV2 {
    const DEFAULT_ACTIVITY_EVENTS_PER_PAGE: usize = 15;

    /// System time when this project was created/updated
    pub async fn system_time(&self) -> DateTime<Utc> {
        self.entity.system_time
    }

    /// Event time when this project was created/updated
    pub async fn event_time(&self) -> DateTime<Utc> {
        self.entity.event_time
    }

    /// Symbolic name of the project
    pub async fn ipnft_symbol(&self) -> &str {
        &self.entity.ipnft_symbol
    }

    /// Unique ID of the IPNFT as `{ipnftAddress}_{ipnftTokenId}`
    pub async fn ipnft_uid(&self) -> &str {
        &self.entity.ipnft_uid
    }

    /// Address of the IPNFT contract
    pub async fn ipnft_address(&self) -> &str {
        &self.entity.ipnft_address
    }

    /// Token ID withing the IPNFT contract
    pub async fn ipnft_token_id(&self) -> U256 {
        U256::new(self.entity.ipnft_token_id.clone())
    }

    /// Project's organizational account
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_account, skip_all)]
    async fn account(&self, ctx: &Context<'_>) -> Result<Account> {
        let account = Account::from_account_id(ctx, self.entity.account_id.clone()).await?;
        Ok(account)
    }

    /// Strongly typed data room accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_data_room, skip_all)]
    async fn data_room(&self, ctx: &Context<'_>) -> Result<MoleculeDataRoom> {
        // TODO: revisit after use-case breakdown (to remove auth checks)

        let Some(dataset) =
            Dataset::try_from_ref(ctx, &self.entity.data_room_dataset_id.as_local_ref()).await?
        else {
            return Err(GqlError::Access(odf::AccessError::Unauthorized(
                "Data room dataset inaccessible".into(),
            )));
        };

        Ok(MoleculeDataRoom {
            dataset,
            project: Arc::new(self.clone()),
        })
    }

    /// Strongly typed announcements accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_announcements, skip_all)]
    async fn announcements(&self, ctx: &Context<'_>) -> Result<MoleculeAnnouncements> {
        // TODO: revisit after use-case breakdown (to remove auth checks)

        let Some(dataset) =
            Dataset::try_from_ref(ctx, &self.entity.announcements_dataset_id.as_local_ref())
                .await?
        else {
            return Err(GqlError::Access(odf::AccessError::Unauthorized(
                "Announcements dataset inaccessible".into(),
            )));
        };

        Ok(MoleculeAnnouncements {
            dataset,
            project: Arc::new(self.clone()),
        })
    }

    /// Project's activity events in reverse chronological order
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_activity, skip_all)]
    async fn activity(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeProjectActivityFilters>,
    ) -> Result<MoleculeActivityEventV2Connection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        let view_project_activities_uc =
            from_catalog_n!(ctx, dyn MoleculeViewProjectActivitiesUseCase);

        let listing = view_project_activities_uc
            .execute(
                &self.entity,
                MoleculeViewProjectActivitiesMode::LatestProjection, /* LatestSource */
                filters.map(Into::into),
                Some(PaginationOpts {
                    limit: per_page,
                    offset: page * per_page,
                }),
            )
            .await
            .map_err(|e| {
                use MoleculeViewDataRoomActivitiesError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    E::Internal(_) => e.int_err().into(),
                }
            })?;

        let self_arc = self.as_arc();

        let nodes = listing
            .list
            .into_iter()
            .map(|activity| match activity {
                MoleculeProjectActivity::DataRoomActivity(data_room_activity_entity) => {
                    let activity_type = data_room_activity_entity.activity_type;
                    let entry = MoleculeDataRoomEntry::new_from_data_room_activity_entity(
                        &self_arc,
                        data_room_activity_entity,
                    );

                    use MoleculeDataRoomFileActivityType as Type;

                    match activity_type {
                        Type::Added => MoleculeActivityEventV2::file_added(entry),
                        Type::Updated => MoleculeActivityEventV2::file_updated(entry),
                        Type::Removed => MoleculeActivityEventV2::file_removed(entry),
                    }
                }
                MoleculeProjectActivity::Announcement(announcement_activity_entity) => {
                    let entry = MoleculeAnnouncementEntry::new_from_announcement(
                        &self_arc,
                        announcement_activity_entity,
                    );
                    MoleculeActivityEventV2::announcement(entry)
                }
            })
            .collect::<Vec<_>>();

        Ok(MoleculeActivityEventV2Connection::new(
            nodes, page, per_page,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    MoleculeProjectV2,
    MoleculeProjectV2Connection,
    MoleculeProjectV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, InputObject)]
pub struct MoleculeProjectActivityFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
    pub by_access_level_rules: Option<Vec<MoleculeAccessLevelRuleInput>>,
    pub by_kinds: Option<Vec<MoleculeActivityKindInput>>,
}

impl From<MoleculeProjectActivityFilters> for kamu_molecule_domain::MoleculeActivitiesFilters {
    fn from(value: MoleculeProjectActivityFilters) -> Self {
        Self {
            by_tags: value.by_tags,
            by_categories: value.by_categories,
            by_access_levels: value.by_access_levels,
            by_access_level_rules: value
                .by_access_level_rules
                .map(|rules| rules.into_iter().map(Into::into).collect()),
            by_kinds: value
                .by_kinds
                .map(|kinds| kinds.into_iter().map(Into::into).collect()),
        }
    }
}

#[derive(Enum, Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoleculeActivityKindInput {
    File,
    Announcement,
}

impl From<MoleculeActivityKindInput> for kamu_molecule_domain::MoleculeActivityKind {
    fn from(value: MoleculeActivityKindInput) -> Self {
        use kamu_molecule_domain::MoleculeActivityKind as Domain;
        match value {
            MoleculeActivityKindInput::File => Domain::DataRoomActivity,
            MoleculeActivityKindInput::Announcement => Domain::Announcement,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
