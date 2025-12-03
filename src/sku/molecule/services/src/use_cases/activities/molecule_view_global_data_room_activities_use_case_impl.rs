// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::LoggedAccount;
use kamu_core::auth;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewGlobalDataRoomActivitiesUseCase)]
pub struct MoleculeViewGlobalDataRoomActivitiesUseCaseImpl {
    molecule_dataset_service: Arc<dyn MoleculeDatasetService>,
}

impl MoleculeViewGlobalDataRoomActivitiesUseCaseImpl {
    async fn get_global_data_room_activities_listing(
        &self,
        molecule_subject: &LoggedAccount,
    ) -> Result<MoleculeDataRoomActivityListing, MoleculeViewDataRoomActivitiesError> {
        let (_, maybe_df) = self
            .molecule_dataset_service
            .get_global_data_room_activity_data_frame(
                &molecule_subject.account_name,
                auth::DatasetAction::Read,
                // TODO: try to create once as start-up job?
                true,
            )
            .await?;

        let Some(df) = maybe_df else {
            return Ok(MoleculeDataRoomActivityListing::default());
        };

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeDataRoomActivityEntity::from_json(record)?;
                Ok(MoleculeGlobalActivity::DataRoomActivity(entity))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeDataRoomActivityListing { list, total_count })
    }

    async fn get_global_announcement_activities_listing(
        &self,
        molecule_subject: &LoggedAccount,
    ) -> Result<MoleculeDataRoomActivityListing, MoleculeViewDataRoomActivitiesError> {
        let (_, maybe_df) = self
            .molecule_dataset_service
            .get_global_announcements_data_frame(
                &molecule_subject.account_name,
                auth::DatasetAction::Read,
                // TODO: try to create once as start-up job?
                true,
            )
            .await?;

        let Some(df) = maybe_df else {
            return Ok(MoleculeDataRoomActivityListing::default());
        };

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeGlobalAnnouncementRecord::from_json(record)?;
                Ok(MoleculeGlobalActivity::Announcement(entity))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeDataRoomActivityListing { list, total_count })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewGlobalDataRoomActivitiesUseCase
    for MoleculeViewGlobalDataRoomActivitiesUseCaseImpl
{
    #[tracing::instrument(level = "debug", name = MoleculeViewGlobalDataRoomActivitiesUseCaseImpl_execute, skip_all, fields(?pagination))]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeDataRoomActivityListing, MoleculeViewDataRoomActivitiesError> {
        let (mut data_room_listing, mut announcement_activities_listing) = tokio::try_join!(
            self.get_global_data_room_activities_listing(molecule_subject),
            self.get_global_announcement_activities_listing(molecule_subject),
        )?;

        // Get the total count before pagination
        let total_count =
            data_room_listing.total_count + announcement_activities_listing.total_count;

        // Merge lists
        let mut list = {
            let mut v = Vec::with_capacity(total_count);
            v.append(&mut data_room_listing.list);
            v.append(&mut announcement_activities_listing.list);
            v
        };

        // Sort by event time descending
        list.sort_unstable_by_key(|item| std::cmp::Reverse(item.event_time()));

        // Pagination
        if let Some(pagination) = pagination {
            let safe_offset = pagination.offset.min(total_count);
            list.drain(..safe_offset);
            list.truncate(pagination.limit);
        }

        Ok(MoleculeDataRoomActivityListing { list, total_count })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
