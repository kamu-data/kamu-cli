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
use datafusion::logical_expr::{col, lit};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::*;

use crate::{MoleculeAnnouncementsService, MoleculeGlobalDataRoomActivitiesService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeSearchUseCase)]
pub struct MoleculeSearchUseCaseImpl {
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
}

impl MoleculeSearchUseCaseImpl {
    async fn get_global_data_room_activities_listing(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
    ) -> Result<MoleculeSearchFoundItemsListing, MoleculeSearchError> {
        use MoleculeSearchType as Type;

        match utils::get_search_result_type(filters.as_ref()) {
            Type::OnlyDataRoomActivities | Type::DataRoomActivitiesAndAnnouncements => {
                /* continue */
            }
            Type::OnlyAnnouncements => return Ok(MoleculeSearchFoundItemsListing::default()),
        }

        // Get read access to global activities dataset
        let data_room_activities_reader = match self
            .global_data_room_activities_service
            .reader(&molecule_subject.account_name)
            .await
        {
            Ok(reader) => Ok(reader),

            // No activities dataset yet is fine, just return an empty listing
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) => {
                return Ok(MoleculeSearchFoundItemsListing::default());
            }

            Err(e) => Err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>(e)),
        }?;

        // Load raw ledger DF
        let maybe_df = data_room_activities_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_df else {
            return Ok(MoleculeSearchFoundItemsListing::default());
        };

        // Filtering
        let maybe_filter = filters.and_then(|f| {
            utils::molecule_fields_filter(
                f.by_ipnft_uids,
                f.by_tags,
                f.by_categories,
                f.by_access_levels,
            )
        });
        let df = if let Some(filter) = maybe_filter {
            kamu_datasets_services::utils::DataFrameExtraDataFieldsFilterApplier::apply(df, filter)
                .int_err()?
        } else {
            df
        };

        use datafusion::logical_expr::{col, lit};

        let df = df.filter(col("description").ilike(lit(prompt))).int_err()?;

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        // Convert to entities
        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeDataRoomActivity::from_json(record)?;
                Ok(MoleculeSearchFoundItem::DataRoomActivity(entity))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeSearchFoundItemsListing { list, total_count })
    }

    async fn get_global_announcement_activities_listing(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
    ) -> Result<MoleculeSearchFoundItemsListing, MoleculeSearchError> {
        use MoleculeSearchType as Type;

        match utils::get_search_result_type(filters.as_ref()) {
            Type::OnlyAnnouncements | Type::DataRoomActivitiesAndAnnouncements => { /* continue */ }
            Type::OnlyDataRoomActivities => return Ok(MoleculeSearchFoundItemsListing::default()),
        }

        // Get read access to global announcements dataset
        let announcements_reader = match self
            .announcements_service
            .global_reader(&molecule_subject.account_name)
            .await
        {
            Ok(reader) => Ok(reader),

            // No announcements dataset yet is fine, just return empty listing
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) => {
                return Ok(MoleculeSearchFoundItemsListing::default());
            }

            Err(e) => Err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>(e)),
        }?;

        // Load raw ledger DF
        let maybe_df = announcements_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_df else {
            return Ok(MoleculeSearchFoundItemsListing::default());
        };

        // Filtering
        let maybe_filter = filters.and_then(|f| {
            utils::molecule_fields_filter(
                f.by_ipnft_uids,
                f.by_tags,
                f.by_categories,
                f.by_access_levels,
            )
        });
        let df = if let Some(filter) = maybe_filter {
            kamu_datasets_services::utils::DataFrameExtraDataFieldsFilterApplier::apply(df, filter)
                .int_err()?
        } else {
            df
        };

        let df = df
            .filter(
                col("headline")
                    .ilike(lit(prompt))
                    .or(col("body"))
                    .ilike(lit(prompt)),
            )
            .int_err()?;

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeGlobalAnnouncement::from_json(record)?;
                Ok(MoleculeSearchFoundItem::Announcement(entity))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeSearchFoundItemsListing { list, total_count })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeSearchUseCase for MoleculeSearchUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeSearchUseCaseImpl_execute, skip_all, fields(?pagination))]
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeSearchFoundItemsListing, MoleculeSearchError> {
        let (mut data_room_listing, mut announcement_activities_listing) = tokio::try_join!(
                self.get_global_data_room_activities_listing(
                    molecule_subject,
                    prompt,
                    filters.clone(),
                ),
                self.get_global_announcement_activities_listing(molecule_subject, prompt, filters),
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

        Ok(MoleculeSearchFoundItemsListing { list, total_count })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
