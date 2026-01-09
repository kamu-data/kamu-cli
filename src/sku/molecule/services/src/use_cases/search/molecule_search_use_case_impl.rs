// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::PaginationOpts;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::*;
use odf::utils::data::DataFrameExt;

use crate::{MoleculeAnnouncementsService, MoleculeGlobalDataRoomActivitiesService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeSearchUseCase)]
pub struct MoleculeSearchUseCaseImpl {
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
}

impl MoleculeSearchUseCaseImpl {
    fn project_global_data_room_activities(
        ledger: DataFrameExt,
    ) -> Result<DataFrameExt, DataFusionError> {
        // TODO: PERF: Re-assess implementation as it may be sub-optimal
        const RANK_COLUMN: &str = "__rank";

        let vocab = odf::metadata::DatasetVocabulary::default();
        // NOTE: Unlike other places, we set PK not by `path`, but by `ref`.
        let primary_key = ["ipnft_uid", "ref"]
            .into_iter()
            .map(|name| col(Column::from_name(name)))
            .collect::<Vec<_>>();

        let projection = ledger
            .window(vec![
                datafusion::functions_window::row_number::row_number()
                    .partition_by(primary_key)
                    .order_by(vec![
                        col(Column::from_name(&vocab.offset_column)).sort(false, false),
                    ])
                    .build()?
                    .alias(RANK_COLUMN),
            ])?
            .filter(
                col(Column::from_name(RANK_COLUMN))
                    .eq(lit(1))
                    .and(col(Column::from_name("activity_type")).not_eq(lit("removed"))),
            )?
            .without_columns(&[RANK_COLUMN])?;

        Ok(projection)
    }

    async fn get_global_data_room_activities_listing(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        search_entity_kinds: &HashSet<MoleculeSearchEntityKind>,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        if !search_entity_kinds.contains(&MoleculeSearchEntityKind::DataRoomActivity) {
            return Ok(MoleculeSearchHitsListing::empty());
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
                return Ok(MoleculeSearchHitsListing::empty());
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
            return Ok(MoleculeSearchHitsListing::empty());
        };

        // Filtering
        let df = if let Some(filters) = filters {
            crate::utils::apply_molecule_filters_to_df(
                df,
                filters.by_ipnft_uids,
                filters.by_tags,
                filters.by_categories,
                filters.by_access_levels,
                filters.by_access_level_rules,
            )?
        } else {
            df
        };

        let df = Self::project_global_data_room_activities(df).int_err()?;

        let pattern = lit(format!("%{prompt}%"));
        let df = df.filter(col("description").ilike(pattern)).int_err()?;

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        // Convert to entities
        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeDataRoomActivity::from_changelog_entry_json(record)?;
                Ok(MoleculeSearchHit::DataRoomActivity(entity))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeSearchHitsListing { list, total_count })
    }

    async fn get_global_announcement_activities_listing(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        search_entity_kinds: &HashSet<MoleculeSearchEntityKind>,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        if !search_entity_kinds.contains(&MoleculeSearchEntityKind::Announcement) {
            return Ok(MoleculeSearchHitsListing::empty());
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
                return Ok(MoleculeSearchHitsListing::empty());
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
            return Ok(MoleculeSearchHitsListing::empty());
        };

        // Filtering
        let df = if let Some(filters) = filters {
            crate::utils::apply_molecule_filters_to_df(
                df,
                filters.by_ipnft_uids,
                filters.by_tags,
                filters.by_categories,
                filters.by_access_levels,
                filters.by_access_level_rules,
            )?
        } else {
            df
        };

        let pattern = lit(format!("%{prompt}%"));
        let df = df
            .filter(
                col("headline")
                    .ilike(pattern.clone())
                    .or(col("body").ilike(pattern)),
            )
            .int_err()?;

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeGlobalAnnouncement::from_changelog_entry_json(record)?;
                Ok(MoleculeSearchHit::Announcement(entity))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeSearchHitsListing { list, total_count })
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
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        let search_entity_kinds = utils::get_search_entity_kinds(filters.as_ref());

        let (mut data_room_listing, mut announcement_activities_listing) = tokio::try_join!(
            self.get_global_data_room_activities_listing(
                molecule_subject,
                prompt,
                filters.clone(),
                &search_entity_kinds
            ),
            self.get_global_announcement_activities_listing(
                molecule_subject,
                prompt,
                filters,
                &search_entity_kinds
            ),
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

        Ok(MoleculeSearchHitsListing { list, total_count })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
