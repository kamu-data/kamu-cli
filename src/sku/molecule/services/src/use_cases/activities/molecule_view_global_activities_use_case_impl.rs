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
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::*;

use crate::MoleculeGlobalDataRoomActivitiesService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewGlobalActivitiesUseCase)]
pub struct MoleculeViewGlobalActivitiesUseCaseImpl {
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    view_global_announcements_uc: Arc<dyn MoleculeViewGlobalAnnouncementsUseCase>,
}

impl MoleculeViewGlobalActivitiesUseCaseImpl {
    async fn get_global_data_room_activities_listing(
        &self,
        molecule_subject: &LoggedAccount,
        filters: Option<MoleculeActivitiesFilters>,
    ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError> {
        // Get read access to global activities dataset
        let data_room_activities_reader = match self
            .global_data_room_activities_service
            .reader(&molecule_subject.account_name)
            .await
        {
            Ok(reader) => Ok(reader),

            // No activities dataset yet is fine, just return an empty listing
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) => {
                return Ok(MoleculeGlobalActivityListing::default());
            }

            Err(e) => Err(MoleculeDatasetErrorExt::adapt::<
                MoleculeViewGlobalActivitiesError,
            >(e)),
        }?;

        // Load raw ledger DF
        let maybe_df = data_room_activities_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewGlobalActivitiesError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_df else {
            return Ok(MoleculeGlobalActivityListing::default());
        };

        // Apply filters, if present
        let maybe_filter = filters.and_then(|f| {
            utils::molecule_fields_filter(None, f.by_tags, f.by_categories, f.by_access_levels)
        });
        let df = if let Some(filter) = maybe_filter {
            kamu_datasets_services::utils::DataFrameExtraDataFieldsFilterApplier::apply(df, filter)
                .int_err()?
        } else {
            df
        };

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        // Convert to entities
        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeDataRoomActivity::from_changelog_entry_json(record)?;
                Ok(MoleculeGlobalActivity::DataRoomActivity(entity))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeGlobalActivityListing { list, total_count })
    }

    async fn get_global_announcement_activities_listing(
        &self,
        molecule_subject: &LoggedAccount,
        filters: Option<MoleculeActivitiesFilters>,
    ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError> {
        let announcements_listing = self
            .view_global_announcements_uc
            .execute(
                molecule_subject,
                filters.map(|filters| MoleculeAnnouncementsFilters {
                    by_tags: filters.by_tags,
                    by_categories: filters.by_categories,
                    by_access_levels: filters.by_access_levels,
                }),
                None,
            )
            .await
            .map_err(|e| match e {
                MoleculeViewGlobalAnnouncementsError::Access(e) => {
                    MoleculeViewGlobalActivitiesError::Access(e)
                }
                MoleculeViewGlobalAnnouncementsError::Internal(e) => {
                    MoleculeViewGlobalActivitiesError::Internal(e)
                }
            })?;

        let list = announcements_listing
            .list
            .into_iter()
            .map(|global_announcement| {
                Ok(MoleculeGlobalActivity::Announcement(global_announcement))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeGlobalActivityListing {
            list,
            total_count: announcements_listing.total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewGlobalActivitiesUseCase for MoleculeViewGlobalActivitiesUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeViewGlobalActivitiesUseCaseImpl_execute, skip_all, fields(?pagination))]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError> {
        async fn fetch_or_empty<Fut>(
            enabled: bool,
            fut: Fut,
        ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError>
        where
            Fut: std::future::Future<
                    Output = Result<
                        MoleculeGlobalActivityListing,
                        MoleculeViewGlobalActivitiesError,
                    >,
                >,
        {
            if enabled {
                fut.await
            } else {
                Ok(MoleculeGlobalActivityListing::default())
            }
        }

        let by_kinds = filters
            .as_ref()
            .and_then(|f| f.by_kinds.as_deref())
            .unwrap_or(&[]);

        let fetch_data_room =
            by_kinds.is_empty() || by_kinds.contains(&MoleculeActivityKind::DataRoomActivity);
        let fetch_announcements =
            by_kinds.is_empty() || by_kinds.contains(&MoleculeActivityKind::Announcement);

        if !fetch_data_room && !fetch_announcements {
            return Ok(MoleculeGlobalActivityListing::default());
        }

        let data_room_fut = fetch_or_empty(
            fetch_data_room,
            self.get_global_data_room_activities_listing(molecule_subject, filters.clone()),
        );

        let announcements_fut = fetch_or_empty(
            fetch_announcements,
            self.get_global_announcement_activities_listing(molecule_subject, filters),
        );

        let (mut data_room_listing, mut announcement_activities_listing) =
            tokio::try_join!(data_room_fut, announcements_fut)?;

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

        Ok(MoleculeGlobalActivityListing { list, total_count })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
