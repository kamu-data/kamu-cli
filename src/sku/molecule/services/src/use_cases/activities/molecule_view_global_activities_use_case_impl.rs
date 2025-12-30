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
use kamu_molecule_domain::{
    molecule_activity_search_schema as activity_schema,
    molecule_announcement_search_schema as announcement_schema,
    molecule_search_schema_common as molecule_schema,
    *,
};
use kamu_search::*;

use crate::{MoleculeGlobalDataRoomActivitiesService, map_molecule_activities_filters_to_search};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewGlobalActivitiesUseCase)]
pub struct MoleculeViewGlobalActivitiesUseCaseImpl {
    catalog: dill::Catalog,
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    view_global_announcements_uc: Arc<dyn MoleculeViewGlobalAnnouncementsUseCase>,
    search_service: Arc<dyn kamu_search::SearchService>,
}

impl MoleculeViewGlobalActivitiesUseCaseImpl {
    async fn global_activities_from_source(
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
            self.global_data_room_activities_listing_from_source(molecule_subject, filters.clone()),
        );

        let announcements_fut = fetch_or_empty(
            fetch_announcements,
            self.global_announcement_activities_listing_from_source(molecule_subject, filters),
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

    async fn global_data_room_activities_listing_from_source(
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

    async fn global_announcement_activities_listing_from_source(
        &self,
        molecule_subject: &LoggedAccount,
        filters: Option<MoleculeActivitiesFilters>,
    ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError> {
        let announcements_listing = self
            .view_global_announcements_uc
            .execute(
                molecule_subject,
                MoleculeViewGlobalAnnouncementsMode::LatestSource,
                if let Some(filters) = filters {
                    Some(MoleculeAnnouncementsFilters {
                        by_tags: filters.by_tags,
                        by_categories: filters.by_categories,
                        by_access_levels: filters.by_access_levels,
                    })
                } else {
                    None
                },
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

    async fn global_activities_from_search(
        &self,
        molecule_subject: &LoggedAccount,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError> {
        let ctx = SearchContext {
            catalog: &self.catalog,
        };

        let entity_schemas = {
            if let Some(by_kinds) = filters.as_ref().and_then(|f| f.by_kinds.as_deref())
                && !by_kinds.is_empty()
            {
                let mut schemas = vec![];
                if by_kinds.contains(&MoleculeActivityKind::DataRoomActivity) {
                    schemas.push(activity_schema::SCHEMA_NAME);
                }
                if by_kinds.contains(&MoleculeActivityKind::Announcement) {
                    schemas.push(announcement_schema::SCHEMA_NAME);
                }
                schemas
            } else {
                vec![
                    activity_schema::SCHEMA_NAME,
                    announcement_schema::SCHEMA_NAME,
                ]
            }
        };

        let filter = {
            let mut and_clauses = vec![];

            // molecule_account_id equality
            and_clauses.push(field_eq_str(
                molecule_schema::fields::MOLECULE_ACCOUNT_ID,
                molecule_subject.account_id.to_string().as_str(),
            ));

            // filters by categories, tags, access levels
            if let Some(filters) = filters {
                and_clauses.extend(map_molecule_activities_filters_to_search(filters));
            }

            SearchFilterExpr::and_clauses(and_clauses)
        };

        let search_results = self
            .search_service
            .search(
                ctx,
                SearchRequest {
                    query: None, // no textual query, just filtering
                    entity_schemas,
                    source: SearchRequestSourceSpec::All,
                    filter: Some(filter),
                    sort: sort!(molecule_schema::fields::SYSTEM_TIME, desc),
                    page: pagination.into(),
                    options: SearchOptions::default(),
                },
            )
            .await?;

        Ok(MoleculeGlobalActivityListing {
            total_count: usize::try_from(search_results.total_hits).unwrap(),
            list: search_results
                .hits
                .into_iter()
                .map(|hit| match hit.schema_name {
                    activity_schema::SCHEMA_NAME => {
                        MoleculeDataRoomActivity::from_search_index_json(hit.source)
                            .map(MoleculeGlobalActivity::DataRoomActivity)
                    }
                    announcement_schema::SCHEMA_NAME => {
                        MoleculeGlobalAnnouncement::from_search_index_json(hit.id, hit.source)
                            .map(MoleculeGlobalActivity::Announcement)
                    }
                    _ => Err(InternalError::new(format!(
                        "Unexpected schema name: {schema_name}",
                        schema_name = hit.schema_name
                    ))),
                })
                .collect::<Result<Vec<_>, InternalError>>()?,
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
        mode: MoleculeViewGlobalActivitiesMode,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalActivityListing, MoleculeViewGlobalActivitiesError> {
        match mode {
            MoleculeViewGlobalActivitiesMode::LatestSource => {
                self.global_activities_from_source(molecule_subject, filters, pagination)
                    .await
            }

            MoleculeViewGlobalActivitiesMode::LatestProjection => {
                self.global_activities_from_search(molecule_subject, filters, pagination)
                    .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
