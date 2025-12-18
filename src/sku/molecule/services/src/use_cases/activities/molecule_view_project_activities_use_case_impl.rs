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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_molecule_domain::{
    molecule_activity_full_text_search_schema as activity_schema,
    molecule_announcement_full_text_search_schema as announcement_schema,
    *,
};
use kamu_search::*;

use crate::{MoleculeDatasetAccessorFactory, map_molecule_activities_filters_to_search};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewProjectActivitiesUseCase)]
pub struct MoleculeViewProjectActivitiesUseCaseImpl {
    catalog: dill::Catalog,
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
    view_project_announcements_uc: Arc<dyn MoleculeViewProjectAnnouncementsUseCase>,
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

impl MoleculeViewProjectActivitiesUseCaseImpl {
    async fn project_activities_from_source(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError> {
        let (mut data_room_listing, mut announcement_activities_listing) = tokio::try_join!(
            self.data_room_activities_listing_from_source(molecule_project, filters.clone()),
            self.announcement_activities_listing_from_source(molecule_project, filters),
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
        list.sort_unstable_by_key(|item: &MoleculeProjectActivity| {
            std::cmp::Reverse(item.event_time())
        });

        // Pagination
        if let Some(pagination) = pagination {
            let safe_offset = pagination.offset.min(total_count);
            list.drain(..safe_offset);
            list.truncate(pagination.limit);
        }

        Ok(MoleculeProjectActivityListing { list, total_count })
    }

    async fn data_room_activities_listing_from_source(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeActivitiesFilters>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError> {
        // Get read access to data room
        let data_room_reader = self
            .accessor_factory
            .reader(&molecule_project.data_room_dataset_id.as_local_ref())
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewDataRoomActivitiesError>)?;

        let maybe_raw_ledger_df = data_room_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewDataRoomActivitiesError>)?;

        let Some(df) = maybe_raw_ledger_df else {
            return Ok(MoleculeProjectActivityListing::default());
        };

        let maybe_filter = filters.and_then(|f| {
            utils::molecule_fields_filter(None, f.by_tags, f.by_categories, f.by_access_levels)
        });
        let df = if let Some(filter) = maybe_filter {
            kamu_datasets_services::utils::DataFrameExtraDataFieldsFilterApplier::apply(df, filter)
                .int_err()?
        } else {
            df
        };

        //

        use datafusion::logical_expr::{col, lit};
        use odf::metadata::OperationType;

        let vocab = odf::metadata::DatasetVocabulary::default();
        let df = df
            .filter(
                // We ignore all records with `molecule_access_level == null` as those are pre
                // V2 migration
                col("molecule_access_level").is_not_null().and(
                    // For any data room update, we always have two entries: -C and +C.
                    // We can ignore all -C entries.
                    col(vocab.operation_type_column.as_str())
                        .not_eq(lit(OperationType::CorrectFrom as i32)),
                ),
            )
            .int_err()?;

        // We need sorting here, since the order is important for processing.
        let df = df.sort(vec![col("offset").sort(false, false)]).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        // For any data room update, we always have two entries: -C and +C.
        let mut nodes = Vec::with_capacity(records.len());
        let mut record_iter = records.into_iter().peekable();

        while let Some(current) = record_iter.next() {
            let (offset, op, entry) =
                MoleculeDataRoomEntry::from_changelog_entry_json(current, &vocab)?;

            let activity_type = match op {
                OperationType::Append => {
                    // NOTE: Reverse order due to ORDER BY.
                    //
                    // If the next entry is equivalent to the current
                    // one, then it's a file update.
                    //
                    // More details: UpdateCollectionEntriesUseCaseImpl

                    // TODO: extract use case based on common logic like
                    //       UpdateCollectionEntriesUseCaseImpl.
                    let is_file_updated_event = if let Some(next) = record_iter.peek() {
                        let (_, next_op, next_entry) =
                            MoleculeDataRoomEntry::from_changelog_entry_json(next.clone(), &vocab)?;

                        next_op == OperationType::Retract && entry.reference == next_entry.reference
                    } else {
                        false
                    };

                    if is_file_updated_event {
                        // Yes, these two records represent the same update event,
                        // no need to process the next one, so we consume it.
                        let _ = record_iter.next();
                        MoleculeDataRoomFileActivityType::Updated
                    } else {
                        MoleculeDataRoomFileActivityType::Added
                    }
                }
                OperationType::Retract => MoleculeDataRoomFileActivityType::Removed,
                OperationType::CorrectTo => MoleculeDataRoomFileActivityType::Updated,
                OperationType::CorrectFrom => {
                    unreachable!()
                }
            };

            nodes.push(MoleculeProjectActivity::DataRoomActivity(
                MoleculeDataRoomActivity::from_data_room_operation(
                    offset,
                    activity_type,
                    entry,
                    molecule_project.ipnft_uid.clone(),
                ),
            ));
        }

        Ok(MoleculeProjectActivityListing {
            total_count: nodes.len(),
            list: nodes,
        })
    }

    async fn announcement_activities_listing_from_source(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeActivitiesFilters>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError> {
        let announcements_listing = self
            .view_project_announcements_uc
            .execute(
                molecule_project,
                MoleculeViewProjectAnnouncementsMode::LatestSource,
                filters.map(|filters| MoleculeAnnouncementsFilters {
                    by_tags: filters.by_tags,
                    by_categories: filters.by_categories,
                    by_access_levels: filters.by_access_levels,
                }),
                None,
            )
            .await
            .map_err(|e| {
                use MoleculeViewProjectAnnouncementsError as E;
                match e {
                    E::Access(e) => MoleculeViewDataRoomActivitiesError::Access(e),
                    E::Internal(_) => MoleculeViewDataRoomActivitiesError::Internal(e.int_err()),
                }
            })?;

        let list = announcements_listing
            .list
            .into_iter()
            .map(MoleculeProjectActivity::Announcement)
            .collect::<Vec<_>>();

        Ok(MoleculeProjectActivityListing {
            list,
            total_count: announcements_listing.total_count,
        })
    }

    async fn project_activities_from_search(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError> {
        let ctx = FullTextSearchContext {
            catalog: &self.catalog,
        };

        let filter = {
            let mut and_clauses = vec![];

            // ipnft_uid equality
            and_clauses.push(field_eq_str(
                activity_schema::FIELD_IPNFT_UID,
                &molecule_project.ipnft_uid,
            ));

            // filters by categories, tags, access levels
            if let Some(filters) = filters {
                and_clauses.extend(map_molecule_activities_filters_to_search(filters));
            }

            FullTextSearchFilterExpr::and_clauses(and_clauses)
        };

        let search_results = self
            .full_text_search_service
            .search(
                ctx,
                FullTextSearchRequest {
                    query: None, // no textual query, just filtering
                    entity_schemas: vec![
                        // Query simultaneously both activities and announcements.
                        // This should give mixed-type listing sorted by time desc
                        announcement_schema::SCHEMA_NAME,
                        activity_schema::SCHEMA_NAME,
                    ],
                    source: FullTextSearchRequestSourceSpec::All,
                    filter: Some(filter),
                    // this sorting field is available in both schemas
                    sort: sort!(announcement_schema::FIELD_SYSTEM_TIME, desc),
                    page: pagination.into(),
                    options: FullTextSearchOptions::default(),
                },
            )
            .await?;

        Ok(MoleculeProjectActivityListing {
            total_count: usize::try_from(search_results.total_hits).unwrap(),
            list: search_results
                .hits
                .into_iter()
                .map(|hit| match hit.schema_name {
                    activity_schema::SCHEMA_NAME => {
                        MoleculeDataRoomActivity::from_search_index_json(hit.source)
                            .map(MoleculeProjectActivity::DataRoomActivity)
                    }
                    announcement_schema::SCHEMA_NAME => {
                        MoleculeAnnouncement::from_search_index_json(hit.id, hit.source)
                            .map(MoleculeProjectActivity::Announcement)
                    }
                    _ => Err(InternalError::new(format!(
                        "Unknown schema name: {schema_name}",
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
impl MoleculeViewProjectActivitiesUseCase for MoleculeViewProjectActivitiesUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeViewProjectActivitiesUseCaseImpl_execute, skip_all, fields(?pagination))]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        mode: MoleculeViewProjectActivitiesMode,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError> {
        match mode {
            MoleculeViewProjectActivitiesMode::LatestSource => {
                self.project_activities_from_source(molecule_project, filters, pagination)
                    .await
            }

            MoleculeViewProjectActivitiesMode::LatestProjection => {
                self.project_activities_from_search(molecule_project, filters, pagination)
                    .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
