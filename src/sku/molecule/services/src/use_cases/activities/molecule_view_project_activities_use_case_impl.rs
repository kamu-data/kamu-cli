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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_molecule_domain::*;

use crate::{MoleculeDatasetAccessorFactory, utils};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewProjectActivitiesUseCase)]
pub struct MoleculeViewProjectActivitiesUseCaseImpl {
    accessor_factory: Arc<MoleculeDatasetAccessorFactory>,
    view_project_announcements_uc: Arc<dyn MoleculeViewProjectAnnouncementsUseCase>,
}

impl MoleculeViewProjectActivitiesUseCaseImpl {
    async fn get_data_room_activities_listing(
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

        let df = if let Some(filters) = filters {
            utils::apply_molecule_filters_to_df(
                df,
                None,
                filters.by_tags,
                filters.by_categories,
                filters.by_access_levels,
                filters.by_access_level_rules,
            )?
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
        let mut last_retract_entry: Option<MoleculeDataRoomEntry> = None;

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
                OperationType::Retract => {
                    last_retract_entry = Some(entry.clone());
                    MoleculeDataRoomFileActivityType::Removed
                }
                OperationType::CorrectTo => {
                    // Ignore synthetic corrections that only change `change_by` right before
                    // retract
                    if op == OperationType::CorrectTo {
                        let next_is_only_change_by_diff = if let Some(next) = record_iter.peek() {
                            let (_, _, next_entry) =
                                MoleculeDataRoomEntry::from_changelog_entry_json(
                                    next.clone(),
                                    &vocab,
                                )?;

                            entry.is_only_change_by_diff(&next_entry)
                        } else {
                            false
                        };

                        let match_retract_system_time = last_retract_entry
                            .as_ref()
                            .map(|prev| prev.system_time == entry.system_time)
                            .unwrap_or(false);

                        if next_is_only_change_by_diff || match_retract_system_time {
                            continue;
                        }
                    }
                    MoleculeDataRoomFileActivityType::Updated
                }
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

    async fn get_announcement_activities_listing(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeActivitiesFilters>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError> {
        let announcements_listing = self
            .view_project_announcements_uc
            .execute(
                molecule_project,
                filters.map(|filters| MoleculeAnnouncementsFilters {
                    by_tags: filters.by_tags,
                    by_categories: filters.by_categories,
                    by_access_levels: filters.by_access_levels,
                    by_access_level_rules: filters.by_access_level_rules,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewProjectActivitiesUseCase for MoleculeViewProjectActivitiesUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeViewProjectActivitiesUseCaseImpl_execute, skip_all, fields(?pagination))]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeActivitiesFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError> {
        // Helper to conditionally fetch data
        async fn fetch_or_empty<Fut>(
            enabled: bool,
            fut: Fut,
        ) -> Result<MoleculeProjectActivityListing, MoleculeViewDataRoomActivitiesError>
        where
            Fut: std::future::Future<
                    Output = Result<
                        MoleculeProjectActivityListing,
                        MoleculeViewDataRoomActivitiesError,
                    >,
                >,
        {
            if enabled {
                fut.await
            } else {
                Ok(MoleculeProjectActivityListing::default())
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
            return Ok(MoleculeProjectActivityListing::default());
        }

        let data_room_fut = fetch_or_empty(
            fetch_data_room,
            self.get_data_room_activities_listing(molecule_project, filters.clone()),
        );

        let announcements_fut = fetch_or_empty(
            fetch_announcements,
            self.get_announcement_activities_listing(molecule_project, filters.clone()),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
