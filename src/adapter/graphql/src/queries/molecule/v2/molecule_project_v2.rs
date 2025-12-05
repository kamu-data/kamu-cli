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
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{QueryService, auth};
use odf::metadata::OperationType;

use crate::prelude::*;
use crate::queries::molecule::v2::{
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

    async fn get_data_room_activity_events(
        &self,
        ctx: &Context<'_>,
        project: &Arc<MoleculeProjectV2>,
        filters: Option<MoleculeProjectActivityFilters>,
    ) -> Result<Vec<MoleculeActivityEventV2>> {
        // TODO: filters
        assert!(filters.is_none());

        let (query_service, rebac_dataset_registry_facade) =
            from_catalog_n!(ctx, dyn QueryService, dyn RebacDatasetRegistryFacade);

        let resolved_dataset = rebac_dataset_registry_facade
            .resolve_dataset_by_ref(
                &self.entity.data_room_dataset_id.as_local_ref(),
                auth::DatasetAction::Read,
            )
            .await
            .map_err(|e| -> GqlError {
                use RebacDatasetRefUnresolvedError as E;

                match e {
                    E::Access(e) => e.into(),
                    E::NotFound(_) | E::Internal(_) => e.int_err().into(),
                }
            })?;

        let df = query_service
            .get_data(resolved_dataset, Default::default())
            .await
            .int_err()?
            .df;

        let Some(df) = df else {
            return Ok(Vec::new());
        };

        // For any data room update, we always have two entries: -C and +C.
        // We can ignore all -C entries.
        use datafusion::logical_expr::{col, lit};

        let vocab = odf::metadata::DatasetVocabulary::default();
        let df = df
            .filter(
                col(vocab.operation_type_column.as_str())
                    .not_eq(lit(OperationType::CorrectFrom as i32)),
            )
            .int_err()?;

        // We need sorting here, since the order is important for processing.
        let df = df.sort(vec![col("offset").sort(false, false)]).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        let mut nodes = Vec::with_capacity(records.len());
        let mut record_iter = records.into_iter().peekable();

        while let Some(current) = record_iter.next() {
            let (op, entry) = MoleculeDataRoomEntry::new_from_json(current, project, &vocab)?;

            let event = match op {
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
                        let (next_op, next_entry) =
                            MoleculeDataRoomEntry::new_from_json(next.clone(), project, &vocab)?;

                        next_op == OperationType::Retract && entry.is_same_reference(&next_entry)
                    } else {
                        false
                    };

                    if is_file_updated_event {
                        // Yes, these two records represent the same update event,
                        // no need to process the next one, so we consume it.
                        let _ = record_iter.next();
                        MoleculeActivityEventV2::file_updated(entry)
                    } else {
                        MoleculeActivityEventV2::file_added(entry)
                    }
                }
                OperationType::Retract => MoleculeActivityEventV2::file_removed(entry),
                OperationType::CorrectTo => MoleculeActivityEventV2::file_updated(entry),
                OperationType::CorrectFrom => {
                    unreachable!()
                }
            };

            nodes.push(event);
        }

        Ok(nodes)
    }

    async fn get_announcement_activity_events(
        &self,
        ctx: &Context<'_>,
        project: &Arc<MoleculeProjectV2>,
        filters: Option<MoleculeProjectActivityFilters>,
    ) -> Result<Vec<MoleculeActivityEventV2>> {
        // TODO: filters
        assert!(filters.is_none());

        // TODO: extract a use-case
        //       (same at MoleculeAnnouncements::tail())

        let molecule_dataset_service =
            from_catalog_n!(ctx, dyn kamu_molecule_domain::MoleculeDatasetService);

        let (_, maybe_df) = molecule_dataset_service
            .get_project_announcements_data_frame(
                &self.entity.announcements_dataset_id,
                auth::DatasetAction::Read,
            )
            .await
            .int_err()?;

        let Some(df) = maybe_df else {
            return Ok(Vec::new());
        };

        // Sorting will be done after merge

        let records = df.collect_json_aos().await.int_err()?;

        let events = records
            .into_iter()
            .map(|record| {
                let entry = MoleculeAnnouncementEntry::from_json(project, record)?;
                Ok(MoleculeActivityEventV2::announcement(entry))
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(events)
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
        // TODO: extract use case

        // TODO: filters
        assert!(filters.is_none());

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        let self_arc = self.as_arc();

        let (mut data_room_activity_events, mut announcement_activity_events) = tokio::try_join!(
            self.get_data_room_activity_events(ctx, &self_arc, filters.clone()),
            self.get_announcement_activity_events(ctx, &self_arc, filters),
        )?;

        // Get the total count before pagination
        let total_count = data_room_activity_events.len() + announcement_activity_events.len();

        // Merge lists
        let mut events = {
            let mut v = Vec::with_capacity(total_count);
            v.append(&mut data_room_activity_events);
            v.append(&mut announcement_activity_events);
            v
        };

        // Sort by event time descending
        events.sort_unstable_by_key(|event| std::cmp::Reverse(event.event_time()));

        // Pagination
        let safe_offset = (page * per_page).min(total_count);
        events.drain(..safe_offset);
        events.truncate(per_page);

        Ok(MoleculeActivityEventV2Connection::new(
            events, page, per_page,
        ))
    }
}

page_based_connection!(
    MoleculeProjectV2,
    MoleculeProjectV2Connection,
    MoleculeProjectV2Edge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, InputObject)]
pub struct MoleculeProjectActivityFilters {
    // TODO: replace w/ real filters.
    /// This filter is provided as an example.
    by_ipnft_uids: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
