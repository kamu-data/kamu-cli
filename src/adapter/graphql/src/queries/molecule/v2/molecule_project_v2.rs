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
    MoleculeAnnouncements,
    MoleculeDataRoom,
    MoleculeDataRoomEntry,
};
use crate::queries::{Account, CollectionEntry, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: revisit after IPNFT-less projects changes.
#[derive(Clone)]
pub struct MoleculeProjectV2 {
    pub(crate) entity: kamu_molecule_domain::MoleculeProjectEntity,
}

impl MoleculeProjectV2 {
    pub fn new(entity: kamu_molecule_domain::MoleculeProjectEntity) -> Self {
        Self { entity }
    }

    fn as_arc(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }

    async fn get_data_room_activity_events(
        &self,
        ctx: &Context<'_>,
        page: usize,
        per_page: usize,
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
        // We can ignore all -C entries, thus getting a 1:1 dataset with the expected
        // one.
        use datafusion::logical_expr::{col, lit};

        let mut df = df
            .filter(col("operation").not_eq(lit(OperationType::CorrectFrom as i32)))
            .int_err()?;

        // Sort the df by offset descending
        df = df.sort(vec![col("offset").sort(false, false)]).int_err()?;

        // Apply pagination
        df = df.limit(page * per_page, Some(per_page)).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        let self_arc = self.as_arc();
        let vocab = odf::metadata::DatasetVocabulary::default();

        let nodes = records
            .into_iter()
            .map(|mut record| -> Result<MoleculeActivityEventV2> {
                let Some(obj) = record.as_object_mut() else {
                    unreachable!()
                };
                let Some(raw_op) = obj[&vocab.operation_type_column].as_i64() else {
                    unreachable!()
                };

                use odf::metadata::OperationType;

                let op = OperationType::try_from(u8::try_from(raw_op).unwrap()).unwrap();
                let collection_entry = CollectionEntry::from_json(record).int_err()?;
                let entry =
                    MoleculeDataRoomEntry::new_from_collection_entry(&self_arc, collection_entry)?;

                let event = match op {
                    OperationType::Append => MoleculeActivityEventV2::file_added(entry),
                    OperationType::Retract => MoleculeActivityEventV2::file_removed(entry),
                    OperationType::CorrectTo => MoleculeActivityEventV2::file_updated(entry),
                    OperationType::CorrectFrom => {
                        unreachable!()
                    }
                };

                Ok(event)
            })
            .collect::<Result<_, _>>()?;

        Ok(nodes)
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
        let Some(dataset) =
            Dataset::try_from_ref(ctx, &self.entity.data_room_dataset_id.as_local_ref()).await?
        else {
            return Err(GqlError::Access(odf::AccessError::Unauthorized(
                "Dataset inaccessible".into(),
            )));
        };

        Ok(MoleculeDataRoom {
            dataset,
            project: Arc::new(self.clone()),
        })
    }

    /// Strongly typed announcements accessor
    #[tracing::instrument(level = "info", name = MoleculeProjectV2_announcements, skip_all)]
    async fn announcements(&self, _ctx: &Context<'_>) -> Result<MoleculeAnnouncements> {
        todo!()
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
        // TODO: filters
        assert!(filters.is_none());

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ACTIVITY_EVENTS_PER_PAGE);

        // TODO: PERF: share data room df with data_room() method (lazy initialization)

        let events = self
            .get_data_room_activity_events(ctx, page, per_page, filters)
            .await?;

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

#[derive(InputObject)]
pub struct MoleculeProjectActivityFilters {
    // TODO: replace w/ real filters.
    /// This filter is provided as an example.
    by_ipnft_uids: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
