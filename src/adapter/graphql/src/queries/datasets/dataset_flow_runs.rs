// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use futures::TryStreamExt;
use kamu_accounts::AccountService;
use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use crate::mutations::{FlowInDatasetError, FlowNotFound, check_if_flow_belongs_to_dataset};
use crate::prelude::*;
use crate::queries::{
    Account,
    DatasetRequestState,
    Flow,
    prepare_flows_filter_by_initiator,
    prepare_flows_filter_by_types,
    prepare_flows_scope_query,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowRuns<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetFlowRuns<'a> {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    #[tracing::instrument(level = "info", name = DatasetFlowRuns_get_flow, skip_all, fields(%flow_id))]
    pub async fn get_flow(&self, ctx: &Context<'_>, flow_id: FlowID) -> Result<GetFlowResult> {
        if let Some(error) = check_if_flow_belongs_to_dataset(
            ctx,
            flow_id,
            &self.dataset_request_state.dataset_handle().id,
        )
        .await?
        {
            return Ok(match error {
                FlowInDatasetError::NotFound(e) => GetFlowResult::NotFound(e),
            });
        }

        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let flow_state = flow_query_service
            .get_flow(flow_id.into())
            .await
            .int_err()?;

        Ok(GetFlowResult::Success(GetFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    #[tracing::instrument(level = "info", name = DatasetFlowRuns_list_flows, skip_all, fields(?page, ?per_page))]
    pub async fn list_flows(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<DatasetFlowFilters>,
    ) -> Result<FlowConnection> {
        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let dataset_id = self.dataset_request_state.dataset_id();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let scope_query = prepare_flows_scope_query(
            filters
                .as_ref()
                .map(|f| f.by_process_type.as_ref())
                .unwrap_or_default(),
            &[dataset_id],
        );

        let maybe_filters = match filters {
            Some(filters) => {
                let by_initiator = prepare_flows_filter_by_initiator(filters.by_initiator);

                let by_flow_types = prepare_flows_filter_by_types(filters.by_process_type.as_ref());

                Some(kamu_flow_system::FlowFilters {
                    by_flow_types,
                    by_flow_status: filters.by_status.map(Into::into),
                    by_initiator,
                })
            }
            None => None,
        };

        let flows_state_listing = flow_query_service
            .list_scoped_flows(
                scope_query,
                maybe_filters.unwrap_or_default(),
                PaginationOpts::from_page(page, per_page),
            )
            .await
            .int_err()?;

        let matched_flow_states: Vec<_> = flows_state_listing.matched_stream.try_collect().await?;
        let matched_flows = Flow::build_batch(matched_flow_states, ctx).await?;

        Ok(FlowConnection::new(
            matched_flows,
            page,
            per_page,
            flows_state_listing.total_count,
        ))
    }

    #[tracing::instrument(level = "info", name = DatasetFlowRuns_list_flow_initiators, skip_all)]
    pub async fn list_flow_initiators(&self, ctx: &Context<'_>) -> Result<AccountConnection> {
        let (flow_query_service, account_service) =
            from_catalog_n!(ctx, dyn fs::FlowQueryService, dyn AccountService);

        let flow_initiator_ids: Vec<_> = flow_query_service
            .list_scoped_flow_initiators(afs::FlowScopeDataset::query_for_single_dataset(
                self.dataset_request_state.dataset_id(),
            ))
            .await
            .int_err()?
            .matched_stream
            .try_collect()
            .await?;

        let flow_initiator_ids_refs = flow_initiator_ids.iter().collect::<Vec<_>>();
        let matched_flow_initiators: Vec<_> = account_service
            .get_accounts_by_ids(&flow_initiator_ids_refs)
            .await?
            .found
            .into_iter()
            .map(Account::from_account)
            .collect();

        let total_count = matched_flow_initiators.len();

        Ok(AccountConnection::new(
            matched_flow_initiators,
            0,
            total_count,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(Flow, FlowConnection, FlowEdge);
page_based_connection!(Account, AccountConnection, AccountEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum GetFlowResult {
    Success(GetFlowSuccess),
    NotFound(FlowNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct GetFlowSuccess {
    pub flow: Flow,
}

#[ComplexObject]
impl GetFlowSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub(crate) struct DatasetFlowFilters {
    by_status: Option<FlowStatus>,
    by_initiator: Option<InitiatorFilterInput>,
    by_process_type: Option<FlowProcessTypeFilterInput>,
}

#[derive(OneofObject, Debug, Clone)]
pub(crate) enum InitiatorFilterInput {
    System(bool),
    Accounts(Vec<AccountID<'static>>),
}

#[derive(OneofObject, Debug, Clone)]
pub(crate) enum FlowProcessTypeFilterInput {
    Primary(FlowProcessTypePrimaryFilterInput),
    Webhooks(FlowProcessTypeWebhooksFilterInput),
}

#[derive(InputObject, Debug, Clone)]
pub(crate) struct FlowProcessTypePrimaryFilterInput {
    pub(crate) by_flow_types: Option<Vec<DatasetFlowType>>,
}

#[derive(InputObject, Debug, Clone)]
pub(crate) struct FlowProcessTypeWebhooksFilterInput {
    pub(crate) subscription_ids: Option<Vec<WebhookSubscriptionID>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
