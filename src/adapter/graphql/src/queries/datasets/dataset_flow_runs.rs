// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use database_common::PaginationOpts;
use futures::TryStreamExt;
use kamu_accounts::AccountService;
use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use crate::mutations::{FlowInDatasetError, FlowNotFound, check_if_flow_belongs_to_dataset};
use crate::prelude::*;
use crate::queries::{Account, DatasetRequestState, Flow};

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
    async fn get_flow(&self, ctx: &Context<'_>, flow_id: FlowID) -> Result<GetFlowResult> {
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
    async fn list_flows(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<DatasetFlowFilters>,
    ) -> Result<FlowConnection> {
        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let maybe_filters = match filters {
            Some(filters) => Some(kamu_flow_system::FlowFilters {
                by_flow_type: filters
                    .by_flow_type
                    .map(|flow_type| map_dataset_flow_type(flow_type).to_string()),
                by_flow_status: filters.by_status.map(Into::into),
                by_initiator: match filters.by_initiator {
                    Some(initiator_filter) => match initiator_filter {
                        InitiatorFilterInput::System(_) => {
                            Some(kamu_flow_system::InitiatorFilter::System)
                        }
                        InitiatorFilterInput::Accounts(account_ids) => {
                            Some(kamu_flow_system::InitiatorFilter::Account(
                                account_ids
                                    .into_iter()
                                    .map(Into::into)
                                    .collect::<HashSet<_>>(),
                            ))
                        }
                    },
                    None => None,
                },
            }),
            None => None,
        };

        let flows_state_listing = flow_query_service
            .list_scoped_flows(
                afs::FlowScopeDataset::query_for_single_dataset(
                    self.dataset_request_state.dataset_id(),
                ),
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
    async fn list_flow_initiators(&self, ctx: &Context<'_>) -> Result<AccountConnection> {
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
enum GetFlowResult {
    Success(GetFlowSuccess),
    NotFound(FlowNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
struct GetFlowSuccess {
    pub flow: Flow,
}

#[ComplexObject]
impl GetFlowSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct DatasetFlowFilters {
    by_flow_type: Option<DatasetFlowType>,
    by_status: Option<FlowStatus>,
    by_initiator: Option<InitiatorFilterInput>,
}

#[derive(OneofObject, Debug)]
pub enum InitiatorFilterInput {
    System(bool),
    Accounts(Vec<AccountID<'static>>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
