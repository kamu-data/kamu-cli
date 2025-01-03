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
use kamu_accounts::AuthenticationService;
use {kamu_flow_system as fs, opendatafabric as odf};

use crate::mutations::{check_if_flow_belongs_to_dataset, FlowInDatasetError, FlowNotFound};
use crate::prelude::*;
use crate::queries::{Account, Flow};
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowRuns {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetFlowRuns {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    async fn get_flow(&self, ctx: &Context<'_>, flow_id: FlowID) -> Result<GetFlowResult> {
        utils::check_dataset_read_access(ctx, &self.dataset_handle).await?;

        if let Some(error) =
            check_if_flow_belongs_to_dataset(ctx, flow_id, &self.dataset_handle).await?
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

    async fn list_flows(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<DatasetFlowFilters>,
    ) -> Result<FlowConnection> {
        utils::check_dataset_read_access(ctx, &self.dataset_handle).await?;

        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let filters = match filters {
            Some(filters) => Some(kamu_flow_system::DatasetFlowFilters {
                by_flow_type: filters.by_flow_type.map(Into::into),
                by_flow_status: filters.by_status.map(Into::into),
                by_initiator: match filters.by_initiator {
                    Some(initiator_filter) => match initiator_filter {
                        InitiatorFilterInput::System(_) => {
                            Some(kamu_flow_system::InitiatorFilter::System)
                        }
                        InitiatorFilterInput::Accounts(account_ids) => Some(
                            kamu_flow_system::InitiatorFilter::Account(HashSet::from_iter(
                                account_ids.iter().map(Into::into).collect::<Vec<_>>(),
                            )),
                        ),
                    },
                    None => None,
                },
            }),
            None => None,
        };

        let filters = filters.unwrap_or_default();

        let flows_state_listing = flow_query_service
            .list_all_flows_by_dataset(
                &self.dataset_handle.id,
                filters,
                PaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                },
            )
            .await
            .int_err()?;

        let matched_flow_states: Vec<_> = flows_state_listing.matched_stream.try_collect().await?;
        let total_count = flows_state_listing.total_count;
        let matched_flows = Flow::build_batch(matched_flow_states, ctx).await?;

        Ok(FlowConnection::new(
            matched_flows,
            page,
            per_page,
            total_count,
        ))
    }

    async fn list_flow_initiators(&self, ctx: &Context<'_>) -> Result<AccountConnection> {
        utils::check_dataset_read_access(ctx, &self.dataset_handle).await?;

        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let flow_initiator_ids: Vec<_> = flow_query_service
            .list_all_flow_initiators_by_dataset(&self.dataset_handle.id)
            .await
            .int_err()?
            .matched_stream
            .try_collect()
            .await?;

        let authentication_service = from_catalog_n!(ctx, dyn AuthenticationService);

        let matched_flow_initiators: Vec<_> = authentication_service
            .accounts_by_ids(flow_initiator_ids)
            .await?
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

#[derive(OneofObject)]
pub enum InitiatorFilterInput {
    System(bool),
    Accounts(Vec<AccountID>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
