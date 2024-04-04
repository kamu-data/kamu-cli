// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use {kamu_flow_system as fs, opendatafabric as odf};

use crate::mutations::{check_if_flow_belongs_to_dataset, FlowInDatasetError, FlowNotFound};
use crate::prelude::*;
use crate::queries::Flow;
use crate::utils;
use crate::utils::ensure_polling_source_exists;

///////////////////////////////////////////////////////////////////////////////

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

        let flow_service = from_catalog::<dyn fs::FlowService>(ctx).unwrap();
        let flow_state = flow_service.get_flow(flow_id.into()).await.int_err()?;

        Ok(GetFlowResult::Success(GetFlowSuccess {
            flow: Flow::new(flow_state),
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

        let flow_service = from_catalog::<dyn fs::FlowService>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        if !ensure_polling_source_exists(ctx, &self.dataset_handle).await? {
            return Ok(FlowConnection::new(vec![], page, per_page, 0));
        }

        let flows_state_listing = flow_service
            .list_all_flows_by_dataset(
                &self.dataset_handle.id,
                match filters {
                    Some(filters) => filters.into(),
                    None => Default::default(),
                },
                fs::FlowPaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                },
            )
            .await
            .int_err()?;

        let matched_flows: Vec<_> = flows_state_listing
            .matched_stream
            .map_ok(Flow::new)
            .try_collect()
            .await?;
        let total_count = flows_state_listing.total_count;

        Ok(FlowConnection::new(
            matched_flows,
            page,
            per_page,
            total_count,
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////

page_based_connection!(Flow, FlowConnection, FlowEdge);

///////////////////////////////////////////////////////////////////////////////

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

#[derive(Debug, Clone)]
pub struct FlowMissingDatasetPollingSource;

#[Object]
impl FlowMissingDatasetPollingSource {
    pub async fn message(&self) -> String {
        "Polling source is required".to_string()
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct DatasetFlowFilters {
    by_flow_type: Option<DatasetFlowType>,
    by_status: Option<FlowStatus>,
    by_initiator: Option<InitiatorFilterInput>,
}

impl From<DatasetFlowFilters> for fs::DatasetFlowFilters {
    fn from(value: DatasetFlowFilters) -> Self {
        Self {
            by_flow_type: value.by_flow_type.map(Into::into),
            by_flow_status: value.by_status.map(Into::into),
            by_initiator: value.by_initiator.map(Into::into),
        }
    }
}

#[derive(OneofObject)]
enum InitiatorFilterInput {
    System(bool),
    Account(AccountName), // TODO: replace on AccountID
}

impl From<InitiatorFilterInput> for fs::InitiatorFilter {
    fn from(value: InitiatorFilterInput) -> Self {
        match value {
            InitiatorFilterInput::System(_) => Self::System,
            InitiatorFilterInput::Account(account_name) => Self::Account(account_name.into()),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
