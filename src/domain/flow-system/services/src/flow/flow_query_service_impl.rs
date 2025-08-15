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
use dill::{component, interface};
use futures::TryStreamExt;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowQueryService)]
pub struct FlowQueryServiceImpl {
    flow_event_store: Arc<dyn FlowEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl FlowQueryService for FlowQueryServiceImpl {
    #[tracing::instrument(level = "debug", name = FlowQueryServiceImpl_list_all_flows, skip_all)]
    async fn list_all_flows(
        &self,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, InternalError> {
        let empty_filters = FlowFilters::default();
        let total_count = self
            .flow_event_store
            .get_count_all_flows(&empty_filters)
            .await?;

        let all_flows: Vec<_> = self
            .flow_event_store
            .get_all_flow_ids(&empty_filters, pagination)
            .try_collect()
            .await?;
        let matched_stream = self.flow_event_store.get_stream(all_flows);

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    #[tracing::instrument(level = "debug", name = FlowQueryServiceImpl_list_scoped_flows, skip_all)]
    async fn list_scoped_flows(
        &self,
        scope_query: FlowScopeQuery,
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, InternalError> {
        tracing::debug!(?scope_query, ?filters, ?pagination, "Listing scoped flows");

        let total_count = self
            .flow_event_store
            .get_count_flows_matching_scope_query(&scope_query, &filters)
            .await?;

        let relevant_flow_ids: Vec<_> = self
            .flow_event_store
            .get_all_flow_ids_matching_scope_query(scope_query, &filters, pagination)
            .try_collect()
            .await?;

        let matched_stream = self.flow_event_store.get_stream(relevant_flow_ids);

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    #[tracing::instrument(level = "debug", name = FlowQueryServiceImpl_list_scoped_flow_initiators, skip_all)]
    async fn list_scoped_flow_initiators(
        &self,
        scope_query: FlowScopeQuery,
    ) -> Result<FlowInitiatorListing, InternalError> {
        tracing::debug!(?scope_query, "Listing scoped flow initiators");

        Ok(FlowInitiatorListing {
            matched_stream: self
                .flow_event_store
                .list_scoped_flow_initiators(scope_query),
        })
    }

    #[tracing::instrument(level = "debug", name = FlowQueryServiceImpl_filter_flow_scopes_having_flows, skip_all)]
    async fn filter_flow_scopes_having_flows(
        &self,
        scopes: &[FlowScope],
    ) -> Result<Vec<FlowScope>, InternalError> {
        self.flow_event_store
            .filter_flow_scopes_having_flows(scopes)
            .await
    }

    #[tracing::instrument(level = "debug", name = FlowQueryServiceImpl_get_flow skip_all, fields(%flow_id))]
    async fn get_flow(&self, flow_id: FlowID) -> Result<FlowState, GetFlowError> {
        let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await?;
        Ok(flow.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
