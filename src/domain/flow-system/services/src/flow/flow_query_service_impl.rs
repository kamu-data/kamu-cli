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
use internal_error::ResultIntoInternal;
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowQueryService)]
pub struct FlowQueryServiceImpl {
    flow_event_store: Arc<dyn FlowEventStore>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowQueryService for FlowQueryServiceImpl {
    /// Returns states of flows associated with a given dataset
    /// ordered by creation time from newest to oldest
    /// Applies specified filters
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?filters, ?pagination))]
    async fn list_all_flows_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError> {
        let total_count = self
            .flow_event_store
            .get_count_flows_by_dataset(dataset_id, &filters)
            .await?;

        let relevant_flow_ids: Vec<_> = self
            .flow_event_store
            .get_all_flow_ids_by_dataset(dataset_id, &filters, pagination)
            .try_collect()
            .await?;

        let matched_stream = self.flow_event_store.get_stream(relevant_flow_ids);

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    /// Returns initiators of flows associated with a given dataset
    /// ordered by creation time from newest to oldest
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn list_all_flow_initiators_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<FlowInitiatorListing, ListFlowsByDatasetError> {
        Ok(FlowInitiatorListing {
            matched_stream: self
                .flow_event_store
                .get_unique_flow_initiator_ids_by_dataset(dataset_id),
        })
    }

    async fn list_all_flows_by_dataset_ids(
        &self,
        dataset_ids: &[&odf::DatasetID],
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError> {
        let total_count = self
            .flow_event_store
            .get_count_flows_by_multiple_datasets(dataset_ids, &filters)
            .await?;

        let relevant_flow_ids: Vec<_> = self
            .flow_event_store
            .get_all_flow_ids_by_datasets(dataset_ids, &filters, pagination)
            .try_collect()
            .await
            .int_err()?;
        let matched_stream = self.flow_event_store.get_stream(relevant_flow_ids);

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    /// Returns datasets with flows associated with a given account
    /// ordered by creation time from newest to oldest.
    #[tracing::instrument(level = "debug", skip_all, fields(%account_id))]
    async fn list_all_datasets_with_flow_by_account(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<odf::DatasetID>, ListFlowsByDatasetError> {
        // Consider using ReBAC: not just owned, but any relation to account
        let owned_dataset_ids = self
            .dataset_entry_service
            .get_owned_dataset_ids(account_id)
            .await
            .int_err()?;

        let owned_dataset_id_refs = owned_dataset_ids.iter().collect::<Vec<_>>();

        Ok(self
            .flow_event_store
            .filter_datasets_having_flows(&owned_dataset_id_refs)
            .await?)
    }

    /// Returns states of system flows
    /// ordered by creation time from newest to oldest
    /// Applies specified filters
    #[tracing::instrument(level = "debug", skip_all, fields(?filters, ?pagination))]
    async fn list_all_system_flows(
        &self,
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListSystemFlowsError> {
        let total_count = self
            .flow_event_store
            .get_count_system_flows(&filters)
            .await
            .int_err()?;

        let relevant_flow_ids: Vec<_> = self
            .flow_event_store
            .get_all_system_flow_ids(&filters, pagination)
            .try_collect()
            .await?;

        let matched_stream = self.flow_event_store.get_stream(relevant_flow_ids);

        Ok(FlowStateListing {
            matched_stream,
            total_count,
        })
    }

    /// Returns state of all flows, whether they are system-level or
    /// dataset-bound, ordered by creation time from newest to oldest
    #[tracing::instrument(level = "debug", skip_all, fields(?pagination))]
    async fn list_all_flows(
        &self,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsError> {
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

    /// Returns current state of a given flow
    #[tracing::instrument(level = "debug", skip_all, fields(%flow_id))]
    async fn get_flow(&self, flow_id: FlowID) -> Result<FlowState, GetFlowError> {
        let flow = Flow::load(flow_id, self.flow_event_store.as_ref()).await?;
        Ok(flow.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
