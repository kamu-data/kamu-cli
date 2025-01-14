// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use dill::{component, interface, Catalog};
use futures::TryStreamExt;
use internal_error::ResultIntoInternal;
use kamu_core::DatasetOwnershipService;
use kamu_flow_system::*;
use opendatafabric::{AccountID, DatasetID};

use crate::{FlowAbortHelper, FlowSchedulingHelper};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowQueryServiceImpl {
    catalog: Catalog,
    flow_event_store: Arc<dyn FlowEventStore>,
    dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
    agent_config: Arc<FlowAgentConfig>,
}

#[component(pub)]
#[interface(dyn FlowQueryService)]
impl FlowQueryServiceImpl {
    pub fn new(
        catalog: Catalog,
        flow_event_store: Arc<dyn FlowEventStore>,
        dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
        agent_config: Arc<FlowAgentConfig>,
    ) -> Self {
        Self {
            catalog,
            flow_event_store,
            dataset_ownership_service,
            agent_config,
        }
    }
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
        dataset_id: &DatasetID,
        filters: DatasetFlowFilters,
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
        dataset_id: &DatasetID,
    ) -> Result<FlowInitiatorListing, ListFlowsByDatasetError> {
        Ok(FlowInitiatorListing {
            matched_stream: self
                .flow_event_store
                .get_unique_flow_initiator_ids_by_dataset(dataset_id),
        })
    }

    /// Returns states of flows associated with a given account
    /// ordered by creation time from newest to oldest
    /// Applies specified filters
    #[tracing::instrument(level = "debug", skip_all, fields(%account_id, ?filters, ?pagination))]
    async fn list_all_flows_by_account(
        &self,
        account_id: &AccountID,
        filters: AccountFlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError> {
        let owned_dataset_ids = self
            .dataset_ownership_service
            .get_owned_datasets(account_id)
            .await
            .map_err(ListFlowsByDatasetError::Internal)?;

        let filtered_dataset_ids = if !filters.by_dataset_ids.is_empty() {
            owned_dataset_ids
                .into_iter()
                .filter(|dataset_id| filters.by_dataset_ids.contains(dataset_id))
                .collect()
        } else {
            owned_dataset_ids
        };

        let mut total_count = 0;
        let dataset_flow_filters = DatasetFlowFilters {
            by_flow_status: filters.by_flow_status,
            by_flow_type: filters.by_flow_type,
            by_initiator: filters.by_initiator,
        };

        for dataset_id in &filtered_dataset_ids {
            total_count += self
                .flow_event_store
                .get_count_flows_by_dataset(dataset_id, &dataset_flow_filters)
                .await?;
        }

        let account_dataset_ids: HashSet<DatasetID> = HashSet::from_iter(filtered_dataset_ids);

        let relevant_flow_ids: Vec<_> = self
            .flow_event_store
            .get_all_flow_ids_by_datasets(account_dataset_ids, &dataset_flow_filters, pagination)
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
        account_id: &AccountID,
    ) -> Result<FlowDatasetListing, ListFlowsByDatasetError> {
        let owned_dataset_ids = self
            .dataset_ownership_service
            .get_owned_datasets(account_id)
            .await
            .map_err(ListFlowsByDatasetError::Internal)?;

        let matched_stream = Box::pin(async_stream::try_stream! {
            for dataset_id in &owned_dataset_ids {
                let dataset_flows_count = self
                    .flow_event_store
                    .get_count_flows_by_dataset(dataset_id, &Default::default())
                    .await?;

                if dataset_flows_count > 0 {
                    yield dataset_id.clone();
                }
            }
        });

        Ok(FlowDatasetListing { matched_stream })
    }

    /// Returns states of system flows
    /// ordered by creation time from newest to oldest
    /// Applies specified filters
    #[tracing::instrument(level = "debug", skip_all, fields(?filters, ?pagination))]
    async fn list_all_system_flows(
        &self,
        filters: SystemFlowFilters,
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
        let empty_filters = AllFlowFilters::default();
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

    /// Triggers the specified flow manually, unless it's already waiting
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(?flow_key, %initiator_account_id)
    )]
    async fn trigger_manual_flow(
        &self,
        trigger_time: DateTime<Utc>,
        flow_key: FlowKey,
        initiator_account_id: AccountID,
        config_snapshot_maybe: Option<FlowConfigurationRule>,
    ) -> Result<FlowState, RequestFlowError> {
        let activation_time = self.agent_config.round_time(trigger_time)?;

        let scheduling_helper = self.catalog.get_one::<FlowSchedulingHelper>().unwrap();
        scheduling_helper
            .trigger_flow_common(
                &flow_key,
                None,
                FlowTriggerType::Manual(FlowTriggerManual {
                    trigger_time: activation_time,
                    initiator_account_id,
                }),
                config_snapshot_maybe,
            )
            .await
            .map_err(RequestFlowError::Internal)
    }

    /// Attempts to cancel the tasks already scheduled for the given flow
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(%flow_id)
    )]
    async fn cancel_scheduled_tasks(
        &self,
        flow_id: FlowID,
    ) -> Result<FlowState, CancelScheduledTasksError> {
        // Abort current flow and it's scheduled tasks
        let abort_helper = self.catalog.get_one::<FlowAbortHelper>().unwrap();
        abort_helper
            .abort_flow(flow_id)
            .await
            .map_err(CancelScheduledTasksError::Internal)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
