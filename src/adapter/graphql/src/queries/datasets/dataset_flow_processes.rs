// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_adapter_flow_dataset::{ingest_dataset_binding, transform_dataset_binding};
use kamu_adapter_flow_webhook::FlowScopeSubscription;
use kamu_flow_system::FlowProcessStateQuery;

use crate::prelude::*;
use crate::queries::{
    DatasetFlowProcess,
    DatasetRequestStateWithOwner,
    WebhookFlowSubProcessGroup,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowProcesses<'a> {
    dataset_request_state: &'a DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetFlowProcesses<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestStateWithOwner) -> Self {
        Self {
            dataset_request_state,
        }
    }

    #[tracing::instrument(level = "info", name = DatasetFlowProcesses_primary, skip_all)]
    pub async fn primary(&self, ctx: &Context<'_>) -> Result<DatasetFlowProcess> {
        let flow_process_state_query = from_catalog_n!(ctx, dyn FlowProcessStateQuery);

        // Updates are the primary periodic process for datasets
        // Choose ingest or transform binding depending on dataset kind
        let dataset_kind = self.dataset_request_state.dataset_kind();
        let flow_binding = match dataset_kind {
            odf::DatasetKind::Root => {
                ingest_dataset_binding(self.dataset_request_state.dataset_id())
            }

            odf::DatasetKind::Derivative => {
                transform_dataset_binding(self.dataset_request_state.dataset_id())
            }
        };

        let maybe_process_state = flow_process_state_query
            .try_get_process_state(&flow_binding)
            .await?;

        // Fetch process state
        if let Some(process_state) = maybe_process_state {
            Ok(DatasetFlowProcess::new(
                self.dataset_request_state.clone(),
                process_state,
            ))
        } else {
            // Or synthesize unconfigured state if no process state exists
            Ok(DatasetFlowProcess::new(
                self.dataset_request_state.clone(),
                kamu_flow_system::FlowProcessState::unconfigured(Utc::now(), flow_binding),
            ))
        }
    }

    // TODO: other secondary processes in future

    #[allow(clippy::unused_async)]
    pub async fn webhooks(&self) -> Result<WebhookFlowSubProcessGroup<'_>> {
        // Form a subprocess group from those that point to
        // webhooks bound to this dataset
        Ok(WebhookFlowSubProcessGroup::new(
            FlowScopeSubscription::query_for_subscriptions_of_dataset(
                self.dataset_request_state.dataset_id(),
            ),
            self.dataset_request_state,
        ))
    }

    // TODO: Kafka exports
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
