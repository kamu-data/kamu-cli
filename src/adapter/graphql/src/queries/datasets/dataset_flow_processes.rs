// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::{ingest_dataset_binding, transform_dataset_binding};
use kamu_adapter_flow_webhook::FlowScopeSubscription;
use kamu_flow_system::FlowTriggerService;

use crate::prelude::*;
use crate::queries::{DatasetRequestState, FlowProcess, WebhookFlowSubProcessGroup};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowProcesses<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetFlowProcesses<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    async fn primary(&self, ctx: &Context<'_>) -> Result<Option<FlowProcess>> {
        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

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

        // Try to find existing trigger for this binding
        let maybe_trigger = flow_trigger_service.find_trigger(&flow_binding).await?;

        // If trigger is present, present it's execution history as a periodic process
        Ok(maybe_trigger.map(FlowProcess::new))
    }

    // TODO: other secondary processes in future

    #[allow(clippy::unused_async)]
    pub async fn webhooks(&self) -> Result<WebhookFlowSubProcessGroup> {
        // Form a subprocess group from those that point to
        // webhooks bound to this dataset
        Ok(WebhookFlowSubProcessGroup::new(
            FlowScopeSubscription::query_for_subscriptions_of_dataset(
                self.dataset_request_state.dataset_id(),
            ),
        ))
    }

    // TODO: Kafka exports
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
