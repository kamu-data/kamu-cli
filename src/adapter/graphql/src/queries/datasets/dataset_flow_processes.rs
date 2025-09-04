// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use kamu_adapter_flow_dataset::{ingest_dataset_binding, transform_dataset_binding};
use kamu_adapter_flow_webhook::FlowScopeSubscription;
use kamu_flow_system::FlowTriggerService;
use kamu_webhooks::{WebhookSubscription, WebhookSubscriptionEventStore};

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

    pub async fn webhooks(&self, ctx: &Context<'_>) -> Result<WebhookFlowSubProcessGroup> {
        let (flow_trigger_service, webhook_subscription_event_store) = from_catalog_n!(
            ctx,
            dyn FlowTriggerService,
            dyn WebhookSubscriptionEventStore
        );

        // Find trigers that point to webhooks bound to this dataset
        let matched_triggers = flow_trigger_service
            .match_triggers(FlowScopeSubscription::query_for_subscriptions_of_dataset(
                self.dataset_request_state.dataset_id(),
            ))
            .await?;

        // Organize triggers by subscription id
        let mut triggers_by_subscription_id = HashMap::new();
        for t in matched_triggers {
            let subscription_id =
                FlowScopeSubscription::new(&t.flow_binding.scope).subscription_id();
            triggers_by_subscription_id.insert(subscription_id, t);
        }

        // Load subscriptions
        let subscriptions = WebhookSubscription::load_multi_simple(
            triggers_by_subscription_id.keys().copied().collect(),
            webhook_subscription_event_store.as_ref(),
        )
        .await
        .int_err()?;

        // Zip the (subscription, trigger) pairs
        let subscription_trigger_pairs: Vec<_> = subscriptions
            .into_iter()
            .filter_map(|subscription| {
                let trigger = triggers_by_subscription_id.remove(&subscription.id())?;
                Some((subscription, trigger))
            })
            .collect();

        // Enough data for a channel group
        Ok(WebhookFlowSubProcessGroup::new(subscription_trigger_pairs))
    }

    // TODO: Kafka exports
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
