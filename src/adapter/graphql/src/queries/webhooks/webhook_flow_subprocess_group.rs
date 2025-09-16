// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap, HashSet};

use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_flow_system::{
    FlowProcessListFilter,
    FlowProcessOrder,
    FlowProcessStateQuery,
    FlowScopeQuery,
};
use kamu_webhooks::{WebhookSubscription, WebhookSubscriptionEventStore};

use crate::prelude::*;
use crate::queries::WebhookFlowSubProcess;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookFlowSubProcessGroup {
    scope_query: FlowScopeQuery,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl WebhookFlowSubProcessGroup {
    #[graphql(skip)]
    pub fn new(scope_query: FlowScopeQuery) -> Self {
        Self { scope_query }
    }

    #[allow(clippy::unused_async)]
    async fn rollup(&self, ctx: &Context<'_>) -> Result<FlowProcessGroupRollup> {
        let flow_process_state_query = from_catalog_n!(ctx, dyn FlowProcessStateQuery);
        let rollup = flow_process_state_query
            .rollup_by_scope(
                self.scope_query.clone(),
                Some(&[FLOW_TYPE_WEBHOOK_DELIVER]),
                None,
            )
            .await?;

        Ok(rollup.into())
    }

    #[allow(clippy::unused_async)]
    async fn subprocesses(&self, ctx: &Context<'_>) -> Result<Vec<WebhookFlowSubProcess>> {
        let (flow_process_state_query, webhook_subscription_event_store) = from_catalog_n!(
            ctx,
            dyn FlowProcessStateQuery,
            dyn WebhookSubscriptionEventStore
        );

        // Load all webhooks processes matching the scope of this group
        let webhook_processes_listing = flow_process_state_query
            .list_processes(
                FlowProcessListFilter::for_scope(self.scope_query.clone())
                    .for_flow_types(&[FLOW_TYPE_WEBHOOK_DELIVER]),
                FlowProcessOrder::recent(),
                None, // No pagination, we expect small number of webhooks in dataset
            )
            .await?;

        // Collect subscription ids from the processes
        let mut subscription_ids = HashSet::new();
        for process in &webhook_processes_listing.processes {
            let subscription_id =
                FlowScopeSubscription::new(&process.flow_binding().scope).subscription_id();
            subscription_ids.insert(subscription_id);
        }

        // Load related subscriptions
        let subscriptions = WebhookSubscription::load_multi_simple(
            subscription_ids.into_iter().collect(),
            webhook_subscription_event_store.as_ref(),
        )
        .await
        .int_err()?;

        // Organize subscriptions by id
        let subscriptions_by_id = subscriptions
            .into_iter()
            .map(|s| (s.id(), s))
            .collect::<HashMap<_, _>>();

        // Prepare processes as GQL objects
        let mut subprocesses = BTreeMap::new();
        for process_state in webhook_processes_listing.processes {
            // Get matching subscription
            let subscription_id =
                FlowScopeSubscription::new(&process_state.flow_binding().scope).subscription_id();
            let webhook_subscription = subscriptions_by_id
                .get(&subscription_id)
                .expect("must be present");

            // Decide on subprocess name
            let subprocess_name = if webhook_subscription.label().as_ref().is_empty() {
                webhook_subscription.target_url().to_string()
            } else {
                webhook_subscription.label().as_ref().to_string()
            };

            // Precollect subprocesses in a name-ordered map
            subprocesses.insert(
                subprocess_name.clone(),
                WebhookFlowSubProcess::new(
                    webhook_subscription.id(),
                    subprocess_name,
                    process_state,
                ),
            );
        }

        // Iterate subprocess states ordered by name
        Ok(subprocesses.into_values().collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
