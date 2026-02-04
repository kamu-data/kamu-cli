// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_flow_system::{
    FlowProcessListFilter,
    FlowProcessOrder,
    FlowProcessStateQuery,
    FlowScopeQuery,
};

use crate::prelude::*;
use crate::queries::{
    DatasetRequestStateWithOwner,
    WebhookFlowSubProcess,
    build_webhook_id_subscription_mapping_from_processes_listing,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookFlowSubProcessGroup<'a> {
    scope_query: FlowScopeQuery,
    parent_dataset_request_state: &'a DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> WebhookFlowSubProcessGroup<'a> {
    #[graphql(skip)]
    pub fn new(
        scope_query: FlowScopeQuery,
        parent_dataset_request_state: &'a DatasetRequestStateWithOwner,
    ) -> Self {
        Self {
            scope_query,
            parent_dataset_request_state,
        }
    }

    #[tracing::instrument(level = "info", name = WebhookFlowSubProcessGroup_rollup, skip_all)]
    pub async fn rollup(&self, ctx: &Context<'_>) -> Result<FlowProcessGroupRollup> {
        let flow_process_state_query = from_catalog_n!(ctx, dyn FlowProcessStateQuery);
        let rollup = flow_process_state_query
            .rollup(
                FlowProcessListFilter::for_scope(self.scope_query.clone())
                    .for_flow_types(&[FLOW_TYPE_WEBHOOK_DELIVER]),
            )
            .await?;

        Ok(rollup.into())
    }

    #[tracing::instrument(level = "info", name = WebhookFlowSubProcessGroup_subprocesses, skip_all)]
    pub async fn subprocesses(&self, ctx: &Context<'_>) -> Result<Vec<WebhookFlowSubProcess>> {
        let flow_process_state_query = from_catalog_n!(ctx, dyn FlowProcessStateQuery);

        // Load all webhooks processes matching the scope of this group
        // Apply default filter to exclude Unconfigured processes
        let webhook_processes_listing = flow_process_state_query
            .list_processes(
                FlowProcessListFilter::for_scope(self.scope_query.clone())
                    .for_flow_types(&[FLOW_TYPE_WEBHOOK_DELIVER])
                    .with_effective_states(
                        kamu_flow_system::FlowProcessEffectiveState::default_filter_states(),
                    ),
                FlowProcessOrder::recent(),
                None, // No pagination, we expect small number of webhooks in dataset
            )
            .await?;

        // Short-circuit if no processes found
        if webhook_processes_listing.processes.is_empty() {
            return Ok(vec![]);
        }

        // Collect unique subscription ids from the processes
        let webhook_subscriptions_by_id =
            build_webhook_id_subscription_mapping_from_processes_listing(
                ctx,
                &webhook_processes_listing,
            )
            .await?;

        // Prepare processes as GQL objects
        let mut subprocesses = BTreeMap::new();
        for process_state in webhook_processes_listing.processes {
            // Get matching subscription
            let subscription_id =
                FlowScopeSubscription::new(&process_state.flow_binding().scope).subscription_id();
            let webhook_subscription = webhook_subscriptions_by_id
                .get(&subscription_id)
                .expect("must be present");

            // Precollect subprocesses in a name-ordered map
            let subprocess = WebhookFlowSubProcess::new(
                webhook_subscription,
                Some(self.parent_dataset_request_state.clone()),
                process_state,
            );
            subprocesses.insert(subprocess.name(ctx).await?, subprocess);
        }

        // Iterate subprocess states ordered by name
        Ok(subprocesses.into_values().collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
