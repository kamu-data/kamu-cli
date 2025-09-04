// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{FlowEventStore, FlowTriggerState, FlowTriggerStatus};
use kamu_webhooks::WebhookSubscription;
use tokio::sync::OnceCell;

use crate::prelude::*;
use crate::queries::WebhookFlowSubProcess;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookFlowSubProcessGroup {
    webhooks_with_triggers: Vec<(WebhookSubscription, FlowTriggerState)>,
    rollup: OnceCell<FlowProcessGroupRollup>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl WebhookFlowSubProcessGroup {
    #[graphql(skip)]
    pub fn new(webhooks_with_triggers: Vec<(WebhookSubscription, FlowTriggerState)>) -> Self {
        Self {
            webhooks_with_triggers,
            rollup: OnceCell::new(),
        }
    }

    #[allow(clippy::unused_async)]
    async fn rollup(&self, ctx: &Context<'_>) -> Result<FlowProcessGroupRollup> {
        let rollup = self
            .rollup
            .get_or_try_init(|| async {
                let mut running_bindings = Vec::new();

                let mut paused: u32 = 0;
                let mut stopped: u32 = 0;

                for (_, trigger) in &self.webhooks_with_triggers {
                    match trigger.status {
                        FlowTriggerStatus::Active => {
                            running_bindings.push(trigger.flow_binding.clone());
                        }
                        FlowTriggerStatus::PausedByUser => paused += 1,
                        FlowTriggerStatus::StoppedAutomatically => stopped += 1,
                        FlowTriggerStatus::ScopeRemoved => {
                            unreachable!("We should not get deleted triggers at this stage")
                        }
                    }
                }

                let mut worst_consecutive_failures = 0;
                let mut active = 0;
                let mut failing = 0;

                if !running_bindings.is_empty() {
                    let flow_event_store = from_catalog_n!(ctx, dyn FlowEventStore);
                    let results = flow_event_store
                        .consecutive_flow_failures_by_binding(running_bindings)
                        .await?;

                    worst_consecutive_failures =
                        results.iter().map(|(_, count)| *count).max().unwrap_or(0);

                    for (_, consecutive_failures_count) in results {
                        if consecutive_failures_count > 0 {
                            failing += 1;
                        } else {
                            active += 1;
                        }
                    }
                }

                let total = active + failing + paused + stopped;

                Ok::<_, InternalError>(FlowProcessGroupRollup {
                    total,
                    active,
                    failing,
                    paused,
                    stopped,
                    worst_consecutive_failures,
                })
            })
            .await?;

        Ok(*rollup)
    }

    #[allow(clippy::unused_async)]
    async fn subprocesses(&self) -> Result<Vec<WebhookFlowSubProcess>> {
        let mut subprocesses = Vec::new();

        for (webhook_subscription, trigger) in &self.webhooks_with_triggers {
            let subprocess_name = if webhook_subscription.label().as_ref().is_empty() {
                webhook_subscription.target_url().to_string()
            } else {
                webhook_subscription.label().as_ref().to_string()
            };

            subprocesses.push(WebhookFlowSubProcess::new(
                webhook_subscription.id(),
                subprocess_name,
                trigger.clone(),
            ));
        }

        Ok(subprocesses)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
