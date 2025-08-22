// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use kamu_flow_system::{FlowTriggerEventStore, *};
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

use crate::MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowTriggerService)]
#[interface(dyn FlowScopeRemovalHandler)]
pub struct FlowTriggerServiceImpl {
    flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
    flow_event_store: Arc<dyn FlowEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowTriggerServiceImpl {
    async fn pause_given_trigger(
        &self,
        request_time: DateTime<Utc>,
        mut flow_trigger: FlowTrigger,
    ) -> Result<(), InternalError> {
        flow_trigger.pause(request_time).int_err()?;
        flow_trigger
            .save(self.flow_trigger_event_store.as_ref())
            .await
            .int_err()?;

        self.publish_flow_trigger_modified(&flow_trigger, request_time)
            .await?;

        Ok(())
    }

    async fn resume_given_trigger(
        &self,
        request_time: DateTime<Utc>,
        mut flow_trigger: FlowTrigger,
    ) -> Result<(), InternalError> {
        flow_trigger.resume(request_time).int_err()?;
        flow_trigger
            .save(self.flow_trigger_event_store.as_ref())
            .await
            .int_err()?;

        self.publish_flow_trigger_modified(&flow_trigger, request_time)
            .await?;

        Ok(())
    }

    async fn publish_flow_trigger_modified(
        &self,
        state: &FlowTriggerState,
        request_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let message = FlowTriggerUpdatedMessage {
            event_time: request_time,
            flow_binding: state.flow_binding.clone(),
            rule: state.rule.clone(),
            paused: !state.is_active(),
        };

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE, message)
            .await
    }

    async fn remove_given_bindings(
        &self,
        flow_bindings: Vec<FlowBinding>,
    ) -> Result<(), InternalError> {
        tracing::trace!(?flow_bindings, "Removing flow bindings");

        for flow_binding in flow_bindings {
            let maybe_flow_trigger =
                FlowTrigger::try_load(flow_binding, self.flow_trigger_event_store.as_ref())
                    .await
                    .int_err()?;

            if let Some(mut flow_trigger) = maybe_flow_trigger {
                flow_trigger
                    .notify_scope_removed(self.time_source.now())
                    .int_err()?;

                flow_trigger
                    .save(self.flow_trigger_event_store.as_ref())
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTriggerService for FlowTriggerServiceImpl {
    async fn find_trigger(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowTriggerState>, InternalError> {
        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_binding, self.flow_trigger_event_store.as_ref())
                .await
                .int_err()?;
        Ok(maybe_flow_trigger.map(Into::into))
    }

    async fn set_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        paused: bool,
        rule: FlowTriggerRule,
        auto_pause_policy: FlowTriggerAutoPausePolicy,
    ) -> Result<FlowTriggerState, SetFlowTriggerError> {
        tracing::info!(
            flow_binding = ?flow_binding,
            rule = ?rule,
            "Setting flow trigger"
        );

        let maybe_flow_trigger =
            FlowTrigger::try_load(&flow_binding, self.flow_trigger_event_store.as_ref()).await?;

        let mut flow_trigger = match maybe_flow_trigger {
            // Modification
            Some(mut flow_trigger) => {
                flow_trigger
                    .modify_rule(self.time_source.now(), paused, rule, auto_pause_policy)
                    .int_err()?;

                flow_trigger
            }
            // New trigger
            None => FlowTrigger::new(
                self.time_source.now(),
                flow_binding,
                paused,
                rule,
                auto_pause_policy,
            ),
        };

        // Skip saving and publishing events if nothing changed
        if flow_trigger.has_updates() {
            flow_trigger
                .save(self.flow_trigger_event_store.as_ref())
                .await
                .int_err()?;

            self.publish_flow_trigger_modified(&flow_trigger, request_time)
                .await?;
        }

        Ok(flow_trigger.into())
    }

    fn list_enabled_triggers(&self) -> FlowTriggerStateStream {
        Box::pin(async_stream::try_stream! {
            use futures::stream::{self, StreamExt, TryStreamExt};
            let flow_bindings: Vec<_> = self.flow_trigger_event_store.stream_all_active_flow_bindings().try_collect().await.int_err()?;

            let flow_triggers = FlowTrigger::load_multi_simple(flow_bindings, self.flow_trigger_event_store.as_ref()).await.int_err()?;
            let stream = stream::iter(flow_triggers)
                .map(|flow_trigger| Ok::<_, InternalError>(flow_trigger.into()));

            for await item in stream {
                yield item?;
            }
        })
    }

    async fn pause_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_binding, self.flow_trigger_event_store.as_ref())
                .await
                .int_err()?;

        if let Some(flow_trigger) = maybe_flow_trigger {
            self.pause_given_trigger(request_time, flow_trigger)
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn resume_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_binding, self.flow_trigger_event_store.as_ref())
                .await
                .int_err()?;

        if let Some(flow_trigger) = maybe_flow_trigger {
            self.resume_given_trigger(request_time, flow_trigger)
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn pause_flow_triggers_for_scopes(
        &self,
        request_time: DateTime<Utc>,
        scopes: &[FlowScope],
    ) -> Result<(), InternalError> {
        // TODO: maybe batch queries would be helpful here,
        // but for now we just iterate over scopes
        for flow_scope in scopes {
            let flow_bindings = self
                .flow_trigger_event_store
                .all_trigger_bindings_for_scope(flow_scope)
                .await
                .int_err()?;

            let flow_triggers = FlowTrigger::load_multi_simple(
                flow_bindings,
                self.flow_trigger_event_store.as_ref(),
            )
            .await
            .int_err()?;

            for flow_trigger in flow_triggers {
                self.pause_given_trigger(request_time, flow_trigger)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }

    async fn resume_flow_triggers_for_scopes(
        &self,
        request_time: DateTime<Utc>,
        scopes: &[FlowScope],
    ) -> Result<(), InternalError> {
        // TODO: maybe batch queries would be helpful here,
        // but for now we just iterate over scopes
        for flow_scope in scopes {
            let flow_bindings = self
                .flow_trigger_event_store
                .all_trigger_bindings_for_scope(flow_scope)
                .await
                .int_err()?;

            let flow_triggers = FlowTrigger::load_multi_simple(
                flow_bindings,
                self.flow_trigger_event_store.as_ref(),
            )
            .await
            .int_err()?;

            for flow_trigger in flow_triggers {
                self.resume_given_trigger(request_time, flow_trigger)
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all, fields(?scopes))]
    async fn has_active_triggers_for_scopes(
        &self,
        scopes: &[FlowScope],
    ) -> Result<bool, InternalError> {
        tracing::info!(?scopes, "Checking for active triggers for scopes");

        self.flow_trigger_event_store
            .has_active_triggers_for_scopes(scopes)
            .await
    }

    #[tracing::instrument(level = "info", skip_all, fields(?flow_binding))]
    async fn evaluate_auto_pause_policy(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError> {
        // Find an active trigger end evaluate it's pause conditions
        let maybe_active_trigger = self.find_trigger(flow_binding).await?;
        if let Some(active_trigger) = maybe_active_trigger {
            match active_trigger.auto_pause_policy {
                FlowTriggerAutoPausePolicy::AfterConsecutiveFailures { failures_count } => {
                    // Determine actual number of consecutive failures.
                    // Note, if policy is set to 1, we can skip the query,
                    // we know the flow has just failed.
                    let failures_count_value = failures_count.into();
                    let actual_failures_count = if failures_count_value > 1 {
                        self.flow_event_store
                            .get_current_consecutive_flow_failures_count(flow_binding)
                            .await?
                    } else {
                        1 /* this one */
                    };
                    if actual_failures_count >= failures_count_value {
                        tracing::warn!(
                            flow_binding = ?flow_binding,
                            "Auto-pausing flow trigger after {} consecutive failure(s)",
                            actual_failures_count
                        );
                        self.pause_flow_trigger(request_time, flow_binding)
                            .await
                            .int_err()?;
                    } else {
                        tracing::info!(
                            flow_binding = ?flow_binding,
                            "Flow has {} consecutive failures, but auto-pause threshold is {}, so keeping it active",
                            actual_failures_count,
                            failures_count_value
                        );
                    }
                }
                FlowTriggerAutoPausePolicy::Never => {
                    // Do nothing
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowScopeRemovalHandler for FlowTriggerServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(flow_scope = ?flow_scope))]
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        let flow_bindings = self
            .flow_trigger_event_store
            .all_trigger_bindings_for_scope(flow_scope)
            .await?;

        self.remove_given_bindings(flow_bindings).await.int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
