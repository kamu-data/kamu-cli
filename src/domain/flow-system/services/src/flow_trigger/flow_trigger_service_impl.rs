// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::slice;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::*;
use kamu_flow_system::{FlowTriggerEventStore, *};
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowTriggerService)]
#[interface(dyn FlowScopeRemovalHandler)]
pub struct FlowTriggerServiceImpl {
    flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowTriggerServiceImpl {
    async fn pause_given_trigger(
        &self,
        request_time: DateTime<Utc>,
        mut flow_trigger: FlowTrigger,
    ) -> Result<FlowTriggerState, InternalError> {
        flow_trigger.pause(request_time).int_err()?;
        self.save_trigger(request_time, flow_trigger).await
    }

    async fn stop_given_trigger(
        &self,
        request_time: DateTime<Utc>,
        mut flow_trigger: FlowTrigger,
    ) -> Result<FlowTriggerState, InternalError> {
        flow_trigger.stop(request_time).int_err()?;
        self.save_trigger(request_time, flow_trigger).await
    }

    async fn resume_given_trigger(
        &self,
        request_time: DateTime<Utc>,
        mut flow_trigger: FlowTrigger,
    ) -> Result<FlowTriggerState, InternalError> {
        flow_trigger.resume(request_time).int_err()?;
        self.save_trigger(request_time, flow_trigger).await
    }

    async fn save_trigger(
        &self,
        request_time: DateTime<Utc>,
        mut flow_trigger: FlowTrigger,
    ) -> Result<FlowTriggerState, InternalError> {
        // Skip saving and publishing events if nothing changed
        if flow_trigger.has_updates() {
            flow_trigger
                .save(self.flow_trigger_event_store.as_ref())
                .await
                .int_err()?;

            self.publish_trigger_updated(&flow_trigger, request_time)
                .await?;
        }

        Ok(flow_trigger.into())
    }

    async fn publish_trigger_updated(
        &self,
        trigger: &FlowTrigger,
        request_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let message = FlowTriggerUpdatedMessage {
            event_time: request_time,
            event_id: trigger.last_stored_event_id().expect("must have event id"),
            flow_binding: trigger.flow_binding.clone(),
            rule: trigger.rule.clone(),
            stop_policy: trigger.stop_policy,
            trigger_status: trigger.status,
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

        let now = self.time_source.now();

        let mut flow_triggers = self.load_alive_triggers(&flow_bindings).await?;
        for flow_trigger in &mut flow_triggers {
            flow_trigger.notify_scope_removed(now).int_err()?;
        }

        FlowTrigger::save_multi(&mut flow_triggers, self.flow_trigger_event_store.as_ref())
            .await
            .int_err()?;

        Ok(())
    }

    /// Loads triggers of the given bindings in bulk, skipping missing and
    /// stopped ones
    async fn load_alive_triggers(
        &self,
        flow_bindings: &[FlowBinding],
    ) -> Result<Vec<FlowTrigger>, InternalError> {
        let mut flow_triggers = Vec::with_capacity(flow_bindings.len());

        for load_result in
            FlowTrigger::try_load_multi(flow_bindings, self.flow_trigger_event_store.as_ref()).await
        {
            match load_result {
                Ok(flow_trigger) => {
                    if flow_trigger.is_alive() {
                        flow_triggers.push(flow_trigger);
                    }
                }
                Err(LoadError::NotFound(_)) => {}
                Err(e @ (LoadError::ProjectionError(_) | LoadError::Internal(_))) => {
                    return Err(e.int_err());
                }
            }
        }

        Ok(flow_triggers)
    }

    /// Applies a status change to all alive triggers of the given scopes, then
    /// saves the changed ones in bulk and announces each of them
    async fn change_triggers_for_scopes(
        &self,
        request_time: DateTime<Utc>,
        scopes: &[FlowScope],
        change: impl Fn(&mut FlowTrigger) -> Result<(), ProjectionError<FlowTriggerState>>,
    ) -> Result<(), InternalError> {
        let flow_bindings = self
            .flow_trigger_event_store
            .all_trigger_bindings_for_scopes(scopes)
            .await?;

        let mut changed_triggers = Vec::new();
        for mut flow_trigger in self.load_alive_triggers(&flow_bindings).await? {
            change(&mut flow_trigger).int_err()?;
            if flow_trigger.has_updates() {
                changed_triggers.push(flow_trigger);
            }
        }

        FlowTrigger::save_multi(
            &mut changed_triggers,
            self.flow_trigger_event_store.as_ref(),
        )
        .await
        .int_err()?;

        for flow_trigger in &changed_triggers {
            self.publish_trigger_updated(flow_trigger, request_time)
                .await?;
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

        Ok(if let Some(flow_trigger) = maybe_flow_trigger {
            if flow_trigger.is_dead() {
                None
            } else {
                Some(flow_trigger.into())
            }
        } else {
            None
        })
    }

    async fn find_triggers(
        &self,
        flow_bindings: &[FlowBinding],
    ) -> Result<Vec<FlowTriggerState>, InternalError> {
        let flow_triggers =
            FlowTrigger::load_multi_simple(flow_bindings, self.flow_trigger_event_store.as_ref())
                .await
                .int_err()?;

        Ok(flow_triggers
            .into_iter()
            .filter(|ft| ft.is_alive())
            .map(Into::into)
            .collect())
    }

    async fn set_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        rule: FlowTriggerRule,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<FlowTriggerState, SetFlowTriggerError> {
        tracing::info!(
            flow_binding = ?flow_binding,
            rule = ?rule,
            stop_policy = ?stop_policy,
            "Setting flow trigger"
        );

        let maybe_flow_trigger =
            FlowTrigger::try_load(&flow_binding, self.flow_trigger_event_store.as_ref()).await?;

        let flow_trigger = match maybe_flow_trigger {
            // Modification
            Some(mut flow_trigger) => {
                flow_trigger
                    .modify_rule(self.time_source.now(), rule, stop_policy)
                    .int_err()?;

                flow_trigger
            }
            // New trigger
            None => FlowTrigger::new(self.time_source.now(), flow_binding, rule, stop_policy),
        };

        // Save trigger
        self.save_trigger(request_time, flow_trigger)
            .await
            .map_err(Into::into)
    }

    fn list_enabled_triggers(&self) -> FlowTriggerStateStream<'_> {
        Box::pin(async_stream::try_stream! {
            use futures::stream::{self, StreamExt, TryStreamExt};
            let flow_bindings: Vec<_> = self.flow_trigger_event_store.stream_all_active_flow_bindings().try_collect().await.int_err()?;

            let flow_triggers = FlowTrigger::load_multi_simple(&flow_bindings, self.flow_trigger_event_store.as_ref()).await.int_err()?;
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
        self.change_triggers_for_scopes(request_time, scopes, |flow_trigger| {
            flow_trigger.pause(request_time)
        })
        .await
    }

    async fn resume_flow_triggers_for_scopes(
        &self,
        request_time: DateTime<Utc>,
        scopes: &[FlowScope],
    ) -> Result<(), InternalError> {
        self.change_triggers_for_scopes(request_time, scopes, |flow_trigger| {
            flow_trigger.resume(request_time)
        })
        .await
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
    async fn apply_trigger_auto_stop_decision(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowTriggerState>, InternalError> {
        // Find an active trigger
        let maybe_active_trigger =
            FlowTrigger::try_load(flow_binding, self.flow_trigger_event_store.as_ref())
                .await
                .int_err()?;

        // If found, stop it
        let maybe_new_trigger_state = if let Some(active_trigger) = maybe_active_trigger {
            let new_trigger_state = self
                .stop_given_trigger(request_time, active_trigger)
                .await?;
            Some(new_trigger_state)
        } else {
            None
        };

        // Return the updated state
        Ok(maybe_new_trigger_state)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowScopeRemovalHandler for FlowTriggerServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(flow_scope = ?flow_scope))]
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        let flow_bindings = self
            .flow_trigger_event_store
            .all_trigger_bindings_for_scopes(slice::from_ref(flow_scope))
            .await?;

        self.remove_given_bindings(flow_bindings).await.int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
