// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use super::flow_description::{FlowDescription, FlowDescriptionBuilder};
use super::{FlowActivationCause, FlowEvent, FlowOutcome, FlowStartCondition};
use crate::prelude::*;
use crate::queries::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Flow {
    flow_state: Box<fs::FlowState>,
    maybe_related_flow_trigger_state: Option<Box<fs::FlowTriggerState>>,
    description: FlowDescription,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Flow {
    #[graphql(skip)]
    pub async fn build_batch(
        flow_states: Vec<fs::FlowState>,
        ctx: &Context<'_>,
    ) -> Result<Vec<Self>> {
        let mut result: Vec<Self> = Vec::new();

        let mut flow_description_builder =
            FlowDescriptionBuilder::prepare(ctx, &flow_states).await?;

        // Load triggers associated with the flows
        let mut flow_related_trigger_states_by_id =
            Self::load_flow_related_triggers(&flow_states, ctx).await?;

        for flow_state in flow_states {
            // We could possibly have an associated trigger as well
            let maybe_related_flow_trigger_state =
                flow_related_trigger_states_by_id.remove(&flow_state.flow_binding);

            // Construct flow description
            let flow_description = flow_description_builder.build(ctx, &flow_state).await?;

            // Finally, compose all values
            result.push(Self {
                flow_state: Box::new(flow_state),
                maybe_related_flow_trigger_state: maybe_related_flow_trigger_state.map(Box::new),
                description: flow_description,
            });
        }

        Ok(result)
    }

    #[graphql(skip)]
    async fn load_flow_related_triggers(
        flow_states: &[fs::FlowState],
        ctx: &Context<'_>,
    ) -> Result<HashMap<fs::FlowBinding, fs::FlowTriggerState>> {
        let flow_trigger_event_store = from_catalog_n!(ctx, dyn fs::FlowTriggerEventStore);

        let unique_bindings = flow_states
            .iter()
            .map(|flow_state| &flow_state.flow_binding)
            .collect::<HashSet<_>>()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        let flow_triggers_res =
            fs::FlowTrigger::try_load_multi(&unique_bindings, flow_trigger_event_store.as_ref())
                .await;

        let mut result = HashMap::new();
        for res in flow_triggers_res {
            match res {
                Ok(trigger_state) => {
                    result.insert(trigger_state.flow_binding.clone(), trigger_state.into());
                }
                Err(fs::LoadError::NotFound(_)) => { /* skip */ }
                Err(fs::LoadError::Internal(e)) => return Err(e.into()),
                Err(e) => return Err(e.int_err().into()),
            }
        }

        Ok(result)
    }

    /// Unique identifier of the flow
    async fn flow_id(&self) -> FlowID {
        self.flow_state.flow_id.into()
    }

    /// Associated dataset ID, if any
    async fn dataset_id(&self) -> Option<DatasetID<'static>> {
        afs::FlowScopeDataset::maybe_dataset_id_in_scope(&self.flow_state.flow_binding.scope)
            .map(Into::into)
    }

    /// Description of key flow parameters
    async fn description(&self) -> &FlowDescription {
        &self.description
    }

    /// Status of the flow
    async fn status(&self) -> FlowStatus {
        self.flow_state.status().into()
    }

    /// Outcome of the flow (Finished state only)
    async fn outcome(&self, ctx: &Context<'_>) -> Result<Option<FlowOutcome>> {
        match self.flow_state.outcome {
            Some(ref outcome) => Ok(Some(
                FlowOutcome::from_flow_outcome(ctx, outcome)
                    .await
                    .int_err()?,
            )),
            None => Ok(None),
        }
    }

    /// Timing records associated with the flow lifecycle
    async fn timing(&self) -> FlowTimingRecords {
        FlowTimingRecords {
            initiated_at: self.flow_state.primary_activation_cause().activation_time(),
            first_attempt_scheduled_at: self.flow_state.timing.first_scheduled_at,
            scheduled_at: self.flow_state.timing.scheduled_for_activation_at,
            awaiting_executor_since: self.flow_state.timing.awaiting_executor_since,
            running_since: self.flow_state.timing.running_since,
            last_attempt_finished_at: self.flow_state.timing.last_attempt_finished_at,
            completed_at: self.flow_state.timing.completed_at,
        }
    }

    /// IDs of associated tasks
    async fn task_ids(&self) -> Vec<TaskID> {
        self.flow_state
            .task_ids
            .iter()
            .copied()
            .map(Into::into)
            .collect()
    }

    /// History of flow events
    #[tracing::instrument(level = "info", name = Flow_history, skip_all)]
    async fn history(&self, ctx: &Context<'_>) -> Result<Vec<FlowEvent>> {
        let flow_event_store = from_catalog_n!(ctx, dyn fs::FlowEventStore);

        use futures::TryStreamExt;
        let flow_events: Vec<_> = flow_event_store
            .get_events(&self.flow_state.flow_id, Default::default())
            .try_collect()
            .await
            .int_err()?;

        let mut history = Vec::new();
        for (event_id, flow_event) in flow_events {
            history.push(FlowEvent::build(event_id, flow_event, &self.flow_state, ctx).await?);
        }
        Ok(history)
    }

    /// A user, who initiated the flow run. None for system-initiated flows
    async fn initiator(&self, ctx: &Context<'_>) -> Result<Option<Account>> {
        let maybe_initiator = self
            .flow_state
            .primary_activation_cause()
            .initiator_account_id();
        Ok(if let Some(initiator) = maybe_initiator {
            Some(Account::from_account_id(ctx, initiator.clone()).await?)
        } else {
            None
        })
    }

    /// Primary flow activation cause
    async fn primary_activation_cause(
        &self,
        ctx: &Context<'_>,
    ) -> Result<FlowActivationCause, InternalError> {
        FlowActivationCause::build(self.flow_state.primary_activation_cause(), ctx).await
    }

    /// Start condition
    #[allow(clippy::unused_async)]
    async fn start_condition(&self) -> Result<Option<FlowStartCondition>> {
        let maybe_condition = self
            .flow_state
            .start_condition
            .as_ref()
            .map(|start_condition| {
                FlowStartCondition::create_from_raw_flow_data(
                    self.flow_state.as_ref(),
                    start_condition,
                )
            });

        Ok(maybe_condition)
    }

    /// Flow config snapshot
    async fn config_snapshot(&self) -> Option<FlowConfigRule> {
        self.flow_state.config_snapshot.clone().map(Into::into)
    }

    /// Flow retry policy
    async fn retry_policy(&self) -> Option<FlowRetryPolicy> {
        self.flow_state.retry_policy.map(Into::into)
    }

    /// Associated flow trigger
    #[allow(clippy::unused_async)]
    async fn related_trigger(&self) -> Result<Option<FlowTrigger>, InternalError> {
        Ok(self
            .maybe_related_flow_trigger_state
            .as_ref()
            .map(|trigger_state| trigger_state.as_ref().clone())
            .map(Into::into))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
