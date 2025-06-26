// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use {kamu_flow_system as fs, kamu_task_system as ts};

use super::flow_description::{FlowDescription, FlowDescriptionBuilder};
use super::{
    FlowConfigurationSnapshot,
    FlowEvent,
    FlowOutcome,
    FlowStartCondition,
    FlowTriggerInstance,
};
use crate::prelude::*;
use crate::queries::{Account, Task};
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Flow {
    flow_state: Box<fs::FlowState>,
    flow_task_states: Vec<ts::TaskState>,
    description: FlowDescription,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
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

        // Load task states associated with the flows
        let mut flow_task_states_by_id =
            Self::load_flow_task_states(flow_states.clone(), ctx).await?;

        for flow_state in flow_states {
            // Extract task states associated with the flow
            let flow_task_states = flow_state
                .task_ids
                .iter()
                .filter_map(|task_id| flow_task_states_by_id.remove(task_id))
                .collect::<Vec<_>>();

            let flow_description = flow_description_builder.build(ctx, &flow_state).await?;
            result.push(Self {
                flow_state: Box::new(flow_state),
                flow_task_states,
                description: flow_description,
            });
        }

        Ok(result)
    }

    #[graphql(skip)]
    async fn load_flow_task_states(
        flow_states: Vec<fs::FlowState>,
        ctx: &Context<'_>,
    ) -> Result<HashMap<ts::TaskID, ts::TaskState>> {
        let task_event_store = from_catalog_n!(ctx, dyn ts::TaskEventStore);

        let flow_task_ids: Vec<_> = flow_states
            .iter()
            .flat_map(|flow_state| flow_state.task_ids.iter().copied())
            .collect();
        let flow_task_states: Vec<ts::TaskState> =
            ts::Task::load_multi_simple(flow_task_ids, task_event_store.as_ref())
                .await
                .int_err()?
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>();

        Ok(flow_task_states
            .into_iter()
            .map(|task_state| (task_state.task_id, task_state))
            .collect::<HashMap<_, _>>())
    }

    /// Unique identifier of the flow
    async fn flow_id(&self) -> FlowID {
        self.flow_state.flow_id.into()
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
            Some(ref outcome) => {
                let maybe_task_outcome = self
                    .flow_task_states
                    .last()
                    .and_then(|task_state| task_state.outcome.as_ref());

                Ok(Some(
                    FlowOutcome::from_flow_and_task_outcomes(ctx, outcome, maybe_task_outcome)
                        .await
                        .int_err()?,
                ))
            }
            None => Ok(None),
        }
    }

    /// Timing records associated with the flow lifecycle
    async fn timing(&self) -> FlowTimingRecords {
        self.flow_state.timing.into()
    }

    /// Associated tasks
    #[tracing::instrument(level = "info", name = Flow_tasks, skip_all)]
    async fn tasks(&self, ctx: &Context<'_>) -> Result<Vec<Task>> {
        let mut tasks = Vec::new();
        for task_id in &self.flow_state.task_ids {
            let ts_task = utils::get_task(ctx, *task_id).await?;
            tasks.push(Task::new(ts_task));
        }
        Ok(tasks)
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
        let maybe_initiator = self.flow_state.primary_trigger().initiator_account_id();
        Ok(if let Some(initiator) = maybe_initiator {
            Some(Account::from_account_id(ctx, initiator.clone()).await?)
        } else {
            None
        })
    }

    /// Primary flow trigger
    async fn primary_trigger(
        &self,
        ctx: &Context<'_>,
    ) -> Result<FlowTriggerInstance, InternalError> {
        FlowTriggerInstance::build(self.flow_state.primary_trigger(), ctx).await
    }

    /// Start condition
    async fn start_condition(&self, ctx: &Context<'_>) -> Result<Option<FlowStartCondition>> {
        let maybe_condition =
            if let Some(start_condition) = self.flow_state.start_condition.as_ref() {
                Some(
                    FlowStartCondition::create_from_raw_flow_data(
                        start_condition,
                        &self.flow_state.triggers,
                        ctx,
                    )
                    .await
                    .int_err()?,
                )
            } else {
                None
            };

        Ok(maybe_condition)
    }

    /// Flow config snapshot
    async fn config_snapshot(&self) -> Option<FlowConfigurationSnapshot> {
        self.flow_state.config_snapshot.clone().map(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
