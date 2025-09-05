// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};
use dill::*;
use kamu_flow_system::*;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowProcessState {
    time_source: Arc<dyn SystemTimeSource>,
    state: Arc<RwLock<State>>,
}

#[component(pub)]
#[interface(dyn FlowProcessStateRepository)]
#[interface(dyn FlowProcessStateQuery)]
#[scope(Singleton)]
impl InMemoryFlowProcessState {
    pub fn new(time_source: Arc<dyn SystemTimeSource>) -> Self {
        Self {
            time_source,
            state: Arc::new(RwLock::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    process_state_by_binding: HashMap<FlowBinding, FlowProcessState>,
}

impl State {
    fn list_matching_process_states<'a>(
        &'a self,
        flow_scope_query: &'a FlowScopeQuery,
    ) -> impl Iterator<Item = &'a FlowProcessState> + 'a {
        self.process_state_by_binding
            .values()
            .filter(move |ps| ps.flow_binding().scope.matches_query(flow_scope_query))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateQuery for InMemoryFlowProcessState {
    async fn try_get_process_state(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowProcessState>, InternalError> {
        let state = self.state.read().unwrap();
        Ok(state.process_state_by_binding.get(flow_binding).cloned())
    }

    async fn list_processes(
        &self,
        _filter: &FlowProcessListFilter<'_>,
        _order: FlowProcessOrder,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<FlowProcessState>, InternalError> {
        unimplemented!()
    }

    /// Compute rollup for matching rows.
    async fn rollup_by_scope(
        &self,
        flow_scope_query: &FlowScopeQuery,
        for_flow_types: Option<&[&str]>,
        effective_state_in: Option<&[FlowProcessEffectiveState]>,
    ) -> Result<FlowProcessGroupRollup, InternalError> {
        let state = self.state.read().unwrap();

        // Get filtered list of matching process states
        let matching_states: Vec<&FlowProcessState> = state
            .list_matching_process_states(flow_scope_query)
            .filter(|ps| {
                if for_flow_types
                    .is_some_and(|types| !types.contains(&ps.flow_binding().flow_type.as_str()))
                {
                    return false;
                }
                if effective_state_in.is_some_and(|states| !states.contains(&ps.effective_state()))
                {
                    return false;
                }
                true
            })
            .collect();

        // Use an array to count states by enum discriminant for performance
        use strum::EnumCount;
        let mut worst_consecutive_failures = 0;
        let mut counts = [0_u32; FlowProcessEffectiveState::COUNT];

        for ps in &matching_states {
            let idx = ps.effective_state() as usize;
            counts[idx] += 1;

            if ps.consecutive_failures() > worst_consecutive_failures {
                worst_consecutive_failures = ps.consecutive_failures();
            }
        }

        let active = counts[FlowProcessEffectiveState::Active];
        let failing = counts[FlowProcessEffectiveState::Failing];
        let paused = counts[FlowProcessEffectiveState::PausedManual];
        let stopped = counts[FlowProcessEffectiveState::StoppedAuto];
        let total = active + failing + paused + stopped;

        // Form final rollup result
        let rollup = FlowProcessGroupRollup {
            total,
            active,
            failing,
            paused,
            stopped,
            worst_consecutive_failures,
        };

        Ok(rollup)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateRepository for InMemoryFlowProcessState {
    async fn insert_process(
        &self,
        flow_binding: FlowBinding,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessInsertError> {
        let mut state = self.state.write().unwrap();
        if state.process_state_by_binding.contains_key(&flow_binding) {
            return Err(FlowProcessInsertError::AlreadyExists { flow_binding });
        }

        state.process_state_by_binding.insert(
            flow_binding.clone(),
            FlowProcessState::new(
                self.time_source.now(),
                flow_binding,
                paused_manual,
                stop_policy,
                trigger_event_id,
            ),
        );

        Ok(())
    }

    async fn update_trigger_state(
        &self,
        flow_binding: FlowBinding,
        paused_manual: Option<bool>,
        stop_policy: Option<FlowTriggerStopPolicy>,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessUpdateError> {
        let mut state = self.state.write().unwrap();
        let process_state = state
            .process_state_by_binding
            .get_mut(&flow_binding)
            .ok_or_else(|| {
                FlowProcessUpdateError::NotFound(FlowProcessNotFoundError {
                    flow_binding: flow_binding.clone(),
                })
            })?;

        process_state
            .update_trigger_state(
                self.time_source.now(),
                paused_manual,
                stop_policy,
                trigger_event_id,
            )
            .int_err()?;

        Ok(())
    }

    async fn apply_flow_result(
        &self,
        flow_binding: FlowBinding,
        success: bool,
        event_time: DateTime<Utc>,
        next_planned_at: Option<DateTime<Utc>>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessApplyResultError> {
        let mut state = self.state.write().unwrap();
        let process_state = state
            .process_state_by_binding
            .get_mut(&flow_binding)
            .ok_or_else(|| {
                FlowProcessApplyResultError::NotFound(FlowProcessNotFoundError {
                    flow_binding: flow_binding.clone(),
                })
            })?;

        if success {
            process_state
                .on_success(
                    self.time_source.now(),
                    event_time,
                    next_planned_at,
                    flow_event_id,
                )
                .int_err()?;
        } else {
            process_state
                .on_failure(
                    self.time_source.now(),
                    event_time,
                    next_planned_at,
                    flow_event_id,
                )
                .int_err()?;
        }

        Ok(())
    }

    async fn delete_process(
        &self,
        flow_binding: FlowBinding,
    ) -> Result<(), FlowProcessDeleteError> {
        let mut state = self.state.write().unwrap();
        if state
            .process_state_by_binding
            .remove(&flow_binding)
            .is_none()
        {
            return Err(FlowProcessDeleteError::NotFound(FlowProcessNotFoundError {
                flow_binding,
            }));
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
