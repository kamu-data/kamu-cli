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
use database_common::PaginationOpts;
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

    #[allow(clippy::collapsible_if)]
    fn apply_filters<'a>(
        &self,
        state: &'a State,
        filter: &'a FlowProcessListFilter<'_>,
    ) -> Vec<&'a FlowProcessState> {
        state
            .list_matching_process_states(&filter.scope)
            .filter(|ps| {
                // Flow types filter
                if let Some(types) = filter.for_flow_types {
                    if !types
                        .iter()
                        .any(|&t| t == ps.flow_binding().flow_type.as_str())
                    {
                        return false;
                    }
                }

                // Effective state filter
                if filter
                    .effective_state_in
                    .as_ref()
                    .is_some_and(|states| !states.contains(&ps.effective_state()))
                {
                    return false;
                }

                // Last attempt time range filter
                if let Some((start, end)) = filter.last_attempt_between {
                    if let Some(last_attempt) = ps.last_attempt_at() {
                        if start > last_attempt || end < last_attempt {
                            return false;
                        }
                    } else {
                        // No last attempt time, so doesn't match range filter
                        return false;
                    }
                }

                // Last failure since filter
                if let Some(since) = filter.last_failure_since {
                    if let Some(last_failure) = ps.last_failure_at() {
                        if since > last_failure {
                            return false;
                        }
                    } else {
                        // No failure recorded, so doesn't match "since" filter
                        return false;
                    }
                }

                // Next planned before filter
                if let Some(before) = filter.next_planned_before {
                    if let Some(next_planned) = ps.next_planned_at() {
                        if next_planned >= before {
                            return false;
                        }
                    } else {
                        // No next planned time, so doesn't match "before" filter
                        return false;
                    }
                }

                // Next planned after filter
                if let Some(after) = filter.next_planned_after {
                    if let Some(next_planned) = ps.next_planned_at() {
                        if next_planned <= after {
                            return false;
                        }
                    } else {
                        // No next planned time, so doesn't match "after" filter
                        return false;
                    }
                }

                // Minimum consecutive failures filter
                if let Some(min_failures) = filter.min_consecutive_failures {
                    if ps.consecutive_failures() < min_failures {
                        return false;
                    }
                }

                true
            })
            .collect()
    }

    fn apply_ordering(&self, states: &mut [&FlowProcessState], order: FlowProcessOrder) {
        // Apply ordering with multi-level sort criteria:
        // 1. Primary field (user-defined)
        // 2. Last attempt time (newest first as tie-breaker, unless it's the primary
        //    field)
        // 3. Flow type (for additional stability, unless it's the primary field)
        // 4. Sort key (final tie-breaker for stable pagination)
        states.sort_by(|a, b| {
            use std::cmp::Ordering;

            // Primary sort field with NULLS LAST for datetime fields
            let primary_ordering = match order.field {
                FlowProcessOrderField::LastAttemptAt => {
                    // NULLS LAST: compare non-null values first, then handle nulls
                    match (a.last_attempt_at(), b.last_attempt_at()) {
                        (Some(a_val), Some(b_val)) => {
                            if order.desc {
                                b_val.cmp(&a_val) // DESC: largest first
                            } else {
                                a_val.cmp(&b_val) // ASC: smallest first
                            }
                        }
                        (Some(_), None) => std::cmp::Ordering::Less, // Some < None (NULLS LAST)
                        (None, Some(_)) => std::cmp::Ordering::Greater, // None > Some (NULLS LAST)
                        (None, None) => std::cmp::Ordering::Equal,   // None == None
                    }
                }
                FlowProcessOrderField::NextPlannedAt => {
                    // NULLS LAST: compare non-null values first, then handle nulls
                    match (a.next_planned_at(), b.next_planned_at()) {
                        (Some(a_val), Some(b_val)) => {
                            if order.desc {
                                b_val.cmp(&a_val) // DESC: largest first
                            } else {
                                a_val.cmp(&b_val) // ASC: smallest first
                            }
                        }
                        (Some(_), None) => std::cmp::Ordering::Less, // Some < None (NULLS LAST)
                        (None, Some(_)) => std::cmp::Ordering::Greater, // None > Some (NULLS LAST)
                        (None, None) => std::cmp::Ordering::Equal,   // None == None
                    }
                }
                FlowProcessOrderField::LastFailureAt => {
                    // NULLS LAST: compare non-null values first, then handle nulls
                    match (a.last_failure_at(), b.last_failure_at()) {
                        (Some(a_val), Some(b_val)) => {
                            if order.desc {
                                b_val.cmp(&a_val) // DESC: largest first
                            } else {
                                a_val.cmp(&b_val) // ASC: smallest first
                            }
                        }
                        (Some(_), None) => std::cmp::Ordering::Less, // Some < None (NULLS LAST)
                        (None, Some(_)) => std::cmp::Ordering::Greater, // None > Some (NULLS LAST)
                        (None, None) => std::cmp::Ordering::Equal,   // None == None
                    }
                }
                FlowProcessOrderField::ConsecutiveFailures => {
                    if order.desc {
                        b.consecutive_failures().cmp(&a.consecutive_failures())
                    } else {
                        a.consecutive_failures().cmp(&b.consecutive_failures())
                    }
                }
                FlowProcessOrderField::EffectiveState => {
                    if order.desc {
                        b.effective_state().cmp(&a.effective_state())
                    } else {
                        a.effective_state().cmp(&b.effective_state())
                    }
                }
                FlowProcessOrderField::FlowType => {
                    if order.desc {
                        b.flow_binding().flow_type.cmp(&a.flow_binding().flow_type)
                    } else {
                        a.flow_binding().flow_type.cmp(&b.flow_binding().flow_type)
                    }
                }
            };

            // If primary field values are equal, use tie-breakers
            match primary_ordering {
                Ordering::Equal => {
                    // Build tie-breaker chain dynamically based on primary field

                    // Tie-breaker 1: Last attempt time (newest first) - skip if it's the primary
                    // field
                    if !matches!(order.field, FlowProcessOrderField::LastAttemptAt) {
                        // NULLS LAST for tie-breaker, newest first (DESC)
                        let attempt_ordering = match (a.last_attempt_at(), b.last_attempt_at()) {
                            // reversed for newest first
                            (Some(a_val), Some(b_val)) => b_val.cmp(&a_val),
                            (Some(_), None) => std::cmp::Ordering::Less, /* Some < None (NULLS */
                            // LAST)
                            (None, Some(_)) => std::cmp::Ordering::Greater, /* None > Some */
                            // (NULLS LAST)
                            (None, None) => std::cmp::Ordering::Equal, // None == None
                        };
                        if attempt_ordering != Ordering::Equal {
                            return attempt_ordering;
                        }
                    }

                    // Tie-breaker 2: Last update event ID
                    a.last_applied_event_id().cmp(&b.last_applied_event_id())
                }
                primary_order => primary_order,
            }
        });
    }

    fn apply_pagination(
        &self,
        states: Vec<&FlowProcessState>,
        pagination: PaginationOpts,
    ) -> Vec<FlowProcessState> {
        states
            .into_iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .cloned()
            .collect()
    }

    fn compute_rollup(&self, matching_states: &[&FlowProcessState]) -> FlowProcessGroupRollup {
        // Use an array to count states by enum discriminant for performance
        use strum::EnumCount;
        let mut worst_consecutive_failures = 0;
        let mut counts = [0_u32; FlowProcessEffectiveState::COUNT];

        for ps in matching_states {
            let idx = ps.effective_state() as usize;
            counts[idx] += 1;

            if ps.consecutive_failures() > worst_consecutive_failures {
                worst_consecutive_failures = ps.consecutive_failures();
            }
        }

        let active = counts[FlowProcessEffectiveState::Active as usize];
        let failing = counts[FlowProcessEffectiveState::Failing as usize];
        let paused = counts[FlowProcessEffectiveState::PausedManual as usize];
        let stopped = counts[FlowProcessEffectiveState::StoppedAuto as usize];
        let unconfigured = counts[FlowProcessEffectiveState::Unconfigured as usize];
        let total = active + failing + paused + stopped + unconfigured;

        // Form final rollup result
        FlowProcessGroupRollup {
            total,
            active,
            failing,
            paused,
            stopped,
            unconfigured,
            worst_consecutive_failures,
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

    fn get_process_state_mut(
        &mut self,
        flow_binding: &FlowBinding,
        current_time: DateTime<Utc>,
    ) -> &mut FlowProcessState {
        self.process_state_by_binding
            .entry(flow_binding.clone())
            .or_insert_with(|| FlowProcessState::unconfigured(current_time, flow_binding.clone()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateQuery for InMemoryFlowProcessState {
    async fn has_any_process_states(&self) -> Result<bool, InternalError> {
        let state = self.state.read().unwrap();
        Ok(!state.process_state_by_binding.is_empty())
    }

    async fn try_get_process_state(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowProcessState>, InternalError> {
        let state = self.state.read().unwrap();
        Ok(state.process_state_by_binding.get(flow_binding).cloned())
    }

    async fn list_processes(
        &self,
        filter: FlowProcessListFilter<'_>,
        order: FlowProcessOrder,
        pagination: Option<PaginationOpts>,
    ) -> Result<FlowProcessStateListing, InternalError> {
        let state = self.state.read().unwrap();

        // Apply filtering
        let mut matching_states = self.apply_filters(&state, &filter);

        // Store total count before pagination
        let total_count = matching_states.len();

        // Apply ordering
        self.apply_ordering(&mut matching_states, order);

        // Apply pagination
        let processes = if let Some(pagination) = pagination {
            self.apply_pagination(matching_states, pagination)
        } else {
            matching_states.iter().map(|ps| (*ps).clone()).collect()
        };

        Ok(FlowProcessStateListing {
            processes,
            total_count,
        })
    }

    /// Compute rollup for matching rows.
    async fn rollup(
        &self,
        filter: FlowProcessListFilter<'_>,
    ) -> Result<FlowProcessGroupRollup, InternalError> {
        let state = self.state.read().unwrap();

        // Get filtered list of matching process states using existing method
        let matching_states = self.apply_filters(&state, &filter);

        // Compute and return the rollup
        let rollup = self.compute_rollup(&matching_states);
        Ok(rollup)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateRepository for InMemoryFlowProcessState {
    async fn upsert_process_state_on_trigger_event(
        &self,
        event_id: EventID,
        flow_binding: FlowBinding,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<FlowProcessState, FlowProcessUpsertError> {
        let mut state = self.state.write().unwrap();
        if let Some(existing) = state.process_state_by_binding.get_mut(&flow_binding) {
            existing
                .update_trigger_state(event_id, self.time_source.now(), paused_manual, stop_policy)
                .int_err()?;
            let state = existing.clone();
            existing.take_pending_events();
            Ok(state)
        } else {
            let user_intent = if paused_manual {
                FlowProcessUserIntent::Paused
            } else {
                FlowProcessUserIntent::Enabled
            };
            let new_state = FlowProcessState::new(
                event_id,
                self.time_source.now(),
                flow_binding.clone(),
                user_intent,
                stop_policy,
            );
            state
                .process_state_by_binding
                .insert(flow_binding, new_state.clone());
            Ok(new_state)
        }
    }

    async fn apply_flow_result(
        &self,
        event_id: EventID,
        flow_binding: &FlowBinding,
        flow_outcome: &FlowOutcome,
        event_time: DateTime<Utc>,
    ) -> Result<FlowProcessState, FlowProcessFlowEventError> {
        let mut state = self.state.write().unwrap();

        let process_state = state.get_process_state_mut(flow_binding, self.time_source.now());

        process_state
            .on_flow_outcome(event_id, self.time_source.now(), event_time, flow_outcome)
            .int_err()?;

        let result = process_state.clone();
        process_state.take_pending_events();
        Ok(result)
    }

    async fn on_flow_scheduled(
        &self,
        event_id: EventID,
        flow_binding: &FlowBinding,
        planned_at: DateTime<Utc>,
    ) -> Result<FlowProcessState, FlowProcessFlowEventError> {
        let mut state = self.state.write().unwrap();

        let process_state = state.get_process_state_mut(flow_binding, self.time_source.now());

        process_state
            .on_scheduled(event_id, self.time_source.now(), planned_at)
            .int_err()?;

        let result = process_state.clone();
        process_state.take_pending_events();
        Ok(result)
    }

    async fn on_flow_task_running(
        &self,
        event_id: EventID,
        flow_binding: &FlowBinding,
        started_at: DateTime<Utc>,
    ) -> Result<FlowProcessState, FlowProcessFlowEventError> {
        let mut state = self.state.write().unwrap();

        let process_state = state.get_process_state_mut(flow_binding, self.time_source.now());

        process_state
            .on_running(event_id, self.time_source.now(), started_at)
            .int_err()?;

        let result = process_state.clone();
        process_state.take_pending_events();
        Ok(result)
    }

    async fn delete_process_states_by_scope(
        &self,
        scope: &FlowScope,
    ) -> Result<(), FlowProcessDeleteError> {
        let mut state = self.state.write().unwrap();
        state
            .process_state_by_binding
            .retain(|binding, _| binding.scope != *scope);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
