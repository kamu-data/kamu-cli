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

    #[allow(clippy::collapsible_if)]
    fn apply_filters<'a>(
        &self,
        state: &'a State,
        filter: &'a FlowProcessListFilter<'_>,
    ) -> Vec<&'a FlowProcessState> {
        // Pre-lowercase the name query once outside the loop for efficiency
        let name_query_lowercase = filter.name_contains.map(str::to_lowercase);

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
                        if before < next_planned {
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
                        if after > next_planned {
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

                // Name contains filter (case-insensitive)
                // Note: sort_key is guaranteed to be lowercase, so we only need to compare
                // against the pre-lowercased query
                if let Some(ref name_query_lower) = name_query_lowercase {
                    if !ps.sort_key().contains(name_query_lower) {
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
                FlowProcessOrderField::NameAlpha => {
                    if order.desc {
                        b.sort_key().cmp(a.sort_key())
                    } else {
                        a.sort_key().cmp(b.sort_key())
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

                    // Tie-breaker 2: Flow type - skip if it's the primary field
                    if !matches!(order.field, FlowProcessOrderField::FlowType) {
                        let flow_type_ordering =
                            a.flow_binding().flow_type.cmp(&b.flow_binding().flow_type);
                        if flow_type_ordering != Ordering::Equal {
                            return flow_type_ordering;
                        }
                    }

                    // Final tie-breaker: Sort key for stable pagination
                    a.sort_key().cmp(b.sort_key())
                }
                primary_order => primary_order,
            }
        });
    }

    fn apply_pagination(
        &self,
        states: Vec<&FlowProcessState>,
        limit: usize,
        offset: usize,
    ) -> Vec<FlowProcessState> {
        states
            .into_iter()
            .skip(offset)
            .take(limit)
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

        let active = counts[FlowProcessEffectiveState::Active];
        let failing = counts[FlowProcessEffectiveState::Failing];
        let paused = counts[FlowProcessEffectiveState::PausedManual];
        let stopped = counts[FlowProcessEffectiveState::StoppedAuto];
        let total = active + failing + paused + stopped;

        // Form final rollup result
        FlowProcessGroupRollup {
            total,
            active,
            failing,
            paused,
            stopped,
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
        filter: FlowProcessListFilter<'_>,
        order: FlowProcessOrder,
        limit: usize,
        offset: usize,
    ) -> Result<FlowProcessStateListing, InternalError> {
        let state = self.state.read().unwrap();

        // Apply filtering
        let mut matching_states = self.apply_filters(&state, &filter);

        // Store total count before pagination
        let total_count = matching_states.len();

        // Apply ordering
        self.apply_ordering(&mut matching_states, order);

        // Apply pagination
        let processes = self.apply_pagination(matching_states, limit, offset);

        Ok(FlowProcessStateListing {
            processes,
            total_count,
        })
    }

    /// Compute rollup for matching rows.
    async fn rollup_by_scope(
        &self,
        flow_scope_query: FlowScopeQuery,
        for_flow_types: Option<&[&'static str]>,
        effective_state_in: Option<&[FlowProcessEffectiveState]>,
    ) -> Result<FlowProcessGroupRollup, InternalError> {
        let state = self.state.read().unwrap();

        // Create a filter using the builder-style API with optional methods
        let filter = FlowProcessListFilter::for_scope(flow_scope_query)
            .for_flow_types_opt(for_flow_types)
            .with_effective_states_opt(effective_state_in);

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
    async fn insert_process(
        &self,
        flow_binding: FlowBinding,
        sort_key: String,
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
                sort_key,
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
