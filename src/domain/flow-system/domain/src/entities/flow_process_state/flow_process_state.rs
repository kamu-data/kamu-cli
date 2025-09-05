// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::EventID;
use thiserror::Error;

use crate::{FlowBinding, FlowProcessEffectiveState, FlowTriggerStopPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowProcessState {
    flow_binding: FlowBinding,
    sort_key: String,

    paused_manual: bool,
    stop_policy: FlowTriggerStopPolicy,

    consecutive_failures: u32,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    last_attempt_at: Option<DateTime<Utc>>,
    next_planned_at: Option<DateTime<Utc>>,

    effective_state: FlowProcessEffectiveState,

    updated_at: DateTime<Utc>,
    last_applied_trigger_event_id: EventID,
    last_applied_flow_event_id: EventID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowProcessState {
    pub fn new(
        current_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        sort_key: String,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
        trigger_event_id: EventID,
    ) -> Self {
        Self {
            flow_binding,
            sort_key,
            paused_manual,
            stop_policy,
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            last_attempt_at: None,
            next_planned_at: None,
            effective_state: FlowProcessEffectiveState::calculate(paused_manual, 0, stop_policy),
            updated_at: current_time,
            last_applied_trigger_event_id: trigger_event_id,
            last_applied_flow_event_id: EventID::new(0),
        }
    }

    pub fn from_storage_row(
        flow_binding: FlowBinding,
        sort_key: String,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
        consecutive_failures: u32,
        last_success_at: Option<DateTime<Utc>>,
        last_failure_at: Option<DateTime<Utc>>,
        last_attempt_at: Option<DateTime<Utc>>,
        next_planned_at: Option<DateTime<Utc>>,
        effective_state: FlowProcessEffectiveState,
        updated_at: DateTime<Utc>,
        last_applied_trigger_event_id: EventID,
        last_applied_flow_event_id: EventID,
    ) -> Self {
        let actual_effective_state =
            FlowProcessEffectiveState::calculate(paused_manual, consecutive_failures, stop_policy);

        assert_eq!(
            effective_state, actual_effective_state,
            "Inconsistent effective state in storage row"
        );

        Self::validate_timing_properties(
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
        );

        Self {
            flow_binding,
            sort_key,
            paused_manual,
            stop_policy,
            consecutive_failures,
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
            effective_state,
            updated_at,
            last_applied_trigger_event_id,
            last_applied_flow_event_id,
        }
    }

    // Inline getters for public-facing fields
    #[inline]
    pub fn flow_binding(&self) -> &FlowBinding {
        &self.flow_binding
    }

    #[inline]
    pub fn sort_key(&self) -> &str {
        &self.sort_key
    }

    #[inline]
    pub fn paused_manual(&self) -> bool {
        self.paused_manual
    }

    #[inline]
    pub fn stop_policy(&self) -> FlowTriggerStopPolicy {
        self.stop_policy
    }

    #[inline]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    #[inline]
    pub fn last_success_at(&self) -> Option<DateTime<Utc>> {
        self.last_success_at
    }

    #[inline]
    pub fn last_failure_at(&self) -> Option<DateTime<Utc>> {
        self.last_failure_at
    }

    #[inline]
    pub fn last_attempt_at(&self) -> Option<DateTime<Utc>> {
        self.last_attempt_at
    }

    #[inline]
    pub fn next_planned_at(&self) -> Option<DateTime<Utc>> {
        self.next_planned_at
    }

    #[inline]
    pub fn effective_state(&self) -> FlowProcessEffectiveState {
        self.effective_state
    }

    pub fn update_trigger_state(
        &mut self,
        current_time: DateTime<Utc>,
        paused_manual: Option<bool>,
        stop_policy: Option<FlowTriggerStopPolicy>,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_trigger_event_order(trigger_event_id)?;

        if let Some(paused_manual) = paused_manual {
            self.paused_manual = paused_manual;
        }
        if let Some(stop_policy) = stop_policy {
            self.stop_policy = stop_policy;
        }

        self.actualize_effective_state();

        self.updated_at = current_time;
        self.last_applied_trigger_event_id = trigger_event_id;

        Ok(())
    }

    pub fn on_success(
        &mut self,
        current_time: DateTime<Utc>,
        event_time: DateTime<Utc>,
        next_planned_at: Option<DateTime<Utc>>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_flow_event_order(flow_event_id)?;

        self.consecutive_failures = 0;
        self.last_success_at = Some(event_time);
        self.last_attempt_at = Some(event_time);
        self.next_planned_at = next_planned_at;

        self.actualize_effective_state();

        self.last_applied_flow_event_id = flow_event_id;
        self.updated_at = current_time;

        Ok(())
    }

    pub fn on_failure(
        &mut self,
        current_time: DateTime<Utc>,
        event_time: DateTime<Utc>,
        next_planned_at: Option<DateTime<Utc>>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_flow_event_order(flow_event_id)?;

        self.consecutive_failures += 1;
        self.last_failure_at = Some(event_time);
        self.last_attempt_at = Some(event_time);
        self.next_planned_at = next_planned_at;

        self.actualize_effective_state();

        self.last_applied_flow_event_id = flow_event_id;
        self.updated_at = current_time;

        Ok(())
    }

    fn actualize_effective_state(&mut self) {
        self.effective_state = FlowProcessEffectiveState::calculate(
            self.paused_manual,
            self.consecutive_failures,
            self.stop_policy,
        );
    }

    fn validate_trigger_event_order(
        &self,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        if trigger_event_id == self.last_applied_trigger_event_id {
            return Err(FlowProcessStateError::DuplicateTriggerEvent {
                last_applied: self.last_applied_trigger_event_id,
                received: trigger_event_id,
            });
        }

        if trigger_event_id <= self.last_applied_trigger_event_id {
            return Err(FlowProcessStateError::OutOfOrderTriggerEvent {
                last_applied: self.last_applied_trigger_event_id,
                received: trigger_event_id,
            });
        }

        Ok(())
    }

    fn validate_flow_event_order(
        &self,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        if flow_event_id == self.last_applied_flow_event_id {
            return Err(FlowProcessStateError::DuplicateFlowEvent {
                last_applied: self.last_applied_flow_event_id,
                received: flow_event_id,
            });
        }

        if flow_event_id <= self.last_applied_flow_event_id {
            return Err(FlowProcessStateError::OutOfOrderFlowEvent {
                last_applied: self.last_applied_flow_event_id,
                received: flow_event_id,
            });
        }

        Ok(())
    }

    fn validate_timing_properties(
        last_success_at: Option<DateTime<Utc>>,
        last_failure_at: Option<DateTime<Utc>>,
        last_attempt_at: Option<DateTime<Utc>>,
        next_planned_at: Option<DateTime<Utc>>,
    ) {
        match (last_success_at, last_failure_at, last_attempt_at) {
            (None, None, None) => {
                // No attempts yet, all should be None
            }
            (Some(success), None, Some(attempt)) => {
                assert_eq!(
                    attempt, success,
                    "last_attempt_at should match last_success_at if only success exists"
                );
            }
            (None, Some(failure), Some(attempt)) => {
                assert_eq!(
                    attempt, failure,
                    "last_attempt_at should match last_failure_at if only failure exists"
                );
            }
            (Some(success), Some(failure), Some(attempt)) => {
                let latest = if success > failure { success } else { failure };
                assert_eq!(
                    attempt, latest,
                    "last_attempt_at should match the latest of last_success_at or last_failure_at"
                );
            }
            _ => {
                panic!(
                    "Invalid combination of last_success_at, last_failure_at, and last_attempt_at"
                );
            }
        }

        if let (Some(next_planned), Some(last_attempt)) = (next_planned_at, last_attempt_at) {
            assert!(
                next_planned > last_attempt,
                "next_planned_at must be later than last_attempt_at"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum FlowProcessStateError {
    #[error("Duplicate trigger event: expected event ID > {last_applied}, got {received}")]
    DuplicateTriggerEvent {
        last_applied: EventID,
        received: EventID,
    },

    #[error("Out-of-order trigger event: expected event ID > {last_applied}, got {received}")]
    OutOfOrderTriggerEvent {
        last_applied: EventID,
        received: EventID,
    },

    #[error("Duplicate flow event: expected event ID > {last_applied}, got {received}")]
    DuplicateFlowEvent {
        last_applied: EventID,
        received: EventID,
    },

    #[error("Out-of-order flow event: expected event ID > {last_applied}, got {received}")]
    OutOfOrderFlowEvent {
        last_applied: EventID,
        received: EventID,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::*;
    use crate::{ConsecutiveFailuresCount, FlowBinding, FlowScope, FlowTriggerStopPolicy};

    fn make_test_flow_binding() -> FlowBinding {
        FlowBinding::new("test.flow.type", FlowScope::make_system_scope())
    }

    fn make_test_sort_key() -> String {
        "test_sort_key".to_string()
    }

    fn make_test_stop_policy_with_failures(count: u32) -> FlowTriggerStopPolicy {
        FlowTriggerStopPolicy::AfterConsecutiveFailures {
            failures_count: ConsecutiveFailuresCount::try_new(count).unwrap(),
        }
    }

    #[test]
    fn test_new_flow_process_state() {
        let current_time = Utc::now();
        let flow_binding = make_test_flow_binding();
        let sort_key = make_test_sort_key();
        let stop_policy = FlowTriggerStopPolicy::Never;
        let trigger_event_id = EventID::new(123);

        // Test creating a new state without manual pause
        let state = FlowProcessState::new(
            current_time,
            flow_binding.clone(),
            sort_key.clone(),
            false, // not paused manually
            stop_policy,
            trigger_event_id,
        );

        assert_eq!(state.flow_binding(), &flow_binding);
        assert_eq!(state.sort_key, sort_key);
        assert!(!state.paused_manual());
        assert_eq!(state.stop_policy(), stop_policy);
        assert_eq!(state.consecutive_failures(), 0);
        assert_eq!(state.last_success_at(), None);
        assert_eq!(state.last_failure_at(), None);
        assert_eq!(state.last_attempt_at(), None);
        assert_eq!(state.next_planned_at(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
        assert_eq!(state.updated_at, current_time);
        assert_eq!(state.last_applied_trigger_event_id, trigger_event_id);
        assert_eq!(state.last_applied_flow_event_id, EventID::new(0));
    }

    #[test]
    fn test_new_flow_process_state_with_manual_pause() {
        let current_time = Utc::now();
        let flow_binding = make_test_flow_binding();
        let sort_key = make_test_sort_key();
        let stop_policy = FlowTriggerStopPolicy::Never;
        let trigger_event_id = EventID::new(456);

        // Test creating a new state with manual pause
        let state = FlowProcessState::new(
            current_time,
            flow_binding.clone(),
            sort_key.clone(),
            true, // paused manually
            stop_policy,
            trigger_event_id,
        );

        assert!(state.paused_manual());
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
    }

    #[test]
    fn test_update_trigger_state_pause_only() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let new_trigger_event_id = EventID::new(2);

        // Update only the pause state
        state
            .update_trigger_state(
                update_time,
                Some(true), // pause manually
                None,       // keep same stop policy
                new_trigger_event_id,
            )
            .unwrap();

        assert!(state.paused_manual());
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.updated_at, update_time);
        assert_eq!(state.last_applied_trigger_event_id, new_trigger_event_id);
    }

    #[test]
    fn test_update_trigger_state_stop_policy_only() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let new_trigger_event_id = EventID::new(3);
        let new_stop_policy = make_test_stop_policy_with_failures(5);

        // Update only the stop policy
        state
            .update_trigger_state(
                update_time,
                None,                  // keep same pause state
                Some(new_stop_policy), // change stop policy
                new_trigger_event_id,
            )
            .unwrap();

        assert!(!state.paused_manual());
        assert_eq!(state.stop_policy, new_stop_policy);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
        assert_eq!(state.updated_at, update_time);
        assert_eq!(state.last_applied_trigger_event_id, new_trigger_event_id);
    }

    #[test]
    fn test_update_trigger_state_both_pause_and_policy() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let new_trigger_event_id = EventID::new(4);
        let new_stop_policy = make_test_stop_policy_with_failures(3);

        // Update both pause and stop policy
        state
            .update_trigger_state(
                update_time,
                Some(true),            // pause manually
                Some(new_stop_policy), // change stop policy
                new_trigger_event_id,
            )
            .unwrap();

        assert!(state.paused_manual());
        assert_eq!(state.stop_policy, new_stop_policy);
        // Manual pause takes precedence
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.updated_at, update_time);
        assert_eq!(state.last_applied_trigger_event_id, new_trigger_event_id);
    }

    #[test]
    fn test_on_success_resets_failures() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(3),
            EventID::new(1),
        );

        // Simulate some failures first
        state.consecutive_failures = 2;
        state.effective_state = FlowProcessEffectiveState::Failing;

        let current_time = Utc::now() + Duration::minutes(10);
        let event_time = Utc::now() + Duration::minutes(9);
        let next_planned = Some(Utc::now() + Duration::hours(1));
        let flow_event_id = EventID::new(100);

        state
            .on_success(current_time, event_time, next_planned, flow_event_id)
            .unwrap();

        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.last_success_at(), Some(event_time));
        assert_eq!(state.last_attempt_at, Some(event_time));
        assert_eq!(state.next_planned_at, next_planned);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
        assert_eq!(state.last_applied_flow_event_id, flow_event_id);
        assert_eq!(state.updated_at, current_time);
    }

    #[test]
    fn test_on_failure_increments_failures() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(3),
            EventID::new(1),
        );

        let current_time = Utc::now() + Duration::minutes(10);
        let event_time = Utc::now() + Duration::minutes(9);
        let next_planned = Some(Utc::now() + Duration::hours(1));
        let flow_event_id = EventID::new(200);

        state
            .on_failure(current_time, event_time, next_planned, flow_event_id)
            .unwrap();

        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.last_failure_at(), Some(event_time));
        assert_eq!(state.last_attempt_at, Some(event_time));
        assert_eq!(state.next_planned_at, next_planned);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);
        assert_eq!(state.last_applied_flow_event_id, flow_event_id);
        assert_eq!(state.updated_at, current_time);
    }

    #[test]
    fn test_on_failure_transitions_to_stopped_auto() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(3),
            EventID::new(1),
        );

        // Simulate 2 failures already
        state.consecutive_failures = 2;
        state.effective_state = FlowProcessEffectiveState::Failing;

        let current_time = Utc::now() + Duration::minutes(10);
        let event_time = Utc::now() + Duration::minutes(9);
        let flow_event_id = EventID::new(300);

        // This third failure should trigger auto-stop
        state
            .on_failure(current_time, event_time, None, flow_event_id)
            .unwrap();

        assert_eq!(state.consecutive_failures, 3);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
    }

    #[test]
    fn test_on_failure_with_never_policy_stays_failing() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let current_time = Utc::now() + Duration::minutes(10);
        let event_time = Utc::now() + Duration::minutes(9);

        // Even with many failures, should stay in Failing state with Never policy
        for i in 1u32..=10u32 {
            state
                .on_failure(
                    current_time,
                    event_time,
                    None,
                    EventID::new(400 + i64::from(i)),
                )
                .unwrap();
            assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);
            assert_eq!(state.consecutive_failures, i);
        }
    }

    #[test]
    fn test_manual_pause_overrides_auto_stop() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(2),
            EventID::new(1),
        );

        // Simulate enough failures to trigger auto-stop
        state.consecutive_failures = 3;
        state.effective_state = FlowProcessEffectiveState::StoppedAuto;

        let update_time = Utc::now() + Duration::minutes(1);
        let trigger_event_id = EventID::new(2);

        // Manual pause should override auto-stop
        state
            .update_trigger_state(
                update_time,
                Some(true), // pause manually
                None,
                trigger_event_id,
            )
            .unwrap();

        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.consecutive_failures, 3); // failures count preserved
    }

    #[test]
    fn test_unpause_with_existing_failures() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            true, // start paused
            make_test_stop_policy_with_failures(2),
            EventID::new(1),
        );

        // Simulate failures while paused (this would happen if failures occurred before
        // pause)
        state.consecutive_failures = 3;
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let trigger_event_id = EventID::new(2);

        // Unpause - should transition to StoppedAuto due to existing failures
        state
            .update_trigger_state(
                update_time,
                Some(false), // unpause
                None,
                trigger_event_id,
            )
            .unwrap();

        assert!(!state.paused_manual());
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
    }

    #[test]
    fn test_actualize_effective_state_idempotent() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(3),
            EventID::new(1),
        );

        let initial_state = state.effective_state();

        // Calling actualize_effective_state multiple times should not change anything
        state.actualize_effective_state();
        assert_eq!(state.effective_state(), initial_state);

        state.actualize_effective_state();
        assert_eq!(state.effective_state(), initial_state);
    }

    #[test]
    fn test_success_after_failure_recovery() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(5),
            EventID::new(1),
        );

        let base_time = Utc::now();

        // First failure
        state
            .on_failure(
                base_time + Duration::minutes(1),
                base_time + Duration::minutes(1),
                None,
                EventID::new(10),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // Second failure
        state
            .on_failure(
                base_time + Duration::minutes(2),
                base_time + Duration::minutes(2),
                None,
                EventID::new(11),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // Success should reset everything back to Active
        state
            .on_success(
                base_time + Duration::minutes(3),
                base_time + Duration::minutes(3),
                Some(base_time + Duration::hours(1)),
                EventID::new(12),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
        assert_eq!(
            state.last_success_at(),
            Some(base_time + Duration::minutes(3))
        );
        assert_eq!(
            state.last_failure_at(),
            Some(base_time + Duration::minutes(2))
        );
    }

    #[test]
    fn test_state_consistency_after_multiple_operations() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(3),
            EventID::new(1),
        );

        let base_time = Utc::now();

        // Complex sequence of operations

        // 1. Initial success
        state
            .on_success(
                base_time + Duration::minutes(1),
                base_time + Duration::minutes(1),
                Some(base_time + Duration::hours(1)),
                EventID::new(10),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);

        // 2. Manual pause
        state
            .update_trigger_state(
                base_time + Duration::minutes(2),
                Some(true),
                None,
                EventID::new(2),
            )
            .unwrap();
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );

        // 3. Failure while paused (should stay paused)
        state
            .on_failure(
                base_time + Duration::minutes(3),
                base_time + Duration::minutes(3),
                None,
                EventID::new(11),
            )
            .unwrap();
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.consecutive_failures, 1);

        // 4. Unpause with existing failure
        state
            .update_trigger_state(
                base_time + Duration::minutes(4),
                Some(false),
                None,
                EventID::new(3),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 5. Change stop policy to Never
        state
            .update_trigger_state(
                base_time + Duration::minutes(5),
                None,
                Some(FlowTriggerStopPolicy::Never),
                EventID::new(4),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 6. Add more failures (should stay Failing with Never policy)
        state
            .on_failure(
                base_time + Duration::minutes(6),
                base_time + Duration::minutes(6),
                None,
                EventID::new(12),
            )
            .unwrap();
        state
            .on_failure(
                base_time + Duration::minutes(7),
                base_time + Duration::minutes(7),
                None,
                EventID::new(13),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 3);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 7. Final success should reset everything
        state
            .on_success(
                base_time + Duration::minutes(8),
                base_time + Duration::minutes(8),
                Some(base_time + Duration::hours(2)),
                EventID::new(14),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
        assert_eq!(state.next_planned_at, Some(base_time + Duration::hours(2)));
    }

    #[test]
    fn test_duplicate_trigger_event_error() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(100),
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let duplicate_event_id = EventID::new(100); // Same as initial

        // Attempting to process the same trigger event ID should fail
        let result = state.update_trigger_state(update_time, Some(true), None, duplicate_event_id);

        assert!(result.is_err());
        match result.unwrap_err() {
            FlowProcessStateError::DuplicateTriggerEvent {
                last_applied,
                received,
            } => {
                assert_eq!(last_applied, EventID::new(100));
                assert_eq!(received, EventID::new(100));
            }
            _ => panic!("Expected DuplicateTriggerEvent error"),
        }
    }

    #[test]
    fn test_out_of_order_trigger_event_error() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(100),
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let older_event_id = EventID::new(50); // Lower than the initial 100

        // Attempting to process an older trigger event should fail
        let result = state.update_trigger_state(update_time, Some(true), None, older_event_id);

        assert!(result.is_err());
        match result.unwrap_err() {
            FlowProcessStateError::OutOfOrderTriggerEvent {
                last_applied,
                received,
            } => {
                assert_eq!(last_applied, EventID::new(100));
                assert_eq!(received, EventID::new(50));
            }
            _ => panic!("Expected OutOfOrderTriggerEvent error"),
        }
    }

    #[test]
    fn test_valid_trigger_event_sequence() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(100),
        );

        let update_time = Utc::now() + Duration::minutes(1);

        // Valid sequence: 100 -> 150 -> 200
        let result1 = state.update_trigger_state(update_time, Some(true), None, EventID::new(150));
        assert!(result1.is_ok());
        assert_eq!(state.last_applied_trigger_event_id, EventID::new(150));

        let result2 = state.update_trigger_state(update_time, Some(false), None, EventID::new(200));
        assert!(result2.is_ok());
        assert_eq!(state.last_applied_trigger_event_id, EventID::new(200));
    }

    #[test]
    fn test_duplicate_flow_event_error() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let current_time = Utc::now() + Duration::minutes(1);
        let event_time = Utc::now();
        let flow_event_id = EventID::new(500);

        // First success should work
        let result1 = state.on_success(current_time, event_time, None, flow_event_id);
        assert!(result1.is_ok());

        // Attempting to process the same flow event ID should fail
        let result2 = state.on_success(
            current_time + Duration::minutes(1),
            event_time + Duration::minutes(1),
            None,
            flow_event_id, // Same event ID
        );

        assert!(result2.is_err());
        match result2.unwrap_err() {
            FlowProcessStateError::DuplicateFlowEvent {
                last_applied,
                received,
            } => {
                assert_eq!(last_applied, EventID::new(500));
                assert_eq!(received, EventID::new(500));
            }
            _ => panic!("Expected DuplicateFlowEvent error"),
        }
    }

    #[test]
    fn test_out_of_order_flow_event_error() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let current_time = Utc::now() + Duration::minutes(1);
        let event_time = Utc::now();
        let flow_event_id = EventID::new(500);

        // First success should work
        let result1 = state.on_success(current_time, event_time, None, flow_event_id);
        assert!(result1.is_ok());

        // Attempting to process an older flow event should fail
        let older_flow_event_id = EventID::new(300); // Lower than 500
        let result2 = state.on_failure(
            current_time + Duration::minutes(1),
            event_time + Duration::minutes(1),
            None,
            older_flow_event_id,
        );

        assert!(result2.is_err());
        match result2.unwrap_err() {
            FlowProcessStateError::OutOfOrderFlowEvent {
                last_applied,
                received,
            } => {
                assert_eq!(last_applied, EventID::new(500));
                assert_eq!(received, EventID::new(300));
            }
            _ => panic!("Expected OutOfOrderFlowEvent error"),
        }
    }

    #[test]
    fn test_valid_flow_event_sequence() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let current_time = Utc::now() + Duration::minutes(1);
        let event_time = Utc::now();

        // Valid sequence: 0 (initial) -> 100 -> 200 -> 350
        let result1 = state.on_success(current_time, event_time, None, EventID::new(100));
        assert!(result1.is_ok());
        assert_eq!(state.last_applied_flow_event_id, EventID::new(100));

        let result2 = state.on_failure(
            current_time + Duration::minutes(1),
            event_time + Duration::minutes(1),
            None,
            EventID::new(200),
        );
        assert!(result2.is_ok());
        assert_eq!(state.last_applied_flow_event_id, EventID::new(200));

        let result3 = state.on_success(
            current_time + Duration::minutes(2),
            event_time + Duration::minutes(2),
            None,
            EventID::new(350),
        );
        assert!(result3.is_ok());
        assert_eq!(state.last_applied_flow_event_id, EventID::new(350));
    }

    #[test]
    fn test_mixed_event_types_ordering() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(10), // trigger event
        );

        let current_time = Utc::now() + Duration::minutes(1);
        let event_time = Utc::now();

        // Process a flow event
        let result1 = state.on_success(
            current_time,
            event_time,
            None,
            EventID::new(50), // flow event
        );
        assert!(result1.is_ok());

        // Process a trigger event (different sequence)
        let result2 = state.update_trigger_state(
            current_time + Duration::minutes(1),
            Some(true),
            None,
            EventID::new(20), // Higher than initial trigger event (10)
        );
        assert!(result2.is_ok());

        // Try to process an older flow event (should fail against flow sequence)
        let result3 = state.on_failure(
            current_time + Duration::minutes(2),
            event_time + Duration::minutes(2),
            None,
            EventID::new(30), // Lower than last flow event (50)
        );
        assert!(result3.is_err());
        match result3.unwrap_err() {
            FlowProcessStateError::OutOfOrderFlowEvent { .. } => {
                // Expected
            }
            _ => panic!("Expected OutOfOrderFlowEvent error"),
        }

        // Try to process an older trigger event (should fail against trigger sequence)
        let result4 = state.update_trigger_state(
            current_time + Duration::minutes(3),
            Some(false),
            None,
            EventID::new(15), // Lower than last trigger event (20)
        );
        assert!(result4.is_err());
        match result4.unwrap_err() {
            FlowProcessStateError::OutOfOrderTriggerEvent { .. } => {
                // Expected
            }
            _ => panic!("Expected OutOfOrderTriggerEvent error"),
        }
    }

    #[test]
    fn test_state_unchanged_on_event_ordering_error() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(3),
            EventID::new(100),
        );

        // Record initial state
        let initial_consecutive_failures = state.consecutive_failures();
        let initial_effective_state = state.effective_state();
        let initial_updated_at = state.updated_at;
        let initial_trigger_event_id = state.last_applied_trigger_event_id;
        let initial_flow_event_id = state.last_applied_flow_event_id;

        let current_time = Utc::now() + Duration::minutes(1);

        // Attempt an invalid trigger event - state should remain unchanged
        let result = state.update_trigger_state(
            current_time,
            Some(true),
            None,
            EventID::new(50), // Lower than initial 100
        );
        assert!(result.is_err());

        // Verify state is unchanged
        assert_eq!(state.consecutive_failures(), initial_consecutive_failures);
        assert_eq!(state.effective_state(), initial_effective_state);
        assert_eq!(state.updated_at, initial_updated_at);
        assert_eq!(
            state.last_applied_trigger_event_id,
            initial_trigger_event_id
        );
        assert_eq!(state.last_applied_flow_event_id, initial_flow_event_id);

        // Attempt an invalid flow event - state should remain unchanged
        let result2 = state.on_failure(
            current_time,
            current_time,
            None,
            EventID::new(-5), // Negative, definitely lower than initial 0
        );
        assert!(result2.is_err());

        // Verify state is still unchanged
        assert_eq!(state.consecutive_failures(), initial_consecutive_failures);
        assert_eq!(state.effective_state(), initial_effective_state);
        assert_eq!(state.updated_at, initial_updated_at);
        assert_eq!(
            state.last_applied_trigger_event_id,
            initial_trigger_event_id
        );
        assert_eq!(state.last_applied_flow_event_id, initial_flow_event_id);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
