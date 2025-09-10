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
use internal_error::InternalError;
use thiserror::Error;

use crate::{FlowBinding, FlowProcessEffectiveState, FlowTriggerStopPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
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
        // Ensure sort_key is lowercase for efficient filtering
        debug_assert_eq!(
            sort_key.to_lowercase(),
            sort_key,
            "sort_key must be lowercase for efficient filtering"
        );

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

    pub fn rehydrate_from_snapshot(
        flow_binding: FlowBinding,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
        consecutive_failures: u32,
        last_success_at: Option<DateTime<Utc>>,
        last_failure_at: Option<DateTime<Utc>>,
        last_attempt_at: Option<DateTime<Utc>>,
        next_planned_at: Option<DateTime<Utc>>,
        effective_state: FlowProcessEffectiveState,
        sort_key: String,
        updated_at: DateTime<Utc>,
        last_applied_trigger_event_id: EventID,
        last_applied_flow_event_id: EventID,
    ) -> Result<Self, InternalError> {
        // Ensure sort_key is lowercase for efficient filtering
        debug_assert_eq!(
            sort_key.to_lowercase(),
            sort_key,
            "sort_key must be lowercase for efficient filtering"
        );

        debug_assert_eq!(
            effective_state,
            FlowProcessEffectiveState::calculate(paused_manual, consecutive_failures, stop_policy,),
            "Inconsistent effective state in storage row"
        );

        Self::validate_timing_properties(
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
        );

        Ok(Self {
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
        })
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

    #[inline]
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[inline]
    pub fn last_applied_trigger_event_id(&self) -> EventID {
        self.last_applied_trigger_event_id
    }

    #[inline]
    pub fn last_applied_flow_event_id(&self) -> EventID {
        self.last_applied_flow_event_id
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
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_flow_event_order(flow_event_id)?;

        self.consecutive_failures = 0;
        self.last_success_at = Some(event_time);
        self.last_attempt_at = Some(event_time);

        self.handle_next_planned_at_update(event_time);
        self.actualize_effective_state();

        self.last_applied_flow_event_id = flow_event_id;
        self.updated_at = current_time;

        Ok(())
    }

    pub fn on_failure(
        &mut self,
        current_time: DateTime<Utc>,
        event_time: DateTime<Utc>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_flow_event_order(flow_event_id)?;

        self.consecutive_failures += 1;
        self.last_failure_at = Some(event_time);
        self.last_attempt_at = Some(event_time);

        self.handle_next_planned_at_update(event_time);
        self.actualize_effective_state();

        self.last_applied_flow_event_id = flow_event_id;
        self.updated_at = current_time;

        Ok(())
    }

    pub fn on_scheduled(
        &mut self,
        current_time: DateTime<Utc>,
        next_planned_at: DateTime<Utc>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_flow_event_order(flow_event_id)?;

        self.next_planned_at = Some(next_planned_at);

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

        // Clear next_planned_at when flow process is not running (stopped or paused)
        if !self.effective_state.is_running() {
            self.next_planned_at = None;
        }
    }

    fn handle_next_planned_at_update(&mut self, event_time: DateTime<Utc>) {
        // Clear next_planned_at only if the old value is in the past relative to the
        // event time
        if self
            .next_planned_at
            .is_some_and(|existing_planned_at| existing_planned_at <= event_time)
        {
            self.next_planned_at = None;
        }
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
                debug_assert_eq!(
                    attempt, success,
                    "last_attempt_at should match last_success_at if only success exists"
                );
            }
            (None, Some(failure), Some(attempt)) => {
                debug_assert_eq!(
                    attempt, failure,
                    "last_attempt_at should match last_failure_at if only failure exists"
                );
            }
            (Some(success), Some(failure), Some(attempt)) => {
                let latest = if success > failure { success } else { failure };
                debug_assert_eq!(
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
            debug_assert!(
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

        // Test with manual pause
        let paused_state = FlowProcessState::new(
            current_time,
            flow_binding,
            sort_key,
            true, // paused manually
            stop_policy,
            EventID::new(456),
        );

        assert!(paused_state.paused_manual());
        assert_eq!(
            paused_state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
    }

    #[test]
    fn test_update_trigger_state() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let new_stop_policy = make_test_stop_policy_with_failures(3);

        // Test updating both pause and stop policy
        state
            .update_trigger_state(
                update_time,
                Some(true),            // pause manually
                Some(new_stop_policy), // change stop policy
                EventID::new(2),
            )
            .unwrap();

        assert!(state.paused_manual());
        assert_eq!(state.stop_policy, new_stop_policy);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.updated_at, update_time);
        assert_eq!(state.last_applied_trigger_event_id, EventID::new(2));

        // Test updating only pause state
        state
            .update_trigger_state(
                update_time + Duration::minutes(1),
                Some(false), // unpause
                None,        // keep same stop policy
                EventID::new(3),
            )
            .unwrap();

        assert!(!state.paused_manual());
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
    }

    #[test]
    fn test_flow_execution_events() {
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

        // Test failure increments failures count
        state
            .on_failure(current_time, event_time, EventID::new(100))
            .unwrap();

        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.last_failure_at(), Some(event_time));
        assert_eq!(state.last_attempt_at, Some(event_time));
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // Test success resets failures count
        state
            .on_success(
                current_time + Duration::minutes(1),
                event_time + Duration::minutes(1),
                EventID::new(101),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(
            state.last_success_at(),
            Some(event_time + Duration::minutes(1))
        );
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);

        // Test multiple failures leading to auto-stop
        state.consecutive_failures = 2; // Simulate existing failures
        state.effective_state = FlowProcessEffectiveState::Failing;

        state
            .on_failure(
                current_time + Duration::minutes(2),
                event_time + Duration::minutes(2),
                EventID::new(102),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 3);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
    }

    #[test]
    fn test_stop_policy_behavior() {
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

        // With Never policy, failures should keep state as Failing
        for i in 1u32..=5u32 {
            state
                .on_failure(current_time, event_time, EventID::new(100 + i64::from(i)))
                .unwrap();
            assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);
            assert_eq!(state.consecutive_failures, i);
        }
    }

    #[test]
    fn test_pause_interactions() {
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

        // Manual pause should override auto-stop
        state
            .update_trigger_state(
                Utc::now() + Duration::minutes(1),
                Some(true), // pause manually
                None,
                EventID::new(2),
            )
            .unwrap();

        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.consecutive_failures, 3); // failures count preserved

        // Unpause should transition back to StoppedAuto due to existing failures
        state
            .update_trigger_state(
                Utc::now() + Duration::minutes(2),
                Some(false), // unpause
                None,
                EventID::new(3),
            )
            .unwrap();

        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
    }

    #[test]
    fn test_event_ordering_validation() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(100),
        );

        let update_time = Utc::now() + Duration::minutes(1);

        // Test duplicate trigger event error
        let result = state.update_trigger_state(update_time, Some(true), None, EventID::new(100));
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::DuplicateTriggerEvent { .. }
        ));

        // Test out-of-order trigger event error
        let result = state.update_trigger_state(update_time, Some(true), None, EventID::new(50));
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::OutOfOrderTriggerEvent { .. }
        ));

        // Test valid trigger event sequence
        let result = state.update_trigger_state(update_time, Some(true), None, EventID::new(150));
        assert!(result.is_ok());
        assert_eq!(state.last_applied_trigger_event_id, EventID::new(150));

        // Test flow event ordering
        let current_time = Utc::now() + Duration::minutes(1);
        let event_time = Utc::now();

        // Valid flow event
        let result = state.on_success(current_time, event_time, EventID::new(200));
        assert!(result.is_ok());

        // Duplicate flow event error
        let result = state.on_success(current_time, event_time, EventID::new(200));
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::DuplicateFlowEvent { .. }
        ));

        // Out-of-order flow event error
        let result = state.on_failure(current_time, event_time, EventID::new(100));
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::OutOfOrderFlowEvent { .. }
        ));

        // State should remain unchanged on ordering errors
        assert_eq!(state.last_applied_flow_event_id, EventID::new(200));
        assert_eq!(state.last_applied_trigger_event_id, EventID::new(150));
    }

    #[test]
    fn test_flow_scheduling() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            FlowTriggerStopPolicy::Never,
            EventID::new(1),
        );

        let base_time = Utc::now();
        let scheduled_time = base_time + Duration::hours(1);

        // Test basic scheduling
        state
            .on_scheduled(base_time, scheduled_time, EventID::new(100))
            .unwrap();

        assert_eq!(state.next_planned_at, Some(scheduled_time));
        assert_eq!(state.last_applied_flow_event_id, EventID::new(100));

        // Test that past planned times are cleared on execution events
        state.next_planned_at = Some(base_time + Duration::minutes(30));
        state
            .on_success(
                base_time + Duration::hours(2),
                base_time + Duration::hours(2),
                EventID::new(101),
            )
            .unwrap();
        assert_eq!(state.next_planned_at, None); // Cleared because it was in the past

        // Test that future planned times are preserved
        let future_time = base_time + Duration::hours(3);
        state.next_planned_at = Some(future_time);
        state
            .on_success(
                base_time + Duration::hours(1),
                base_time + Duration::hours(1),
                EventID::new(102),
            )
            .unwrap();
        assert_eq!(state.next_planned_at, Some(future_time)); // Preserved because it's in the future

        // Test that non-running states clear planned time
        state
            .on_scheduled(base_time, base_time + Duration::hours(2), EventID::new(103))
            .unwrap();
        state
            .update_trigger_state(
                base_time + Duration::minutes(10),
                Some(true), // pause
                None,
                EventID::new(2),
            )
            .unwrap();
        assert_eq!(state.next_planned_at, None); // Cleared because state is paused
    }

    #[test]
    fn test_comprehensive_state_transitions() {
        let mut state = FlowProcessState::new(
            Utc::now(),
            make_test_flow_binding(),
            make_test_sort_key(),
            false,
            make_test_stop_policy_with_failures(3),
            EventID::new(1),
        );

        let base_time = Utc::now();

        // 1. Success -> Active state
        state
            .on_success(
                base_time + Duration::minutes(1),
                base_time + Duration::minutes(1),
                EventID::new(10),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);

        // 2. Manual pause -> PausedManual (overrides everything)
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

        // 3. Failure while paused -> stays PausedManual
        state
            .on_failure(
                base_time + Duration::minutes(3),
                base_time + Duration::minutes(3),
                EventID::new(11),
            )
            .unwrap();
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.consecutive_failures, 1);

        // 4. Unpause -> transitions to Failing due to existing failures
        state
            .update_trigger_state(
                base_time + Duration::minutes(4),
                Some(false),
                None,
                EventID::new(3),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 5. Change policy to Never -> stays Failing
        state
            .update_trigger_state(
                base_time + Duration::minutes(5),
                None,
                Some(FlowTriggerStopPolicy::Never),
                EventID::new(4),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 6. Add more failures -> stays Failing with Never policy
        state
            .on_failure(
                base_time + Duration::minutes(6),
                base_time + Duration::minutes(6),
                EventID::new(12),
            )
            .unwrap();
        state
            .on_failure(
                base_time + Duration::minutes(7),
                base_time + Duration::minutes(7),
                EventID::new(13),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 3);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 7. Success resets everything -> Active
        state
            .on_success(
                base_time + Duration::minutes(8),
                base_time + Duration::minutes(8),
                EventID::new(14),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
