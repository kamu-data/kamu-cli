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

use super::flow_process_event::{
    FlowProcessEvent,
    FlowProcessEventAutoStopped,
    FlowProcessEventEffectiveStateChanged,
};
use crate::{
    FlowBinding,
    FlowOutcome,
    FlowProcessAutoStopReason,
    FlowProcessEffectiveState,
    FlowProcessUserIntent,
    FlowTriggerStopPolicy,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FlowProcessState {
    flow_binding: FlowBinding,

    user_intent: FlowProcessUserIntent,
    stop_policy: FlowTriggerStopPolicy,

    consecutive_failures: u32,
    last_success_at: Option<DateTime<Utc>>,
    last_failure_at: Option<DateTime<Utc>>,
    last_attempt_at: Option<DateTime<Utc>>,
    next_planned_at: Option<DateTime<Utc>>,
    paused_at: Option<DateTime<Utc>>,
    running_since: Option<DateTime<Utc>>,
    auto_stopped_at: Option<DateTime<Utc>>,

    effective_state: FlowProcessEffectiveState,
    auto_stopped_reason: Option<FlowProcessAutoStopReason>,

    updated_at: DateTime<Utc>,
    last_applied_event_id: EventID,

    pending_events: Vec<FlowProcessEvent>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowProcessState {
    pub fn new(
        event_id: EventID,
        current_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        user_intent: FlowProcessUserIntent,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Self {
        Self {
            flow_binding,
            user_intent,
            stop_policy,
            consecutive_failures: 0,
            last_success_at: None,
            last_failure_at: None,
            last_attempt_at: None,
            next_planned_at: None,
            paused_at: None,
            running_since: None,
            auto_stopped_at: None,
            effective_state: FlowProcessEffectiveState::calculate(user_intent, 0, stop_policy),
            auto_stopped_reason: None,
            updated_at: current_time,
            last_applied_event_id: event_id,
            pending_events: Vec::new(),
        }
    }

    pub fn unconfigured(current_time: DateTime<Utc>, flow_binding: FlowBinding) -> Self {
        Self::new(
            EventID::new(0),
            current_time,
            flow_binding,
            FlowProcessUserIntent::Undefined, // no explicit user intent yet
            FlowTriggerStopPolicy::default(),
        )
    }

    pub fn rehydrate_from_snapshot(
        flow_binding: FlowBinding,
        user_intent: FlowProcessUserIntent,
        stop_policy: FlowTriggerStopPolicy,
        consecutive_failures: u32,
        last_success_at: Option<DateTime<Utc>>,
        last_failure_at: Option<DateTime<Utc>>,
        last_attempt_at: Option<DateTime<Utc>>,
        next_planned_at: Option<DateTime<Utc>>,
        paused_at: Option<DateTime<Utc>>,
        running_since: Option<DateTime<Utc>>,
        auto_stopped_at: Option<DateTime<Utc>>,
        effective_state: FlowProcessEffectiveState,
        auto_stopped_reason: Option<FlowProcessAutoStopReason>,
        updated_at: DateTime<Utc>,
        last_applied_event_id: EventID,
    ) -> Result<Self, InternalError> {
        // Only validate effective state if not auto-stopped, as auto-stopped state
        // overrides the calculated state
        if auto_stopped_reason.is_none() {
            debug_assert_eq!(
                effective_state,
                FlowProcessEffectiveState::calculate(
                    user_intent,
                    consecutive_failures,
                    stop_policy,
                ),
                "Inconsistent effective state in storage row"
            );
        } else {
            debug_assert_eq!(
                effective_state,
                FlowProcessEffectiveState::StoppedAuto,
                "Auto-stopped flow must have StoppedAuto effective state"
            );
        }

        Self::validate_timing_properties(
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
        );

        Ok(Self {
            flow_binding,
            user_intent,
            stop_policy,
            consecutive_failures,
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
            paused_at,
            running_since,
            auto_stopped_at,
            effective_state,
            auto_stopped_reason,
            updated_at,
            last_applied_event_id,
            pending_events: Vec::new(),
        })
    }

    // Inline getters for public-facing fields
    #[inline]
    pub fn flow_binding(&self) -> &FlowBinding {
        &self.flow_binding
    }

    #[inline]
    pub fn user_intent(&self) -> FlowProcessUserIntent {
        self.user_intent
    }

    /// Returns true if the flow process is manually paused by the user
    #[inline]
    pub fn is_paused_manually(&self) -> bool {
        self.user_intent == FlowProcessUserIntent::Paused
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
    pub fn paused_at(&self) -> Option<DateTime<Utc>> {
        self.paused_at
    }

    #[inline]
    pub fn running_since(&self) -> Option<DateTime<Utc>> {
        self.running_since
    }

    #[inline]
    pub fn auto_stopped_at(&self) -> Option<DateTime<Utc>> {
        self.auto_stopped_at
    }

    #[inline]
    pub fn effective_state(&self) -> FlowProcessEffectiveState {
        self.effective_state
    }

    #[inline]
    pub fn auto_stopped_reason(&self) -> Option<FlowProcessAutoStopReason> {
        self.auto_stopped_reason
    }

    #[inline]
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[inline]
    pub fn last_applied_event_id(&self) -> EventID {
        self.last_applied_event_id
    }

    /// Get and clear all pending domain events
    pub fn take_pending_events(&mut self) -> Vec<FlowProcessEvent> {
        std::mem::take(&mut self.pending_events)
    }

    pub fn update_trigger_state(
        &mut self,
        event_id: EventID,
        current_time: DateTime<Utc>,
        paused: bool,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_event_order(event_id)?;

        let new_user_intent = if paused {
            FlowProcessUserIntent::Paused
        } else {
            FlowProcessUserIntent::Enabled
        };

        // Check if we're resuming from a stopped state
        // (transition from STOPPED -> ACTIVE)
        let was_stopped_auto = self.effective_state == FlowProcessEffectiveState::StoppedAuto;
        let is_resuming = was_stopped_auto && !paused;

        // Track when flow was paused or resumed
        let was_paused = self.user_intent == FlowProcessUserIntent::Paused;
        if paused && !was_paused {
            // Transitioning to paused state
            self.paused_at = Some(current_time);
        } else if !paused && was_paused {
            // Resuming from paused state
            self.paused_at = None;
        }

        self.user_intent = new_user_intent;
        self.stop_policy = stop_policy;

        // Reset consecutive failures and clear auto-stop state when resuming from
        // stopped state This gives users a fresh start after they've corrected
        // issues
        if is_resuming {
            self.consecutive_failures = 0;
            self.auto_stopped_reason = None;
            self.auto_stopped_at = None;
        }

        // If we were auto-stopped but now manually changing the trigger state,
        // clear the auto-stop reason since it's no longer relevant
        if was_stopped_auto && !is_resuming {
            self.auto_stopped_reason = None;
            self.auto_stopped_at = None;
        }

        // Capture old state before actualization
        let old_state = self.effective_state;

        // Use simple actualize since this is trigger state update, not flow outcome
        self.actualize_effective_state_simple();

        // Emit state change event if needed
        if self.effective_state != old_state {
            self.pending_events
                .push(FlowProcessEvent::EffectiveStateChanged(
                    FlowProcessEventEffectiveStateChanged {
                        old_state,
                        new_state: self.effective_state,
                        event_time: current_time,
                    },
                ));
        }

        self.updated_at = current_time;
        self.last_applied_event_id = event_id;

        Ok(())
    }

    pub fn on_flow_outcome(
        &mut self,
        event_id: EventID,
        current_time: DateTime<Utc>,
        event_time: DateTime<Utc>,
        flow_outcome: &FlowOutcome,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_event_order(event_id)?;

        // Capture old state for event emission
        let old_state = self.effective_state;
        let old_auto_stopped = self.auto_stopped_reason.is_some();

        match flow_outcome {
            FlowOutcome::Success(_) => {
                self.last_success_at = Some(event_time);
                self.last_attempt_at = Some(event_time);
                // Flow completed, no longer running
                self.running_since = None;

                // Only enabled flows participate in auto-stop logic
                if self.user_intent == FlowProcessUserIntent::Enabled {
                    self.consecutive_failures = 0;
                    // Clear auto-stop reason on success as the flow is working again
                    self.auto_stopped_reason = None;
                    self.auto_stopped_at = None;
                }
            }
            FlowOutcome::Failed(task_error) => {
                self.last_failure_at = Some(event_time);
                self.last_attempt_at = Some(event_time);
                // Flow completed (with failure), no longer running
                self.running_since = None;

                // Only enabled flows participate in auto-stop logic, and only if not already
                // auto-stopped
                if self.user_intent == FlowProcessUserIntent::Enabled
                    && self.auto_stopped_reason.is_none()
                {
                    self.consecutive_failures += 1;

                    // Check if this is an unrecoverable failure that should auto-stop immediately
                    if !task_error.recoverable {
                        self.auto_stopped_reason =
                            Some(FlowProcessAutoStopReason::UnrecoverableFailure);
                        self.auto_stopped_at = Some(event_time);
                    }
                    // For recoverable failures, the stop policy will be
                    // evaluated
                    // in actualize_effective_state
                }
            }
            FlowOutcome::Aborted => {
                // Unexpected, we don't track aborted flows in this projection
                unreachable!()
            }
        }

        self.handle_next_planned_at_update(event_time);
        self.actualize_effective_state_with_auto_stop_check(event_time);

        // Emit events for state changes
        if self.effective_state != old_state {
            self.pending_events
                .push(FlowProcessEvent::EffectiveStateChanged(
                    FlowProcessEventEffectiveStateChanged {
                        old_state,
                        new_state: self.effective_state,
                        event_time,
                    },
                ));
        }

        // Emit auto-stop event if just auto-stopped
        if !old_auto_stopped && self.auto_stopped_reason.is_some() {
            self.pending_events
                .push(FlowProcessEvent::AutoStopped(FlowProcessEventAutoStopped {
                    reason: self.auto_stopped_reason.unwrap(),
                    event_time,
                }));
        }

        self.last_applied_event_id = event_id;
        self.updated_at = current_time;

        Ok(())
    }

    pub fn on_scheduled(
        &mut self,
        event_id: EventID,
        current_time: DateTime<Utc>,
        next_planned_at: DateTime<Utc>,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_event_order(event_id)?;

        // Only enabled flows participate in automatic scheduling
        if self.user_intent == FlowProcessUserIntent::Enabled {
            self.next_planned_at = Some(next_planned_at);
        }

        self.last_applied_event_id = event_id;
        self.updated_at = current_time;

        Ok(())
    }

    pub fn on_running(
        &mut self,
        event_id: EventID,
        current_time: DateTime<Utc>,
        started_at: DateTime<Utc>,
    ) -> Result<(), FlowProcessStateError> {
        self.validate_event_order(event_id)?;

        // Clear next_planned_at only if the old value is in the past relative to the
        // started_at time
        self.handle_next_planned_at_update(started_at);

        // Track when flow started running
        self.running_since = Some(started_at);

        self.last_applied_event_id = event_id;
        self.updated_at = current_time;

        Ok(())
    }

    fn actualize_effective_state_with_auto_stop_check(&mut self, event_time: DateTime<Utc>) {
        // If we already have an auto-stop reason (like unrecoverable failure), maintain
        // stopped state
        if self.auto_stopped_reason.is_some() {
            self.effective_state = FlowProcessEffectiveState::StoppedAuto;
        } else if self.user_intent == FlowProcessUserIntent::Enabled {
            // Only enabled flows should consider stop policy for auto-stopping
            let should_auto_stop_per_policy = if self.consecutive_failures > 0 {
                match self.stop_policy {
                    FlowTriggerStopPolicy::AfterConsecutiveFailures { failures_count }
                        if self.consecutive_failures >= failures_count.into_inner() =>
                    {
                        true
                    }
                    FlowTriggerStopPolicy::AfterConsecutiveFailures { .. }
                    | FlowTriggerStopPolicy::Never => false,
                }
            } else {
                false
            };

            if should_auto_stop_per_policy {
                self.auto_stopped_reason = Some(FlowProcessAutoStopReason::StopPolicy);
                self.auto_stopped_at = Some(event_time);
                self.effective_state = FlowProcessEffectiveState::StoppedAuto;
            } else {
                // Use normal calculation for enabled flows
                self.effective_state = FlowProcessEffectiveState::calculate(
                    self.user_intent,
                    self.consecutive_failures,
                    self.stop_policy,
                );
            }
        } else {
            // For non-enabled flows (undefined/paused), use normal calculation
            // This handles unconfigured and paused states correctly
            self.effective_state = FlowProcessEffectiveState::calculate(
                self.user_intent,
                self.consecutive_failures,
                self.stop_policy,
            );
        }

        // Clear next_planned_at when flow process is not running (stopped or paused)
        if !self.effective_state.is_running() {
            self.next_planned_at = None;
        }
    }

    fn actualize_effective_state_simple(&mut self) {
        // Simple version without auto-stop reason update (for trigger state updates)
        self.effective_state = FlowProcessEffectiveState::calculate(
            self.user_intent,
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

    fn validate_event_order(&self, event_id: EventID) -> Result<(), FlowProcessStateError> {
        if event_id == self.last_applied_event_id {
            return Err(FlowProcessStateError::DuplicateEvent {
                last_applied: self.last_applied_event_id,
                received: event_id,
            });
        }

        if event_id <= self.last_applied_event_id {
            return Err(FlowProcessStateError::OutOfOrderEvent {
                last_applied: self.last_applied_event_id,
                received: event_id,
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
                next_planned >= last_attempt,
                "next_planned_at ({next_planned}) must not be earlier than last_attempt_at \
                 ({last_attempt})"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum FlowProcessStateError {
    #[error("Duplicate event: expected event ID > {last_applied}, got {received}")]
    DuplicateEvent {
        last_applied: EventID,
        received: EventID,
    },

    #[error("Out-of-order event: expected event ID > {last_applied}, got {received}")]
    OutOfOrderEvent {
        last_applied: EventID,
        received: EventID,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use kamu_task_system::{TaskError, TaskResult};

    use super::*;
    use crate::{ConsecutiveFailuresCount, FlowBinding, FlowScope, FlowTriggerStopPolicy};

    fn make_test_flow_binding() -> FlowBinding {
        FlowBinding::new("test.flow.type", FlowScope::make_system_scope())
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
        let stop_policy = FlowTriggerStopPolicy::Never;
        let event_id = EventID::new(123);

        // Test creating a new state with enabled intent
        let state = FlowProcessState::new(
            event_id,
            current_time,
            flow_binding.clone(),
            FlowProcessUserIntent::Enabled,
            stop_policy,
        );

        assert_eq!(state.flow_binding(), &flow_binding);
        assert_eq!(state.user_intent(), FlowProcessUserIntent::Enabled);
        assert!(!state.is_paused_manually());
        assert_eq!(state.stop_policy(), stop_policy);
        assert_eq!(state.consecutive_failures(), 0);
        assert_eq!(state.last_success_at(), None);
        assert_eq!(state.last_failure_at(), None);
        assert_eq!(state.last_attempt_at(), None);
        assert_eq!(state.next_planned_at(), None);
        assert_eq!(state.paused_at(), None);
        assert_eq!(state.running_since(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
        assert_eq!(state.updated_at, current_time);
        assert_eq!(state.last_applied_event_id, event_id);

        // Test with paused intent
        let paused_state = FlowProcessState::new(
            EventID::new(456),
            current_time,
            flow_binding.clone(),
            FlowProcessUserIntent::Paused,
            stop_policy,
        );

        assert_eq!(paused_state.user_intent(), FlowProcessUserIntent::Paused);
        assert!(paused_state.is_paused_manually());
        assert_eq!(
            paused_state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );

        // Test with undefined intent
        let undefined_state = FlowProcessState::new(
            EventID::new(789),
            current_time,
            flow_binding,
            FlowProcessUserIntent::Undefined,
            stop_policy,
        );

        assert_eq!(
            undefined_state.user_intent(),
            FlowProcessUserIntent::Undefined
        );
        assert!(!undefined_state.is_paused_manually());
        assert_eq!(
            undefined_state.effective_state(),
            FlowProcessEffectiveState::Unconfigured
        );

        // Unconfigured state should not be running
        assert!(!undefined_state.effective_state().is_running());
    }

    #[test]
    fn test_update_trigger_state() {
        let mut state = FlowProcessState::new(
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            FlowTriggerStopPolicy::Never,
        );

        let update_time = Utc::now() + Duration::minutes(1);
        let new_stop_policy = make_test_stop_policy_with_failures(3);

        // Verify initially not paused
        assert_eq!(state.paused_at(), None);

        // Test updating both pause state and stop policy
        state
            .update_trigger_state(
                EventID::new(2),
                update_time,
                true,            // pause
                new_stop_policy, // change stop policy
            )
            .unwrap();

        assert_eq!(state.user_intent(), FlowProcessUserIntent::Paused);
        assert!(state.is_paused_manually());
        assert_eq!(state.stop_policy, new_stop_policy);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.paused_at(), Some(update_time));
        assert_eq!(state.updated_at, update_time);
        assert_eq!(state.last_applied_event_id, EventID::new(2));

        // Test updating to enabled state clears paused_at
        state
            .update_trigger_state(
                EventID::new(3),
                update_time + Duration::minutes(1),
                false,           // enable
                new_stop_policy, // keep same stop policy
            )
            .unwrap();

        assert_eq!(state.user_intent(), FlowProcessUserIntent::Enabled);
        assert!(!state.is_paused_manually());
        assert_eq!(state.paused_at(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
    }

    #[test]
    fn test_flow_execution_events() {
        let mut state = FlowProcessState::new(
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(3),
        );

        let current_time = Utc::now() + Duration::minutes(10);
        let event_time = Utc::now() + Duration::minutes(9);

        // Simulate flow running before outcome
        state.running_since = Some(event_time - Duration::minutes(5));

        // Test failure increments failures count and clears running_since
        state
            .on_flow_outcome(
                EventID::new(2),
                current_time,
                event_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.last_failure_at(), Some(event_time));
        assert_eq!(state.last_attempt_at, Some(event_time));
        assert_eq!(state.running_since(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // Simulate flow running again
        state.running_since = Some(event_time);

        // Test success resets failures count and clears running_since
        state
            .on_flow_outcome(
                EventID::new(3),
                current_time + Duration::minutes(1),
                event_time + Duration::minutes(1),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(
            state.last_success_at(),
            Some(event_time + Duration::minutes(1))
        );
        assert_eq!(state.running_since(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);

        // Test multiple failures leading to auto-stop
        state.consecutive_failures = 2; // Simulate existing failures
        state.effective_state = FlowProcessEffectiveState::Failing;

        state
            .on_flow_outcome(
                EventID::new(4),
                current_time + Duration::minutes(2),
                event_time + Duration::minutes(2),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
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
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            FlowTriggerStopPolicy::Never,
        );

        let current_time = Utc::now() + Duration::minutes(10);
        let event_time = Utc::now() + Duration::minutes(9);

        // With Never policy, failures should keep state as Failing
        for i in 1u32..=5u32 {
            state
                .on_flow_outcome(
                    EventID::new(1 + i64::from(i)),
                    current_time,
                    event_time,
                    &FlowOutcome::Failed(TaskError::empty_recoverable()),
                )
                .unwrap();
            assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);
            assert_eq!(state.consecutive_failures, i);
        }
    }

    #[test]
    fn test_pause_interactions() {
        let mut state = FlowProcessState::new(
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(2),
        );

        // Simulate enough failures to trigger auto-stop
        state.consecutive_failures = 3;
        state.effective_state = FlowProcessEffectiveState::StoppedAuto;

        // Manual pause should override auto-stop
        state
            .update_trigger_state(
                EventID::new(2),
                Utc::now() + Duration::minutes(1),
                true, // pause
                make_test_stop_policy_with_failures(2),
            )
            .unwrap();

        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.consecutive_failures, 3); // failures count preserved

        // Enable should transition back to StoppedAuto due to existing failures
        state
            .update_trigger_state(
                EventID::new(3),
                Utc::now() + Duration::minutes(2),
                false, // enable
                make_test_stop_policy_with_failures(2),
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
            EventID::new(100),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            FlowTriggerStopPolicy::Never,
        );

        let update_time = Utc::now() + Duration::minutes(1);

        // Test duplicate event error
        let result = state.update_trigger_state(
            EventID::new(100),
            update_time,
            true,
            FlowTriggerStopPolicy::Never,
        );
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::DuplicateEvent { .. }
        ));

        // Test out-of-order event error
        let result = state.update_trigger_state(
            EventID::new(50),
            update_time,
            true,
            FlowTriggerStopPolicy::Never,
        );
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::OutOfOrderEvent { .. }
        ));

        // Test valid event sequence
        let result = state.update_trigger_state(
            EventID::new(150),
            update_time,
            true,
            FlowTriggerStopPolicy::Never,
        );
        assert!(result.is_ok());
        assert_eq!(state.last_applied_event_id, EventID::new(150));

        // Test flow event ordering with unified stream
        let current_time = Utc::now() + Duration::minutes(1);
        let event_time = Utc::now();

        // Valid flow event
        let result = state.on_flow_outcome(
            EventID::new(200),
            current_time,
            event_time,
            &FlowOutcome::Success(TaskResult::empty()),
        );
        assert!(result.is_ok());

        // Duplicate flow event error
        let result = state.on_flow_outcome(
            EventID::new(200),
            current_time,
            event_time,
            &FlowOutcome::Success(TaskResult::empty()),
        );
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::DuplicateEvent { .. }
        ));

        // Out-of-order flow event error
        let result = state.on_flow_outcome(
            EventID::new(100),
            current_time,
            event_time,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
        );
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::OutOfOrderEvent { .. }
        ));

        // State should remain unchanged on ordering errors
        assert_eq!(state.last_applied_event_id, EventID::new(200));
    }

    #[test]
    fn test_flow_scheduling() {
        let base_time = Utc::now();
        let scheduled_time = base_time + Duration::hours(1);

        // Test 1: Enabled flows - scheduling works normally
        let mut enabled_state = FlowProcessState::new(
            EventID::new(1),
            base_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            FlowTriggerStopPolicy::Never,
        );

        // Basic scheduling for enabled flows
        enabled_state
            .on_scheduled(EventID::new(2), base_time, scheduled_time)
            .unwrap();

        assert_eq!(enabled_state.next_planned_at, Some(scheduled_time));
        assert_eq!(enabled_state.last_applied_event_id, EventID::new(2));

        // Test realistic flow: schedule → running → outcome
        // Schedule a flow for the near future
        let scheduled_time = base_time + Duration::minutes(30);
        enabled_state
            .on_scheduled(EventID::new(3), base_time, scheduled_time)
            .unwrap();
        assert_eq!(enabled_state.next_planned_at, Some(scheduled_time));

        // Flow starts running - should clear the past scheduled time
        let execution_start_time = base_time + Duration::hours(1); // After scheduled time
        enabled_state
            .on_running(EventID::new(4), execution_start_time, execution_start_time)
            .unwrap();
        assert_eq!(enabled_state.next_planned_at, None); // Cleared because scheduled time was in the past

        // Flow completes successfully
        enabled_state
            .on_flow_outcome(
                EventID::new(5),
                execution_start_time + Duration::minutes(5),
                execution_start_time + Duration::minutes(5),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();

        // Test that past planned times are cleared on execution events (existing
        // behavior)
        enabled_state.next_planned_at = Some(base_time + Duration::minutes(30));
        enabled_state
            .on_flow_outcome(
                EventID::new(6),
                base_time + Duration::hours(2),
                base_time + Duration::hours(2),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();
        assert_eq!(enabled_state.next_planned_at, None); // Cleared because it was in the past

        // Test that future planned times are preserved
        let future_time = base_time + Duration::hours(3);
        enabled_state.next_planned_at = Some(future_time);
        enabled_state
            .on_flow_outcome(
                EventID::new(7),
                base_time + Duration::hours(1),
                base_time + Duration::hours(1),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();
        assert_eq!(enabled_state.next_planned_at, Some(future_time)); // Preserved because it's in the future

        // Test that non-running states clear planned time
        enabled_state
            .on_scheduled(EventID::new(8), base_time, base_time + Duration::hours(2))
            .unwrap();
        enabled_state
            .update_trigger_state(
                EventID::new(9),
                base_time + Duration::minutes(10),
                true, // pause
                FlowTriggerStopPolicy::Never,
            )
            .unwrap();
        assert_eq!(enabled_state.next_planned_at, None); // Cleared because state is paused

        // Test 2: Paused flows - scheduling should not affect next_planned_at
        let mut paused_state = FlowProcessState::new(
            EventID::new(1),
            base_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Paused,
            FlowTriggerStopPolicy::Never,
        );

        // Scheduling events should be ignored for paused flows
        paused_state
            .on_scheduled(EventID::new(2), base_time, scheduled_time)
            .unwrap();

        assert_eq!(paused_state.next_planned_at, None); // Should remain None
        assert_eq!(paused_state.last_applied_event_id, EventID::new(2)); // Event ID should still update

        // Test 3: Unconfigured flows - scheduling should not affect next_planned_at
        let mut unconfigured_state = FlowProcessState::new(
            EventID::new(1),
            base_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Undefined,
            FlowTriggerStopPolicy::Never,
        );

        // Scheduling events should be ignored for unconfigured flows
        unconfigured_state
            .on_scheduled(EventID::new(2), base_time, scheduled_time)
            .unwrap();

        assert_eq!(unconfigured_state.next_planned_at, None); // Should remain None
        assert_eq!(unconfigured_state.last_applied_event_id, EventID::new(2)); // Event ID should still update
    }

    #[test]
    fn test_on_running_clears_past_scheduled_times() {
        let base_time = Utc::now();
        let mut state = FlowProcessState::new(
            EventID::new(1),
            base_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            FlowTriggerStopPolicy::Never,
        );

        // Test 1: Past scheduled time should be cleared
        let past_time = base_time + Duration::minutes(30);
        state.next_planned_at = Some(past_time);

        let execution_time = base_time + Duration::hours(1); // Later than scheduled time
        state
            .on_running(EventID::new(2), execution_time, execution_time)
            .unwrap();

        assert_eq!(state.next_planned_at, None); // Cleared because past_time <= execution_time
        assert_eq!(state.running_since(), Some(execution_time)); // Track when flow started
        assert_eq!(state.last_applied_event_id, EventID::new(2));

        // Test 2: Future scheduled time should be preserved
        let future_time = base_time + Duration::hours(3);
        state.next_planned_at = Some(future_time);

        let earlier_execution_time = base_time + Duration::hours(2); // Earlier than scheduled time
        state
            .on_running(
                EventID::new(3),
                earlier_execution_time,
                earlier_execution_time,
            )
            .unwrap();

        assert_eq!(state.next_planned_at, Some(future_time)); // Preserved because future_time > execution_time
        assert_eq!(state.running_since(), Some(earlier_execution_time)); // Updated to new start time

        // Test 3: No scheduled time - should remain None
        state.next_planned_at = None;
        let final_exec_time = base_time + Duration::hours(4);
        state
            .on_running(EventID::new(4), final_exec_time, final_exec_time)
            .unwrap();

        assert_eq!(state.next_planned_at, None); // Should remain None
        assert_eq!(state.running_since(), Some(final_exec_time)); // Track flow start

        // Test 4: Event ordering validation
        let result = state.on_running(
            EventID::new(4), // Duplicate event ID
            base_time + Duration::hours(5),
            base_time + Duration::hours(5),
        );
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::DuplicateEvent { .. }
        ));

        // Test 5: Out-of-order event validation
        let result = state.on_running(
            EventID::new(2), // Lower than last applied (4)
            base_time + Duration::hours(5),
            base_time + Duration::hours(5),
        );
        assert!(matches!(
            result.unwrap_err(),
            FlowProcessStateError::OutOfOrderEvent { .. }
        ));

        // State should remain unchanged after validation errors
        assert_eq!(state.last_applied_event_id, EventID::new(4));
    }

    #[test]
    fn test_comprehensive_state_transitions() {
        let mut state = FlowProcessState::new(
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(3),
        );

        let base_time = Utc::now();

        // 1. Success -> Active state
        state
            .on_flow_outcome(
                EventID::new(2),
                base_time + Duration::minutes(1),
                base_time + Duration::minutes(1),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);

        // 2. Manual pause -> PausedManual (overrides everything)
        state
            .update_trigger_state(
                EventID::new(3),
                base_time + Duration::minutes(2),
                true,
                make_test_stop_policy_with_failures(3),
            )
            .unwrap();
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );

        // 3. Manual failure while paused -> stays PausedManual, no consecutive failure
        //    increment
        state
            .on_flow_outcome(
                EventID::new(4),
                base_time + Duration::minutes(3),
                base_time + Duration::minutes(3),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(state.consecutive_failures, 0); // Manual failures don't increment

        // 4. Unpause -> transitions to Active since no automatic failures occurred
        state
            .update_trigger_state(
                EventID::new(5),
                base_time + Duration::minutes(4),
                false,
                make_test_stop_policy_with_failures(3),
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);

        // 5. Add automatic failures -> transitions to Failing
        state
            .on_flow_outcome(
                EventID::new(6),
                base_time + Duration::minutes(5),
                base_time + Duration::minutes(5),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 6. Change policy to Never -> stays Failing
        state
            .update_trigger_state(
                EventID::new(7),
                base_time + Duration::minutes(6),
                false,
                FlowTriggerStopPolicy::Never,
            )
            .unwrap();
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 7. Add more failures -> stays Failing with Never policy
        state
            .on_flow_outcome(
                EventID::new(8),
                base_time + Duration::minutes(7),
                base_time + Duration::minutes(7),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);

        // 8. Success resets everything -> Active
        state
            .on_flow_outcome(
                EventID::new(9),
                base_time + Duration::minutes(8),
                base_time + Duration::minutes(8),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
    }

    #[test]
    fn test_failure_reset_on_resume_from_stopped_state() {
        let mut state = FlowProcessState::new(
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(2),
        );

        let base_time = Utc::now();

        // Generate enough failures to trigger auto-stop
        state
            .on_flow_outcome(
                EventID::new(2),
                base_time,
                base_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();
        state
            .on_flow_outcome(
                EventID::new(3),
                base_time,
                base_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );

        // Resume (unpause) from stopped state -> should reset failures
        state
            .update_trigger_state(
                EventID::new(4),
                base_time + Duration::minutes(1),
                false, // not paused (resume)
                make_test_stop_policy_with_failures(2),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 0); // failures reset
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);

        // Normal pause/unpause should NOT reset failures
        state
            .on_flow_outcome(
                EventID::new(5),
                base_time,
                base_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 1);

        state
            .update_trigger_state(
                EventID::new(6),
                base_time + Duration::minutes(2),
                true, // pause
                make_test_stop_policy_with_failures(2),
            )
            .unwrap();
        state
            .update_trigger_state(
                EventID::new(7),
                base_time + Duration::minutes(3),
                false, // unpause (but wasn't in StoppedAuto)
                make_test_stop_policy_with_failures(2),
            )
            .unwrap();
        assert_eq!(state.consecutive_failures, 1); // failures preserved
    }

    #[test]
    fn test_unrecoverable_failure_auto_stop() {
        let current_time = Utc::now();
        let event_time = current_time;

        let mut state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(5), // High threshold
        );

        // Trigger an unrecoverable failure - should auto-stop immediately
        state
            .on_flow_outcome(
                EventID::new(2),
                current_time + Duration::minutes(1),
                event_time + Duration::minutes(1),
                &FlowOutcome::Failed(TaskError::empty_unrecoverable()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::UnrecoverableFailure)
        );
        assert_eq!(
            state.auto_stopped_at(),
            Some(event_time + Duration::minutes(1))
        );
    }

    #[test]
    fn test_recoverable_vs_unrecoverable_failure() {
        let current_time = Utc::now();
        let event_time = current_time;

        let mut state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(3),
        );

        // First: recoverable failure - should not auto-stop yet
        state
            .on_flow_outcome(
                EventID::new(2),
                current_time + Duration::minutes(1),
                event_time + Duration::minutes(1),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);
        assert_eq!(state.auto_stopped_reason(), None);

        // Second: another recoverable failure - should not auto-stop yet
        state
            .on_flow_outcome(
                EventID::new(3),
                current_time + Duration::minutes(2),
                event_time + Duration::minutes(2),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);
        assert_eq!(state.auto_stopped_reason(), None);

        // Third: unrecoverable failure - should auto-stop immediately regardless of
        // policy
        state
            .on_flow_outcome(
                EventID::new(4),
                current_time + Duration::minutes(3),
                event_time + Duration::minutes(3),
                &FlowOutcome::Failed(TaskError::empty_unrecoverable()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 3);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::UnrecoverableFailure)
        );
    }

    #[test]
    fn test_stop_policy_auto_stop_reason() {
        let current_time = Utc::now();
        let event_time = current_time;

        let mut state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(2),
        );

        // First failure - should not auto-stop
        state
            .on_flow_outcome(
                EventID::new(2),
                current_time + Duration::minutes(1),
                event_time + Duration::minutes(1),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Failing);
        assert_eq!(state.auto_stopped_reason(), None);

        // Second failure - should auto-stop due to policy
        state
            .on_flow_outcome(
                EventID::new(3),
                current_time + Duration::minutes(2),
                event_time + Duration::minutes(2),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::StopPolicy)
        );
        assert_eq!(
            state.auto_stopped_at(),
            Some(event_time + Duration::minutes(2))
        );
    }

    #[test]
    fn test_success_clears_auto_stop_reason() {
        let current_time = Utc::now();
        let event_time = current_time;

        let mut state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(1),
        );

        // Trigger unrecoverable failure - should auto-stop
        state
            .on_flow_outcome(
                EventID::new(2),
                current_time + Duration::minutes(1),
                event_time + Duration::minutes(1),
                &FlowOutcome::Failed(TaskError::empty_unrecoverable()),
            )
            .unwrap();

        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::UnrecoverableFailure)
        );
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );

        // Success should clear auto-stop reason
        state
            .on_flow_outcome(
                EventID::new(3),
                current_time + Duration::minutes(2),
                event_time + Duration::minutes(2),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();

        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.auto_stopped_reason(), None);
        assert_eq!(state.auto_stopped_at(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
    }

    #[test]
    fn test_resume_from_auto_stopped_state() {
        let current_time = Utc::now();

        let mut state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(1),
        );

        // Trigger auto-stop via stop policy
        state
            .on_flow_outcome(
                EventID::new(2),
                current_time + Duration::minutes(1),
                current_time + Duration::minutes(1),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::StopPolicy)
        );

        // Resume by updating trigger state (unpause)
        state
            .update_trigger_state(
                EventID::new(3),
                current_time + Duration::minutes(2),
                false, // not paused
                make_test_stop_policy_with_failures(2),
            )
            .unwrap();

        // Should reset auto-stop state and consecutive failures
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.auto_stopped_reason(), None);
        assert_eq!(state.auto_stopped_at(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
    }

    #[test]
    fn test_domain_events_effective_state_changed() {
        let mut state = FlowProcessState::new(
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            FlowTriggerStopPolicy::default(),
        );

        // Pause the trigger
        let now = Utc::now();
        state
            .update_trigger_state(EventID::new(2), now, true, FlowTriggerStopPolicy::default())
            .unwrap();

        let events = state.take_pending_events();
        assert_eq!(events.len(), 1);
        match &events[0] {
            FlowProcessEvent::EffectiveStateChanged(event) => {
                assert_eq!(event.old_state, FlowProcessEffectiveState::Active);
                assert_eq!(event.new_state, FlowProcessEffectiveState::PausedManual);
            }
            _ => panic!("Expected EffectiveStateChanged event"),
        }
    }

    #[test]
    fn test_domain_events_auto_stopped() {
        let mut state = FlowProcessState::new(
            EventID::new(1),
            Utc::now(),
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(1),
        );

        // Trigger auto-stop with failure
        let now = Utc::now();
        let outcome = FlowOutcome::Failed(TaskError::empty_recoverable());
        state
            .on_flow_outcome(EventID::new(2), now, now, &outcome)
            .unwrap();

        let events = state.take_pending_events();
        assert_eq!(events.len(), 2); // EffectiveStateChanged + AutoStopped

        let auto_stopped_event = events
            .iter()
            .find(|e| matches!(e, FlowProcessEvent::AutoStopped(_)));
        assert!(auto_stopped_event.is_some());

        match auto_stopped_event.unwrap() {
            FlowProcessEvent::AutoStopped(event) => {
                assert_eq!(event.reason, FlowProcessAutoStopReason::StopPolicy);
            }
            _ => panic!("Expected AutoStopped event"),
        }
    }

    #[test]
    fn test_manual_vs_automatic_flow_behavior() {
        let current_time = Utc::now();

        // Test 1: Paused flows (manual executions only)
        let mut paused_state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Paused,
            make_test_stop_policy_with_failures(2),
        );

        assert_eq!(
            paused_state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );

        // Manual failures should not affect consecutive_failures or auto-stop logic
        paused_state
            .on_flow_outcome(
                EventID::new(2),
                current_time,
                current_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(paused_state.consecutive_failures, 0); // No increment for manual flows
        assert_eq!(
            paused_state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(paused_state.auto_stopped_reason(), None);

        // Even unrecoverable failures should not auto-stop manual flows
        paused_state
            .on_flow_outcome(
                EventID::new(3),
                current_time,
                current_time,
                &FlowOutcome::Failed(TaskError::empty_unrecoverable()),
            )
            .unwrap();

        assert_eq!(paused_state.consecutive_failures, 0);
        assert_eq!(
            paused_state.effective_state(),
            FlowProcessEffectiveState::PausedManual
        );
        assert_eq!(paused_state.auto_stopped_reason(), None);

        // Test 2: Unconfigured flows (manual executions only)
        let mut unconfigured_state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Undefined,
            make_test_stop_policy_with_failures(1),
        );

        assert_eq!(
            unconfigured_state.effective_state(),
            FlowProcessEffectiveState::Unconfigured
        );

        // Manual failures should not affect auto-stop logic
        unconfigured_state
            .on_flow_outcome(
                EventID::new(2),
                current_time,
                current_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(unconfigured_state.consecutive_failures, 0);
        assert_eq!(
            unconfigured_state.effective_state(),
            FlowProcessEffectiveState::Unconfigured
        );

        // Test 3: Enabled flows (automatic executions)
        let mut enabled_state = FlowProcessState::new(
            EventID::new(1),
            current_time,
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled,
            make_test_stop_policy_with_failures(2),
        );

        // Automatic failures should increment consecutive_failures and trigger
        // auto-stop
        enabled_state
            .on_flow_outcome(
                EventID::new(2),
                current_time,
                current_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(enabled_state.consecutive_failures, 1);
        assert_eq!(
            enabled_state.effective_state(),
            FlowProcessEffectiveState::Failing
        );

        enabled_state
            .on_flow_outcome(
                EventID::new(3),
                current_time,
                current_time,
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        assert_eq!(enabled_state.consecutive_failures, 2);
        assert_eq!(
            enabled_state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        assert_eq!(
            enabled_state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::StopPolicy)
        );

        // Test 4: Timing fields should be updated regardless of user intent
        assert!(paused_state.last_failure_at().is_some());
        assert!(unconfigured_state.last_failure_at().is_some());
        assert!(enabled_state.last_failure_at().is_some());

        // Test 5: Scheduling behavior - only enabled flows should be scheduled
        let scheduled_time = current_time + Duration::hours(1);

        // Enabled flows should accept scheduling
        enabled_state
            .on_scheduled(EventID::new(10), current_time, scheduled_time)
            .unwrap();
        assert_eq!(enabled_state.next_planned_at, Some(scheduled_time));

        // Paused flows should ignore scheduling
        paused_state
            .on_scheduled(EventID::new(11), current_time, scheduled_time)
            .unwrap();
        assert_eq!(paused_state.next_planned_at, None);

        // Unconfigured flows should ignore scheduling
        unconfigured_state
            .on_scheduled(EventID::new(12), current_time, scheduled_time)
            .unwrap();
        assert_eq!(unconfigured_state.next_planned_at, None);
    }

    #[test]
    fn test_auto_stopped_state_manual_debugging_scenario() {
        let current_time = Utc::now();

        // Create a flow that's already in auto-stopped state (enabled but auto-stopped)
        let mut state = FlowProcessState::rehydrate_from_snapshot(
            make_test_flow_binding(),
            FlowProcessUserIntent::Enabled, // Still enabled, but auto-stopped
            make_test_stop_policy_with_failures(2),
            2,                  // consecutive_failures that caused auto-stop
            None,               // last_success_at
            Some(current_time), // last_failure_at
            Some(current_time), // last_attempt_at
            None,               /* next_planned_at (cleared because not
                                 * running) */
            None,                                        // paused_at
            None,                                        // running_since
            Some(current_time),                          // auto_stopped_at
            FlowProcessEffectiveState::StoppedAuto,      // effective_state
            Some(FlowProcessAutoStopReason::StopPolicy), // auto_stopped_reason
            current_time,
            EventID::new(100),
        )
        .unwrap();

        // Verify initial auto-stopped state
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::StopPolicy)
        );
        assert_eq!(state.consecutive_failures, 2);
        let original_auto_stopped_at = state.auto_stopped_at().unwrap();

        // User runs first manual debugging attempt on auto-stopped enabled flow - it
        // fails
        state
            .on_flow_outcome(
                EventID::new(101),
                current_time + Duration::minutes(5),
                current_time + Duration::minutes(5),
                &FlowOutcome::Failed(TaskError::empty_recoverable()),
            )
            .unwrap();

        // Manual failure on auto-stopped flow should NOT modify auto-stop state
        assert_eq!(state.consecutive_failures, 2); // Unchanged
        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::StopPolicy)
        ); // Unchanged
        assert_eq!(state.auto_stopped_at(), Some(original_auto_stopped_at)); // Unchanged
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        // But timing should still be updated for throttling purposes
        assert_eq!(
            state.last_failure_at(),
            Some(current_time + Duration::minutes(5))
        );
        assert_eq!(
            state.last_attempt_at(),
            Some(current_time + Duration::minutes(5))
        );

        // User runs second manual debugging attempt - also fails
        // (unrecoverable this time)
        state
            .on_flow_outcome(
                EventID::new(102),
                current_time + Duration::minutes(10),
                current_time + Duration::minutes(10),
                &FlowOutcome::Failed(TaskError::empty_unrecoverable()),
            )
            .unwrap();

        // Still no change to auto-stop state - manual failures don't modify existing
        // auto-stop, even if they are unrecoverable
        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(
            state.auto_stopped_reason(),
            Some(FlowProcessAutoStopReason::StopPolicy)
        );
        assert_eq!(state.auto_stopped_at(), Some(original_auto_stopped_at));
        assert_eq!(
            state.effective_state(),
            FlowProcessEffectiveState::StoppedAuto
        );
        assert_eq!(
            state.last_failure_at(),
            Some(current_time + Duration::minutes(10))
        );

        // User finally succeeds on third manual attempt - fixes the issue!
        state
            .on_flow_outcome(
                EventID::new(103),
                current_time + Duration::minutes(15),
                current_time + Duration::minutes(15),
                &FlowOutcome::Success(TaskResult::empty()),
            )
            .unwrap();

        // Manual success should clear auto-stop state and reset consecutive failures
        // (self-healing)
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.auto_stopped_reason(), None);
        assert_eq!(state.auto_stopped_at(), None);
        assert_eq!(state.effective_state(), FlowProcessEffectiveState::Active);
        assert_eq!(
            state.last_success_at(),
            Some(current_time + Duration::minutes(15))
        );

        // Flow is now healthy and ready for automatic execution again
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
