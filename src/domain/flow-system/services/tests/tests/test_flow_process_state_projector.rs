// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{Duration, Utc};
use dill::*;
use kamu_adapter_flow_dataset::ingest_dataset_binding;
use kamu_adapter_task_dataset::TaskResultDatasetUpdate;
use kamu_core::PullResult;
use kamu_flow_system::*;
use kamu_flow_system_inmem::InMemoryFlowProcessState;
use kamu_flow_system_services::*;
use kamu_task_system::{TaskError, TaskResult};
use messaging_outbox::{MockOutbox, Outbox};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_trigger_events() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_binding = ingest_dataset_binding(&foo_id);

    let foo_schedule = FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
        every: Duration::hours(1),
    }));

    let mock_flow_trigger_service = {
        let mut m = MockFlowTriggerService::new();
        m.expect_find_trigger().times(3).returning(|_| Ok(None));
        m
    };

    let mock_outbox = {
        let mut m = MockOutbox::new();
        let mut s = mockall::Sequence::new();
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::EffectiveStateChanged(e)
                            if e.old_effective_state == FlowProcessEffectiveState::Active &&
                                e.new_effective_state == FlowProcessEffectiveState::PausedManual
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::EffectiveStateChanged(e)
                            if e.old_effective_state == FlowProcessEffectiveState::PausedManual &&
                                e.new_effective_state == FlowProcessEffectiveState::Active
                    )
            })
            .returning(|_, _, _| Ok(()));
        m
    };

    let harness = FlowProcessStateProjectorHarness::new(
        mock_flow_trigger_service,
        MockFlowSchedulingService::new(),
        mock_outbox,
    );

    // No state initially
    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert!(state.is_none());

    // Apply trigger created event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(1),
            tx_id: 1,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Created(FlowTriggerEventCreated {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: false,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::Never,
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0
    );

    // Pause the trigger
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(2),
            tx_id: 2,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Modified(FlowTriggerEventModified {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: true,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::Never,
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Paused &&
        s.effective_state() == FlowProcessEffectiveState::PausedManual &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0
    );

    // Resume the trigger
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(3),
            tx_id: 3,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Modified(FlowTriggerEventModified {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: false,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::Never,
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0
    );

    // Remove trigger scope
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(4),
            tx_id: 4,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::ScopeRemoved(
                FlowTriggerEventScopeRemoved {
                    event_time: Utc::now(),
                    flow_binding: foo_binding.clone(),
                },
            ))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert!(state.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_scheduled() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_binding = ingest_dataset_binding(&foo_id);

    let foo_schedule = FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
        every: Duration::hours(1),
    }));

    let mock_flow_trigger_service = {
        let mut m = MockFlowTriggerService::new();
        m.expect_find_trigger().times(2).returning(|_| Ok(None));
        m
    };

    let mock_outbox = MockOutbox::new();

    let harness = FlowProcessStateProjectorHarness::new(
        mock_flow_trigger_service,
        MockFlowSchedulingService::new(),
        mock_outbox,
    );

    // Apply trigger created event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(1),
            tx_id: 1,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Created(FlowTriggerEventCreated {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: false,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::Never,
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_none() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Apply flow scheduled event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(2),
            tx_id: 2,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::ScheduledForActivation(
                FlowEventScheduledForActivation {
                    event_time: Utc::now(),
                    flow_binding: foo_binding.clone(),
                    flow_id: FlowID::new(1),
                    scheduled_for_activation_at: Utc::now() + Duration::minutes(10),
                },
            ))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_none() &&
        s.last_success_at().is_none() &&
        s.last_failure_at().is_none() &&
        s.next_planned_at().is_some()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_succeeded() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_binding = ingest_dataset_binding(&foo_id);

    let foo_schedule = FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
        every: Duration::hours(1),
    }));

    let mock_flow_trigger_service = {
        let mut m = MockFlowTriggerService::new();
        let mut s = mockall::Sequence::new();
        m.expect_find_trigger()
            .times(1)
            .in_sequence(&mut s)
            .returning(|_| Ok(None));
        m.expect_find_trigger()
            .times(1)
            .in_sequence(&mut s)
            .returning(|_| {
                Ok(Some(FlowTriggerState {
                    flow_binding: ingest_dataset_binding(&odf::DatasetID::new_seeded_ed25519(
                        b"foo",
                    )),
                    status: FlowTriggerStatus::Active,
                    rule: FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
                        every: Duration::hours(1),
                    })),
                    stop_policy: FlowTriggerStopPolicy::Never,
                }))
            });
        m
    };

    let mock_flow_scheduling_service = {
        let mut m = MockFlowSchedulingService::new();
        m.expect_try_schedule_auto_polling_flow_continuation_if_enabled()
            .times(1)
            .returning(|_, _, _| Ok(()));
        m
    };

    let mock_outbox = MockOutbox::new();

    let harness = FlowProcessStateProjectorHarness::new(
        mock_flow_trigger_service,
        mock_flow_scheduling_service,
        mock_outbox,
    );

    // Apply trigger created event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(1),
            tx_id: 1,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Created(FlowTriggerEventCreated {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: false,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::Never,
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_none() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Apply flow succeeded event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(2),
            tx_id: 2,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::Completed(FlowEventCompleted {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                flow_id: FlowID::new(1),
                outcome: FlowOutcome::Success(
                    TaskResultDatasetUpdate {
                        pull_result: PullResult::Updated {
                            old_head: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                            new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                        },
                    }
                    .into_task_result(),
                ),
                late_activation_causes: vec![],
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_some() &&
        s.last_attempt_at() == s.last_success_at() &&
        s.last_failure_at().is_none() &&
        s.next_planned_at().is_none()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_failing_until_threshold_reached() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_binding = ingest_dataset_binding(&foo_id);

    let foo_schedule = FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
        every: Duration::hours(1),
    }));

    let mock_flow_trigger_service = {
        let mut m = MockFlowTriggerService::new();
        m.expect_find_trigger().times(4).returning(|_| Ok(None));
        m.expect_apply_trigger_auto_stop_decision()
            .times(1)
            .returning(|_, _| Ok(None));
        m
    };

    let mock_outbox = {
        let mut m = MockOutbox::new();
        let mut s = mockall::Sequence::new();
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::FailureRegistered(e)
                            if e.new_consecutive_failures == 1
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::EffectiveStateChanged(e)
                            if e.old_effective_state == FlowProcessEffectiveState::Active &&
                                e.new_effective_state == FlowProcessEffectiveState::Failing
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::FailureRegistered(e)
                            if e.new_consecutive_failures == 2
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::FailureRegistered(e)
                            if e.new_consecutive_failures == 3
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::EffectiveStateChanged(e)
                            if e.old_effective_state == FlowProcessEffectiveState::Failing &&
                                e.new_effective_state == FlowProcessEffectiveState::StoppedAuto
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::TriggerAutoStopped(e)
                            if e.reason == FlowProcessAutoStopReason::StopPolicy
                    )
            })
            .returning(|_, _, _| Ok(()));
        m
    };

    let harness = FlowProcessStateProjectorHarness::new(
        mock_flow_trigger_service,
        MockFlowSchedulingService::new(),
        mock_outbox,
    );

    // Apply trigger created event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(1),
            tx_id: 1,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Created(FlowTriggerEventCreated {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: false,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::AfterConsecutiveFailures {
                    failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
                },
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_none() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Apply flow failed event (recoverable)
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(2),
            tx_id: 2,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::Completed(FlowEventCompleted {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                flow_id: FlowID::new(1),
                outcome: FlowOutcome::Failed(TaskError::empty_recoverable()),
                late_activation_causes: vec![],
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Failing &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 1 &&
        s.last_attempt_at().is_some() &&
        s.last_attempt_at() == s.last_failure_at() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Apply another recoverable failure
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(3),
            tx_id: 3,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::Completed(FlowEventCompleted {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                flow_id: FlowID::new(2),
                outcome: FlowOutcome::Failed(TaskError::empty_recoverable()),
                late_activation_causes: vec![],
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Failing &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 2 &&
        s.last_attempt_at().is_some() &&
        s.last_attempt_at() == s.last_failure_at() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Third recoverable failure, but should stop now
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(4),
            tx_id: 4,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::Completed(FlowEventCompleted {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                flow_id: FlowID::new(3),
                outcome: FlowOutcome::Failed(TaskError::empty_recoverable()),
                late_activation_causes: vec![],
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::StoppedAuto &&
        s.auto_stopped_reason() == Some(FlowProcessAutoStopReason::StopPolicy) &&
        s.consecutive_failures() == 3 &&
        s.last_attempt_at().is_some() &&
        s.last_attempt_at() == s.last_failure_at() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_failing_unrecoverable() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_binding = ingest_dataset_binding(&foo_id);

    let foo_schedule = FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
        every: Duration::hours(1),
    }));

    let mock_flow_trigger_service = {
        let mut m = MockFlowTriggerService::new();
        m.expect_find_trigger().times(2).returning(|_| Ok(None));
        m.expect_apply_trigger_auto_stop_decision()
            .times(1)
            .returning(|_, _| Ok(None));
        m
    };

    let mock_outbox = {
        let mut m = MockOutbox::new();
        let mut s = mockall::Sequence::new();
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::FailureRegistered(e)
                            if e.new_consecutive_failures == 1
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::EffectiveStateChanged(e)
                            if e.old_effective_state == FlowProcessEffectiveState::Active &&
                                e.new_effective_state == FlowProcessEffectiveState::StoppedAuto
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::TriggerAutoStopped(e)
                            if e.reason == FlowProcessAutoStopReason::UnrecoverableFailure
                    )
            })
            .returning(|_, _, _| Ok(()));
        m
    };

    let harness = FlowProcessStateProjectorHarness::new(
        mock_flow_trigger_service,
        MockFlowSchedulingService::new(),
        mock_outbox,
    );

    // Apply trigger created event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(1),
            tx_id: 1,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Created(FlowTriggerEventCreated {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: false,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::AfterConsecutiveFailures {
                    failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
                },
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_none() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Apply flow failed event (unrecoverable)
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(2),
            tx_id: 2,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::Completed(FlowEventCompleted {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                flow_id: FlowID::new(1),
                outcome: FlowOutcome::Failed(TaskError::empty_unrecoverable()),
                late_activation_causes: vec![],
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::StoppedAuto &&
        s.auto_stopped_reason() == Some(FlowProcessAutoStopReason::UnrecoverableFailure) &&
        s.consecutive_failures() == 1 &&
        s.last_attempt_at().is_some() &&
        s.last_attempt_at() == s.last_failure_at() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_self_healed() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_binding = ingest_dataset_binding(&foo_id);

    let foo_schedule = FlowTriggerRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
        every: Duration::hours(1),
    }));

    let mock_flow_trigger_service = {
        let mut m = MockFlowTriggerService::new();
        m.expect_find_trigger().times(3).returning(|_| Ok(None));
        m
    };

    let mock_outbox = {
        let mut m = MockOutbox::new();
        let mut s = mockall::Sequence::new();
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::FailureRegistered(e)
                            if e.new_consecutive_failures == 1
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::EffectiveStateChanged(e)
                            if e.old_effective_state == FlowProcessEffectiveState::Active &&
                                e.new_effective_state == FlowProcessEffectiveState::Failing
                    )
            })
            .returning(|_, _, _| Ok(()));
        m.expect_post_message_as_json()
            .times(1)
            .in_sequence(&mut s)
            .withf(|producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_FLOW_PROCESS_STATE_PROJECTOR
                    && matches!(
                        serde_json::from_value(message.clone()).unwrap(),
                        FlowProcessLifecycleMessage::EffectiveStateChanged(e)
                            if e.old_effective_state == FlowProcessEffectiveState::Failing &&
                                e.new_effective_state == FlowProcessEffectiveState::Active
                    )
            })
            .returning(|_, _, _| Ok(()));
        m
    };

    let harness = FlowProcessStateProjectorHarness::new(
        mock_flow_trigger_service,
        MockFlowSchedulingService::new(),
        mock_outbox,
    );

    // Apply trigger created event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(1),
            tx_id: 1,
            source_type: FlowSystemEventSourceType::FlowTrigger,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowTriggerEvent::Created(FlowTriggerEventCreated {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                paused: false,
                rule: foo_schedule.clone(),
                stop_policy: FlowTriggerStopPolicy::AfterConsecutiveFailures {
                    failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
                },
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_none() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Apply flow failed event (recoverable)
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(2),
            tx_id: 2,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::Completed(FlowEventCompleted {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                flow_id: FlowID::new(1),
                outcome: FlowOutcome::Failed(TaskError::empty_recoverable()),
                late_activation_causes: vec![],
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Failing &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 1 &&
        s.last_attempt_at().is_some() &&
        s.last_attempt_at() == s.last_failure_at() &&
        s.last_success_at().is_none() &&
        s.next_planned_at().is_none()
    );

    // Apply flow success event
    harness
        .projector
        .apply(&FlowSystemEvent {
            event_id: EventID::new(3),
            tx_id: 3,
            source_type: FlowSystemEventSourceType::Flow,
            occurred_at: Utc::now(),
            payload: serde_json::to_value(FlowEvent::Completed(FlowEventCompleted {
                event_time: Utc::now(),
                flow_binding: foo_binding.clone(),
                flow_id: FlowID::new(2),
                outcome: FlowOutcome::Success(TaskResult::empty()),
                late_activation_causes: vec![],
            }))
            .unwrap(),
        })
        .await
        .unwrap();

    let state = harness
        .flow_process_state_query
        .try_get_process_state(&foo_binding)
        .await
        .unwrap();
    assert_matches!(state, Some(s) if
        *s.flow_binding() == foo_binding &&
        s.user_intent() == FlowProcessUserIntent::Enabled &&
        s.effective_state() == FlowProcessEffectiveState::Active &&
        s.auto_stopped_reason().is_none() &&
        s.consecutive_failures() == 0 &&
        s.last_attempt_at().is_some() &&
        s.last_attempt_at() == s.last_success_at() &&
        s.last_failure_at().is_some() && // still stays
        s.next_planned_at().is_none()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowProcessStateProjectorHarness {
    projector: Arc<FlowProcessStateProjector>,
    flow_process_state_query: Arc<dyn FlowProcessStateQuery>,
}

impl FlowProcessStateProjectorHarness {
    fn new(
        mock_flow_trigger_service: MockFlowTriggerService,
        mock_flow_scheduling_service: MockFlowSchedulingService,
        mock_outbox: MockOutbox,
    ) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_value(mock_outbox)
                .bind::<dyn Outbox, MockOutbox>()
                .add::<FlowProcessStateProjector>()
                .add::<InMemoryFlowProcessState>()
                .add_value(mock_flow_trigger_service)
                .bind::<dyn FlowTriggerService, MockFlowTriggerService>()
                .add_value(mock_flow_scheduling_service)
                .bind::<dyn FlowSchedulingService, MockFlowSchedulingService>()
                .add::<SystemTimeSourceDefault>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        Self {
            projector: catalog.get_one().unwrap(),
            flow_process_state_query: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
