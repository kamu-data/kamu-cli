// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{Duration, DurationRound, Utc};
use dill::Catalog;
use kamu_adapter_flow_dataset::ingest_dataset_binding;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_empty_state_table_initially(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    let single_state = flow_process_state_query
        .try_get_process_state(&ingest_dataset_binding(
            &odf::DatasetID::new_seeded_ed25519(b"random-dataset-id"),
        ))
        .await
        .unwrap();
    assert!(single_state.is_none());

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert!(listing.processes.is_empty());

    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup.total, 0);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.worst_consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_index_single_process_in_initial_state(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);
    let sort_key = "kamu/random-dataset-id".to_string();

    flow_process_repository
        .insert_process_state(
            EventID::new(1),
            flow_binding.clone(),
            sort_key.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    let single_state = flow_process_state_query
        .try_get_process_state(&ingest_dataset_binding(&dataset_id))
        .await
        .unwrap();
    assert!(single_state.is_some());

    let single_state = single_state.unwrap();
    assert_eq!(*single_state.flow_binding(), flow_binding);
    assert_eq!(*single_state.sort_key(), sort_key);
    assert!(!single_state.paused_manual());
    assert_eq!(
        single_state.effective_state(),
        FlowProcessEffectiveState::Active,
    );
    assert_eq!(single_state.consecutive_failures(), 0);
    assert!(single_state.last_success_at().is_none());
    assert!(single_state.last_failure_at().is_none());
    assert!(single_state.last_attempt_at().is_none());
    assert!(single_state.next_planned_at().is_none());
    assert_eq!(
        single_state.last_applied_trigger_event_id(),
        EventID::new(1)
    );
    assert_eq!(single_state.last_applied_flow_event_id(), EventID::new(0));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(listing.processes.len(), 1);
    assert_eq!(listing.processes[0], single_state);

    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 1);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.worst_consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_index_single_process_after_immediate_stop(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);
    let sort_key = "kamu/random-dataset-id".to_string();

    flow_process_repository
        .insert_process_state(
            EventID::new(1),
            flow_binding.clone(),
            sort_key.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(EventID::new(1), &flow_binding, true, success_time)
        .await
        .unwrap();

    // Schedule flow after success
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(2),
            &flow_binding,
            success_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let failure_time = success_time + Duration::hours(1);
    flow_process_repository
        .apply_flow_result(EventID::new(3), &flow_binding, false, failure_time)
        .await
        .unwrap();

    let single_state = flow_process_state_query
        .try_get_process_state(&ingest_dataset_binding(&dataset_id))
        .await
        .unwrap();
    assert!(single_state.is_some());

    let single_state = single_state.unwrap();
    assert_eq!(*single_state.flow_binding(), flow_binding);
    assert_eq!(*single_state.sort_key(), sort_key);
    assert!(!single_state.paused_manual());
    assert_eq!(
        single_state.effective_state(),
        FlowProcessEffectiveState::StoppedAuto,
    );
    assert_eq!(single_state.consecutive_failures(), 1);
    assert_eq!(single_state.last_success_at(), Some(success_time));
    assert_eq!(single_state.last_failure_at(), Some(failure_time));
    assert_eq!(single_state.last_attempt_at(), Some(failure_time));
    assert!(single_state.next_planned_at().is_none());
    assert_eq!(
        single_state.last_applied_trigger_event_id(),
        EventID::new(1)
    );
    assert_eq!(single_state.last_applied_flow_event_id(), EventID::new(3));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(listing.processes.len(), 1);
    assert_eq!(listing.processes[0], single_state);

    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 1);
    assert_eq!(rollup.worst_consecutive_failures, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_index_single_process_in_failing_state(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);
    let sort_key = "kamu/random-dataset-id".to_string();

    flow_process_repository
        .insert_process_state(
            EventID::new(1),
            flow_binding.clone(),
            sort_key.clone(),
            false,
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(EventID::new(2), &flow_binding, true, success_time)
        .await
        .unwrap();

    // Schedule after success
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(3),
            &flow_binding,
            success_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let failure_time = success_time + Duration::hours(1);
    flow_process_repository
        .apply_flow_result(EventID::new(4), &flow_binding, false, failure_time)
        .await
        .unwrap();

    // Schedule after failure
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(5),
            &flow_binding,
            failure_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let second_failure_time = failure_time + Duration::hours(1);
    let next_run_time = Some(second_failure_time + Duration::hours(1));
    flow_process_repository
        .apply_flow_result(EventID::new(6), &flow_binding, false, second_failure_time)
        .await
        .unwrap();

    // Schedule after second failure if needed
    if let Some(planned_at) = next_run_time {
        flow_process_repository
            .on_flow_scheduled(EventID::new(7), &flow_binding, planned_at)
            .await
            .unwrap();
    }

    let single_state = flow_process_state_query
        .try_get_process_state(&ingest_dataset_binding(&dataset_id))
        .await
        .unwrap();
    assert!(single_state.is_some());

    let single_state = single_state.unwrap();
    assert_eq!(*single_state.flow_binding(), flow_binding);
    assert_eq!(*single_state.sort_key(), sort_key);
    assert!(!single_state.paused_manual());
    assert_eq!(
        single_state.effective_state(),
        FlowProcessEffectiveState::Failing,
    );
    assert_eq!(single_state.consecutive_failures(), 2);
    assert_eq!(single_state.last_success_at(), Some(success_time));
    assert_eq!(single_state.last_failure_at(), Some(second_failure_time));
    assert_eq!(single_state.last_attempt_at(), Some(second_failure_time));
    assert_eq!(single_state.next_planned_at(), next_run_time);
    assert_eq!(
        single_state.last_applied_trigger_event_id(),
        EventID::new(1)
    );
    assert_eq!(single_state.last_applied_flow_event_id(), EventID::new(7));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(listing.processes.len(), 1);
    assert_eq!(listing.processes[0], single_state);

    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 1);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.worst_consecutive_failures, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_index_single_process_after_recovery(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);
    let sort_key = "kamu/random-dataset-id".to_string();

    flow_process_repository
        .insert_process_state(
            EventID::new(1),
            flow_binding.clone(),
            sort_key.clone(),
            false,
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(EventID::new(2), &flow_binding, true, success_time)
        .await
        .unwrap();

    // Schedule after success
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(3),
            &flow_binding,
            success_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let failure_time = success_time + Duration::hours(1);
    flow_process_repository
        .apply_flow_result(EventID::new(4), &flow_binding, false, failure_time)
        .await
        .unwrap();

    // Schedule after failure
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(5),
            &flow_binding,
            failure_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let second_failure_time = failure_time + Duration::hours(1);
    flow_process_repository
        .apply_flow_result(EventID::new(6), &flow_binding, false, second_failure_time)
        .await
        .unwrap();

    // Schedule after second failure
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(7),
            &flow_binding,
            second_failure_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let recovery_time = second_failure_time + Duration::hours(1);
    flow_process_repository
        .apply_flow_result(EventID::new(8), &flow_binding, true, recovery_time)
        .await
        .unwrap();

    // Schedule after recovery
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(9),
            &flow_binding,
            recovery_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let single_state = flow_process_state_query
        .try_get_process_state(&ingest_dataset_binding(&dataset_id))
        .await
        .unwrap();
    assert!(single_state.is_some());

    let single_state = single_state.unwrap();
    assert_eq!(*single_state.flow_binding(), flow_binding);
    assert_eq!(*single_state.sort_key(), sort_key);
    assert!(!single_state.paused_manual());
    assert_eq!(
        single_state.effective_state(),
        FlowProcessEffectiveState::Active,
    );
    assert_eq!(single_state.consecutive_failures(), 0);
    assert_eq!(single_state.last_success_at(), Some(recovery_time));
    assert_eq!(single_state.last_failure_at(), Some(second_failure_time));
    assert_eq!(single_state.last_attempt_at(), Some(recovery_time));
    assert_eq!(
        single_state.next_planned_at(),
        Some(recovery_time + Duration::hours(1))
    );
    assert_eq!(
        single_state.last_applied_trigger_event_id(),
        EventID::new(1)
    );
    assert_eq!(single_state.last_applied_flow_event_id(), EventID::new(9));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(listing.processes.len(), 1);
    assert_eq!(listing.processes[0], single_state);

    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 1);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.worst_consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_index_single_process_after_pause(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);
    let sort_key = "kamu/random-dataset-id".to_string();

    flow_process_repository
        .insert_process_state(
            EventID::new(1),
            flow_binding.clone(),
            sort_key.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(EventID::new(2), &flow_binding, true, success_time)
        .await
        .unwrap();

    // Schedule after success
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(3),
            &flow_binding,
            success_time + Duration::hours(1),
        )
        .await
        .unwrap();

    flow_process_repository
        .update_trigger_state(
            &flow_binding,
            EventID::new(4),
            true,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    let single_state = flow_process_state_query
        .try_get_process_state(&ingest_dataset_binding(&dataset_id))
        .await
        .unwrap();
    assert!(single_state.is_some());

    let single_state = single_state.unwrap();
    assert_eq!(*single_state.flow_binding(), flow_binding);
    assert_eq!(*single_state.sort_key(), sort_key);
    assert!(single_state.paused_manual());
    assert_eq!(
        single_state.effective_state(),
        FlowProcessEffectiveState::PausedManual,
    );
    assert_eq!(single_state.consecutive_failures(), 0);
    assert_eq!(single_state.last_success_at(), Some(success_time));
    assert_eq!(single_state.last_failure_at(), None);
    assert_eq!(single_state.last_attempt_at(), Some(success_time));
    assert_eq!(single_state.next_planned_at(), None); // Cleared when paused
    assert_eq!(
        single_state.last_applied_trigger_event_id(),
        EventID::new(4) // Updated to match new event ID sequence
    );
    assert_eq!(single_state.last_applied_flow_event_id(), EventID::new(3));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(listing.processes.len(), 1);
    assert_eq!(listing.processes[0], single_state);

    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 1);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.worst_consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_process(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);
    let sort_key = "kamu/random-dataset-id".to_string();

    // Insert a process first
    flow_process_repository
        .insert_process_state(
            EventID::new(1),
            flow_binding.clone(),
            sort_key.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    // Verify it exists
    let single_state = flow_process_state_query
        .try_get_process_state(&flow_binding)
        .await
        .unwrap();
    assert!(single_state.is_some());

    let listing_before = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert_eq!(listing_before.processes.len(), 1);

    let rollup_before = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup_before.total, 1);

    // Delete the process
    flow_process_repository
        .delete_process_state(&flow_binding)
        .await
        .unwrap();

    // Verify it's gone
    let single_state_after = flow_process_state_query
        .try_get_process_state(&flow_binding)
        .await
        .unwrap();
    assert!(single_state_after.is_none());

    let listing_after = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            100,
            0,
        )
        .await
        .unwrap();
    assert!(listing_after.processes.is_empty());

    let rollup_after = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup_after.total, 0);
    assert_eq!(rollup_after.active, 0);
    assert_eq!(rollup_after.failing, 0);
    assert_eq!(rollup_after.paused, 0);
    assert_eq!(rollup_after.stopped, 0);
    assert_eq!(rollup_after.worst_consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_process_not_found(catalog: &Catalog) {
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"nonexistent-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);

    // Try to delete a process that doesn't exist
    let result = flow_process_repository
        .delete_process_state(&flow_binding)
        .await;

    // Should return NotFound error
    assert!(result.is_err());
    match result.unwrap_err() {
        FlowProcessDeleteError::NotFound(err) => {
            assert_eq!(err.flow_binding, flow_binding);
        }
        other => panic!("Expected NotFound error, got: {other:?}",),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_process_with_history(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);
    let sort_key = "kamu/random-dataset-id".to_string();

    // Insert a process
    flow_process_repository
        .insert_process_state(
            EventID::new(1),
            flow_binding.clone(),
            sort_key.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    // Apply some flow results to create history
    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(EventID::new(2), &flow_binding, true, success_time)
        .await
        .unwrap();

    // Schedule after success
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(3),
            &flow_binding,
            success_time + Duration::hours(1),
        )
        .await
        .unwrap();

    let failure_time = success_time + Duration::hours(1);
    flow_process_repository
        .apply_flow_result(EventID::new(4), &flow_binding, false, failure_time)
        .await
        .unwrap();

    // Schedule after failure
    flow_process_repository
        .on_flow_scheduled(
            EventID::new(5),
            &flow_binding,
            failure_time + Duration::hours(1),
        )
        .await
        .unwrap();

    // Update trigger state
    flow_process_repository
        .update_trigger_state(
            &flow_binding,
            EventID::new(3),
            true,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    // Verify the process exists and has the expected state
    let single_state = flow_process_state_query
        .try_get_process_state(&flow_binding)
        .await
        .unwrap();
    assert!(single_state.is_some());
    let state = single_state.unwrap();
    assert_eq!(state.consecutive_failures(), 1);
    assert_eq!(state.last_success_at(), Some(success_time));
    assert_eq!(state.last_failure_at(), Some(failure_time));
    assert!(state.paused_manual());

    // Delete the process
    flow_process_repository
        .delete_process_state(&flow_binding)
        .await
        .unwrap();

    // Verify it's completely gone
    let single_state_after = flow_process_state_query
        .try_get_process_state(&flow_binding)
        .await
        .unwrap();
    assert!(single_state_after.is_none());

    let rollup_after = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup_after.total, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
