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
use kamu_task_system::{TaskError, TaskResult};

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
            None,
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

    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            flow_binding.clone(),
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
    assert_eq!(single_state.last_applied_event_id(), EventID::new(1));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            None,
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

    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            flow_binding.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(
            EventID::new(2),
            &flow_binding,
            &FlowOutcome::Success(TaskResult::empty()),
            success_time,
        )
        .await
        .unwrap();

    // Schedule flow after success
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
        .apply_flow_result(
            EventID::new(4),
            &flow_binding,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
            failure_time,
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
    assert_eq!(single_state.last_applied_event_id(), EventID::new(4));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            None,
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

    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            flow_binding.clone(),
            false,
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(
            EventID::new(2),
            &flow_binding,
            &FlowOutcome::Success(TaskResult::empty()),
            success_time,
        )
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
        .apply_flow_result(
            EventID::new(4),
            &flow_binding,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
            failure_time,
        )
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
        .apply_flow_result(
            EventID::new(6),
            &flow_binding,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
            second_failure_time,
        )
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
    assert_eq!(single_state.last_applied_event_id(), EventID::new(7));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            None,
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

    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            flow_binding.clone(),
            false,
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(
            EventID::new(2),
            &flow_binding,
            &FlowOutcome::Success(TaskResult::empty()),
            success_time,
        )
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
        .apply_flow_result(
            EventID::new(4),
            &flow_binding,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
            failure_time,
        )
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
        .apply_flow_result(
            EventID::new(6),
            &flow_binding,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
            second_failure_time,
        )
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
        .apply_flow_result(
            EventID::new(8),
            &flow_binding,
            &FlowOutcome::Success(TaskResult::empty()),
            recovery_time,
        )
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
    assert_eq!(single_state.last_applied_event_id(), EventID::new(9));

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            None,
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

    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            flow_binding.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(
            EventID::new(2),
            &flow_binding,
            &FlowOutcome::Success(TaskResult::empty()),
            success_time,
        )
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
        .upsert_process_state_on_trigger_event(
            EventID::new(4),
            flow_binding.clone(),
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
    assert_eq!(single_state.last_applied_event_id(), EventID::new(4)); // Updated to match new event ID sequence

    let listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            None,
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

    // Insert a process first
    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            flow_binding.clone(),
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
            None,
        )
        .await
        .unwrap();
    assert_eq!(listing_before.processes.len(), 1);

    let rollup_before = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup_before.total, 1);

    // Delete the parent scope
    flow_process_repository
        .delete_process_states_by_scope(&flow_binding.scope)
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
            None,
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

pub async fn test_delete_multiple_process_types_by_scope(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");

    // Create multiple flow types for the same dataset scope
    use kamu_adapter_flow_dataset::{
        compaction_dataset_binding,
        ingest_dataset_binding,
        transform_dataset_binding,
    };

    let ingest_binding = ingest_dataset_binding(&dataset_id);
    let transform_binding = transform_dataset_binding(&dataset_id);
    let compaction_binding = compaction_dataset_binding(&dataset_id);

    // Verify all bindings have the same scope but different flow types
    assert_eq!(ingest_binding.scope, transform_binding.scope);
    assert_eq!(ingest_binding.scope, compaction_binding.scope);
    assert_ne!(ingest_binding.flow_type, transform_binding.flow_type);
    assert_ne!(ingest_binding.flow_type, compaction_binding.flow_type);
    assert_ne!(transform_binding.flow_type, compaction_binding.flow_type);

    // Insert process states for all three flow types
    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            ingest_binding.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(2),
            transform_binding.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(3),
            compaction_binding.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    // Add some history to the processes
    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    // Ingest process: successful
    flow_process_repository
        .apply_flow_result(
            EventID::new(4),
            &ingest_binding,
            &FlowOutcome::Success(TaskResult::empty()),
            success_time,
        )
        .await
        .unwrap();

    // Transform process: failed
    let failure_time = success_time + Duration::hours(1);
    flow_process_repository
        .apply_flow_result(
            EventID::new(5),
            &transform_binding,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
            failure_time,
        )
        .await
        .unwrap();

    // Compaction process: scheduled
    let scheduled_time = failure_time + Duration::hours(1);
    flow_process_repository
        .on_flow_scheduled(EventID::new(6), &compaction_binding, scheduled_time)
        .await
        .unwrap();

    // Verify all three process states exist
    let ingest_state = flow_process_state_query
        .try_get_process_state(&ingest_binding)
        .await
        .unwrap();
    assert!(ingest_state.is_some());

    let transform_state = flow_process_state_query
        .try_get_process_state(&transform_binding)
        .await
        .unwrap();
    assert!(transform_state.is_some());

    let compaction_state = flow_process_state_query
        .try_get_process_state(&compaction_binding)
        .await
        .unwrap();
    assert!(compaction_state.is_some());

    // Verify listing shows all three processes
    let listing_before = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(listing_before.processes.len(), 3);

    // Verify rollup counts all three processes
    let rollup_before = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();
    assert_eq!(rollup_before.total, 3);
    assert_eq!(rollup_before.active, 2); // ingest success + compaction scheduled
    assert_eq!(rollup_before.failing, 0); // transform failed but is stopped, not failing
    assert_eq!(rollup_before.paused, 0);
    assert_eq!(rollup_before.stopped, 1); // transform stopped after single failure

    // Delete by scope - this should remove ALL flow types for this dataset
    flow_process_repository
        .delete_process_states_by_scope(&ingest_binding.scope)
        .await
        .unwrap();

    // Verify all process states are gone
    let ingest_state_after = flow_process_state_query
        .try_get_process_state(&ingest_binding)
        .await
        .unwrap();
    assert!(ingest_state_after.is_none());

    let transform_state_after = flow_process_state_query
        .try_get_process_state(&transform_binding)
        .await
        .unwrap();
    assert!(transform_state_after.is_none());

    let compaction_state_after = flow_process_state_query
        .try_get_process_state(&compaction_binding)
        .await
        .unwrap();
    assert!(compaction_state_after.is_none());

    // Verify listing is empty
    let listing_after = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            None,
        )
        .await
        .unwrap();
    assert!(listing_after.processes.is_empty());

    // Verify rollup shows everything is gone
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

pub async fn test_delete_process_with_history(catalog: &Catalog) {
    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();
    let flow_process_repository = catalog.get_one::<dyn FlowProcessStateRepository>().unwrap();

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"random-dataset-id");
    let flow_binding = ingest_dataset_binding(&dataset_id);

    // Insert a process
    flow_process_repository
        .upsert_process_state_on_trigger_event(
            EventID::new(1),
            flow_binding.clone(),
            false,
            FlowTriggerStopPolicy::default(),
        )
        .await
        .unwrap();

    // Apply some flow results to create history
    let success_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    flow_process_repository
        .apply_flow_result(
            EventID::new(2),
            &flow_binding,
            &FlowOutcome::Success(TaskResult::empty()),
            success_time,
        )
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
        .apply_flow_result(
            EventID::new(4),
            &flow_binding,
            &FlowOutcome::Failed(TaskError::empty_recoverable()),
            failure_time,
        )
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
        .upsert_process_state_on_trigger_event(
            EventID::new(6),
            flow_binding.clone(),
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

    // Delete the flow scope
    flow_process_repository
        .delete_process_states_by_scope(&flow_binding.scope)
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
