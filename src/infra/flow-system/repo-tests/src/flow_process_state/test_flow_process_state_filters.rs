// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use dill::Catalog;
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_flow_system::*;

use super::csv_flow_process_state_loader::CsvFlowProcessStateLoader;
use crate::flow_process_state::helpers::{
    assert_effective_state_distribution,
    assert_flow_type_distribution,
    assert_last_attempt_at_ordering,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_from_csv_unfiltered_with_default_ordering(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test unfiltered listing with default order (recent first) and wide pagination
    let filter = FlowProcessListFilter::all();
    let order = FlowProcessOrder::recent();

    let listing = flow_process_state_query
        .list_processes(filter, order, None)
        .await
        .unwrap();

    // Should get all 24 processes from CSV
    assert_eq!(listing.processes.len(), 24);
    assert_eq!(listing.total_count, 24);

    // Verify ordering: recent first (by last_attempt_at DESC)
    assert_last_attempt_at_ordering(
        &listing.processes,
        true,
        "test_list_processes_from_csv_unfiltered",
    );

    // Verify that we have the expected distribution of flow types
    assert_flow_type_distribution(&listing.processes, 6, 6, 12);

    // Verify that we have the expected distribution of effective states
    assert_effective_state_distribution(&listing.processes, 9, 9, 3, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_flow_types(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by ingest flows only
    let ingest_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().for_flow_types(&[FLOW_TYPE_DATASET_INGEST]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(ingest_listing.processes.len(), 6);
    assert_eq!(ingest_listing.total_count, 6);
    assert_flow_type_distribution(&ingest_listing.processes, 6, 0, 0);

    // Test filtering by webhook flows only
    let webhook_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().for_flow_types(&[FLOW_TYPE_WEBHOOK_DELIVER]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(webhook_listing.processes.len(), 12);
    assert_eq!(webhook_listing.total_count, 12);
    assert_flow_type_distribution(&webhook_listing.processes, 0, 0, 12);

    // Test filtering by multiple flow types
    let multi_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all()
                .for_flow_types(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(multi_listing.processes.len(), 12);
    assert_eq!(multi_listing.total_count, 12);
    assert_flow_type_distribution(&multi_listing.processes, 6, 6, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_effective_states(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by active state only
    let active_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all()
                .with_effective_states(&[FlowProcessEffectiveState::Active]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(active_listing.processes.len(), 9);
    assert_eq!(active_listing.total_count, 9);
    assert_effective_state_distribution(&active_listing.processes, 9, 0, 0, 0);

    // Test filtering by failing state only
    let failing_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all()
                .with_effective_states(&[FlowProcessEffectiveState::Failing]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(failing_listing.processes.len(), 9);
    assert_eq!(failing_listing.total_count, 9);
    assert_effective_state_distribution(&failing_listing.processes, 0, 9, 0, 0);

    // Test filtering by paused manual state only
    let paused_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all()
                .with_effective_states(&[FlowProcessEffectiveState::PausedManual]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(paused_listing.processes.len(), 3);
    assert_eq!(paused_listing.total_count, 3);
    assert_effective_state_distribution(&paused_listing.processes, 0, 0, 3, 0);

    // Test filtering by stopped auto state only
    let stopped_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all()
                .with_effective_states(&[FlowProcessEffectiveState::StoppedAuto]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(stopped_listing.processes.len(), 3);
    assert_eq!(stopped_listing.total_count, 3);
    assert_effective_state_distribution(&stopped_listing.processes, 0, 0, 0, 3);

    // Test filtering by multiple states
    let multi_state_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_effective_states(&[
                FlowProcessEffectiveState::PausedManual,
                FlowProcessEffectiveState::StoppedAuto,
            ]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(multi_state_listing.processes.len(), 6);
    assert_eq!(multi_state_listing.total_count, 6);
    assert_effective_state_distribution(&multi_state_listing.processes, 0, 0, 3, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_last_attempt_between(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Define time constants
    const START_TIME_STR: &str = "2025-09-08T07:00:00Z";
    const END_TIME_STR: &str = "2025-09-08T12:00:00Z";
    let start_time = DateTime::parse_from_rfc3339(START_TIME_STR)
        .unwrap()
        .with_timezone(&Utc);
    let end_time = DateTime::parse_from_rfc3339(END_TIME_STR)
        .unwrap()
        .with_timezone(&Utc);

    // Test filtering by last_attempt_between
    let time_window_attempts = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_last_attempt_between(start_time, end_time),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find processes with last attempts between 07:00 and 12:00 on
    // 2025-09-08
    assert!(!time_window_attempts.processes.is_empty());
    assert_eq!(time_window_attempts.processes.len(), 15);
    assert_eq!(time_window_attempts.total_count, 15);
    assert_flow_type_distribution(&time_window_attempts.processes, 4, 3, 8);
    assert_effective_state_distribution(&time_window_attempts.processes, 6, 7, 0, 2);

    // All results should have last_attempt_at within the specified range
    // (inclusive)
    for process in &time_window_attempts.processes {
        if let Some(last_attempt) = process.last_attempt_at() {
            assert!(
                last_attempt >= start_time && last_attempt <= end_time,
                "last_attempt_at {last_attempt:?} should be between {start_time:?} and \
                 {end_time:?}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_last_failure_since(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Define time constant
    const FAILURE_SINCE_STR: &str = "2025-09-08T07:00:00Z";
    let failure_since = DateTime::parse_from_rfc3339(FAILURE_SINCE_STR)
        .unwrap()
        .with_timezone(&Utc);

    // Test filtering by last_failure_since
    let recent_failures = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_last_failure_since(failure_since),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find processes that failed after 07:00 on 2025-09-08
    assert!(!recent_failures.processes.is_empty());
    assert_eq!(recent_failures.processes.len(), 9);
    assert_eq!(recent_failures.total_count, 9);
    assert_flow_type_distribution(&recent_failures.processes, 2, 2, 5);
    assert_effective_state_distribution(&recent_failures.processes, 0, 7, 0, 2);

    // All results should have last_failure_at >= 2025-09-08T07:00:00Z
    for process in &recent_failures.processes {
        if let Some(last_failure) = process.last_failure_at() {
            assert!(last_failure >= failure_since);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_planned_before(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Define time constant
    const PLANNED_BEFORE_STR: &str = "2025-09-08T10:00:00Z";
    let planned_before = DateTime::parse_from_rfc3339(PLANNED_BEFORE_STR)
        .unwrap()
        .with_timezone(&Utc);

    // Test filtering by next_planned_before
    let upcoming_soon = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_next_planned_before(planned_before),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find processes scheduled before 10:00
    assert!(!upcoming_soon.processes.is_empty());

    // All implementations should consistently return 3 processes
    // (excluding the process at exactly 2025-09-08T10:00:00Z)
    assert_eq!(upcoming_soon.processes.len(), 3);
    assert_eq!(upcoming_soon.total_count, 3);
    assert_flow_type_distribution(&upcoming_soon.processes, 3, 0, 0); // (3 ingest, 0 transform, 0 webhook)
    assert_effective_state_distribution(&upcoming_soon.processes, 2, 1, 0, 0); // (2 active, 1 failing, 0 paused, 0 stopped)

    for process in &upcoming_soon.processes {
        if let Some(next_planned) = process.next_planned_at() {
            assert!(next_planned <= planned_before);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_planned_after(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Define time constant
    const PLANNED_AFTER_STR: &str = "2025-09-08T11:30:00Z";
    let planned_after = DateTime::parse_from_rfc3339(PLANNED_AFTER_STR)
        .unwrap()
        .with_timezone(&Utc);

    // Test filtering by next_planned_after
    let future_scheduled = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_next_planned_after(planned_after),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find processes scheduled after 11:30 on 2025-09-08
    // From CSV: 12:00:00Z (zeta/metrics.daily), 12:00:00Z (acme/logs webhook),
    // 14:00:00Z (gamma/audit) The filter is "after" so 11:30:00Z should be
    // excluded, leaving 3 results
    assert!(!future_scheduled.processes.is_empty());
    assert_eq!(future_scheduled.processes.len(), 3);
    assert_eq!(future_scheduled.total_count, 3);
    assert_flow_type_distribution(&future_scheduled.processes, 1, 1, 1);
    assert_effective_state_distribution(&future_scheduled.processes, 1, 2, 0, 0);

    // All results should have next_planned_at > 2025-09-08T11:30:00Z
    for process in &future_scheduled.processes {
        if let Some(next_planned) = process.next_planned_at() {
            assert!(next_planned > planned_after);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_consecutive_failures(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by 0 consecutive failures - should return all processes
    let all_processes = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_min_consecutive_failures(0),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find all 24 processes (0+ failures includes everything)
    assert_eq!(all_processes.processes.len(), 24);
    assert_eq!(all_processes.total_count, 24);
    assert_flow_type_distribution(&all_processes.processes, 6, 6, 12);
    assert_effective_state_distribution(&all_processes.processes, 9, 9, 3, 3);

    // Test filtering by minimum consecutive failures
    let chronic_failures = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_min_consecutive_failures(3),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find processes with 3 or more consecutive failures
    assert!(!chronic_failures.processes.is_empty());
    assert_eq!(chronic_failures.processes.len(), 4);
    assert_eq!(chronic_failures.total_count, 4);
    assert_flow_type_distribution(&chronic_failures.processes, 1, 0, 3);
    assert_effective_state_distribution(&chronic_failures.processes, 0, 2, 0, 2);

    for process in &chronic_failures.processes {
        assert!(process.consecutive_failures() >= 3);
    }

    // Test filtering by very high consecutive failures
    let severe_failures = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_min_consecutive_failures(10),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find only the most severe cases (10+ failures)
    assert_eq!(severe_failures.processes.len(), 1);
    assert_eq!(severe_failures.total_count, 1);
    assert_flow_type_distribution(&severe_failures.processes, 0, 0, 1);
    assert_effective_state_distribution(&severe_failures.processes, 0, 0, 0, 1);

    for process in &severe_failures.processes {
        assert!(process.consecutive_failures() >= 10);
    }

    // Test filtering by consecutive failures higher than any data in dataset
    let no_results = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_min_consecutive_failures(11),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find no processes (11+ failures exceeds maximum in dataset)
    assert!(no_results.processes.is_empty());
    assert_eq!(no_results.processes.len(), 0);
    assert_eq!(no_results.total_count, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_scope(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Create dataset IDs for reuse throughout tests
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"acme/orders");
    let dataset_id2 = odf::DatasetID::new_seeded_ed25519(b"acme/users");
    let dataset_id3 = odf::DatasetID::new_seeded_ed25519(b"beta/catalog");

    // Test 1: Filter by single dataset scope
    let single_dataset_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);

    let single_dataset_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::for_scope(single_dataset_query),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find processes for acme/orders dataset (rows 1, 5, 6, 7 = 4 total)
    assert_eq!(single_dataset_listing.processes.len(), 4);
    assert_eq!(single_dataset_listing.total_count, 4);
    assert_flow_type_distribution(&single_dataset_listing.processes, 1, 0, 3);
    assert_effective_state_distribution(&single_dataset_listing.processes, 2, 1, 1, 0);

    // Verify all processes are related to acme/orders dataset
    for process in &single_dataset_listing.processes {
        let scope = &process.flow_binding().scope;
        match scope.scope_type() {
            "Dataset" => {
                let dataset_scope = FlowScopeDataset::new(scope);
                assert_eq!(dataset_scope.dataset_id(), dataset_id.clone());
            }
            "WebhookSubscription" => {
                let webhook_scope = FlowScopeSubscription::new(scope);
                assert_eq!(webhook_scope.maybe_dataset_id(), Some(dataset_id.clone()));
            }
            _ => panic!("Unexpected scope type: {}", scope.scope_type()),
        }
    }

    // Test 2: Filter by multiple datasets scope
    let multi_dataset_query =
        FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id, &dataset_id2, &dataset_id3]);

    let multi_dataset_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::for_scope(multi_dataset_query),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // acme/orders (4) + acme/users (3) + beta/catalog (4) = 11 total
    assert_eq!(multi_dataset_listing.processes.len(), 11);
    assert_eq!(multi_dataset_listing.total_count, 11);
    assert_flow_type_distribution(&multi_dataset_listing.processes, 3, 0, 8);
    assert_effective_state_distribution(&multi_dataset_listing.processes, 3, 5, 1, 2);

    // Test 3: Filter by webhook subscriptions for single dataset
    let subscription_query = FlowScopeSubscription::query_for_subscriptions_of_dataset(&dataset_id);

    let subscription_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::for_scope(subscription_query),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find webhook subscription processes for acme/orders dataset (rows 5,
    // 6, 7 = 3 total)
    assert_eq!(subscription_listing.processes.len(), 3);
    assert_eq!(subscription_listing.total_count, 3);
    assert_flow_type_distribution(&subscription_listing.processes, 0, 0, 3);
    assert_effective_state_distribution(&subscription_listing.processes, 1, 1, 1, 0);

    // Verify all processes are webhook subscriptions for the correct dataset
    for process in &subscription_listing.processes {
        let scope = &process.flow_binding().scope;
        assert_eq!(scope.scope_type(), "WebhookSubscription");
        let webhook_scope = FlowScopeSubscription::new(scope);
        assert_eq!(webhook_scope.maybe_dataset_id(), Some(dataset_id.clone()));
        assert_eq!(process.flow_binding().flow_type, FLOW_TYPE_WEBHOOK_DELIVER);
    }

    // Test 4: Filter by dataset scopes only (exclude webhook subscriptions)
    let dataset_only_query = FlowScopeDataset::query_for_single_dataset_only(&dataset_id);

    let dataset_only_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::for_scope(dataset_only_query),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find only Dataset scope processes for acme/orders (row 1 = 1 total)
    assert_eq!(dataset_only_listing.processes.len(), 1);
    assert_eq!(dataset_only_listing.total_count, 1);
    assert_flow_type_distribution(&dataset_only_listing.processes, 1, 0, 0);
    assert_effective_state_distribution(&dataset_only_listing.processes, 1, 0, 0, 0);

    // Verify the process is a dataset scope, not webhook subscription
    let process = &dataset_only_listing.processes[0];
    let scope = &process.flow_binding().scope;
    assert_eq!(scope.scope_type(), "Dataset");
    let dataset_scope = FlowScopeDataset::new(scope);
    assert_eq!(dataset_scope.dataset_id(), dataset_id.clone());

    // Test 5: Filter by webhook subscriptions regardless of dataset
    let all_webhooks_query = FlowScopeSubscription::query_for_all_subscriptions();

    let all_webhooks_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::for_scope(all_webhooks_query),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find all webhook subscription processes (12 total from CSV)
    assert_eq!(all_webhooks_listing.processes.len(), 12);
    assert_eq!(all_webhooks_listing.total_count, 12);
    assert_flow_type_distribution(&all_webhooks_listing.processes, 0, 0, 12);
    assert_effective_state_distribution(&all_webhooks_listing.processes, 4, 4, 1, 3);

    // Verify all processes are webhook subscriptions
    for process in &all_webhooks_listing.processes {
        let scope = &process.flow_binding().scope;
        assert_eq!(scope.scope_type(), "WebhookSubscription");
        assert_eq!(process.flow_binding().flow_type, FLOW_TYPE_WEBHOOK_DELIVER);
    }

    // Test 6: Filter by webhook subscriptions for multiple datasets
    let multi_webhook_query =
        FlowScopeSubscription::query_for_subscriptions_of_multiple_datasets(&[
            &dataset_id,
            &dataset_id2,
        ]);

    let multi_webhook_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::for_scope(multi_webhook_query),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find webhook subscriptions for acme/orders and acme/users (5 total)
    assert_eq!(multi_webhook_listing.processes.len(), 5);
    assert_eq!(multi_webhook_listing.total_count, 5);
    assert_flow_type_distribution(&multi_webhook_listing.processes, 0, 0, 5);
    assert_effective_state_distribution(&multi_webhook_listing.processes, 2, 2, 1, 0);

    // Verify all processes are webhook subscriptions for the correct datasets
    for process in &multi_webhook_listing.processes {
        let scope = &process.flow_binding().scope;
        assert_eq!(scope.scope_type(), "WebhookSubscription");
        let webhook_scope = FlowScopeSubscription::new(scope);
        let process_dataset_id = webhook_scope.maybe_dataset_id();
        assert!(
            process_dataset_id == Some(dataset_id.clone())
                || process_dataset_id == Some(dataset_id2.clone())
        );
        assert_eq!(process.flow_binding().flow_type, FLOW_TYPE_WEBHOOK_DELIVER);
    }

    // Test 7: Filter by system flows (should return empty since our CSV has no
    // system flows)
    let system_query = FlowScopeQuery::build_for_system_scope();

    let system_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::for_scope(system_query),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find no system flows in our test data
    assert!(system_listing.processes.is_empty());
    assert_eq!(system_listing.processes.len(), 0);
    assert_eq!(system_listing.total_count, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_combined_filters(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test combining flow type and effective state filters
    let failing_webhooks = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all()
                .for_flow_types(&[FLOW_TYPE_WEBHOOK_DELIVER])
                .with_effective_states(&[FlowProcessEffectiveState::Failing]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find only failing webhook processes
    assert!(!failing_webhooks.processes.is_empty());
    assert_flow_type_distribution(
        &failing_webhooks.processes,
        0,
        0,
        failing_webhooks.processes.len(),
    );
    assert_effective_state_distribution(
        &failing_webhooks.processes,
        0,
        failing_webhooks.processes.len(),
        0,
        0,
    );

    // Test combining multiple filters
    let complex_filter = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all()
                .for_flow_types(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM])
                .with_effective_states(&[
                    FlowProcessEffectiveState::Active,
                    FlowProcessEffectiveState::Failing,
                ]),
            FlowProcessOrder::recent(),
            None,
        )
        .await
        .unwrap();

    // Should find only acme ingest/transform processes that are active or failing
    assert!(!complex_filter.processes.is_empty());
    for process in &complex_filter.processes {
        // Check flow type
        let flow_type = &process.flow_binding().flow_type;
        assert!(flow_type == FLOW_TYPE_DATASET_INGEST || flow_type == FLOW_TYPE_DATASET_TRANSFORM);

        // Check effective state
        let state = process.effective_state();
        assert!(
            state == FlowProcessEffectiveState::Active
                || state == FlowProcessEffectiveState::Failing
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
