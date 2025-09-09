// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_adapter_flow_dataset::{FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM};
use kamu_adapter_flow_webhook::FLOW_TYPE_WEBHOOK_DELIVER;
use kamu_flow_system::*;

use super::csv_flow_process_state_loader::CsvFlowProcessStateLoader;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_from_csv_unfiltered(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test unfiltered listing with default order (recent first) and wide pagination
    let filter = FlowProcessListFilter::all();
    let order = FlowProcessOrder::recent();
    let limit = 100;
    let offset = 0;

    let listing = flow_process_state_query
        .list_processes(filter, order, limit, offset)
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

pub async fn test_list_processes_ordering_last_attempt_at_nulls_last(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // ordering by last_attempt_at ASC - should have non-nulls first, nulls last
    let listing_asc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: false,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify ASC ordering with NULLS LAST
    assert_last_attempt_at_ordering(&listing_asc.processes, false, "ASC ordering test");

    // ordering by last_attempt_at DESC - should have non-nulls first
    // (newest first), nulls last
    let listing_desc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastAttemptAt,
                desc: true,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify DESC ordering with NULLS LAST
    assert_last_attempt_at_ordering(&listing_desc.processes, true, "DESC ordering test");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_pagination(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    let filter = FlowProcessListFilter::all();
    let order = FlowProcessOrder::recent();

    // Get the full list for comparison
    let full_listing = flow_process_state_query
        .list_processes(filter, order, 100, 0)
        .await
        .unwrap();

    // Test pagination: get first 10 processes
    let first_page = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            10,
            0,
        )
        .await
        .unwrap();

    assert_eq!(first_page.processes.len(), 10);

    // Get second page
    let second_page = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            10,
            10,
        )
        .await
        .unwrap();

    assert_eq!(second_page.processes.len(), 10);

    // Get third page (should have 4 remaining)
    let third_page = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            10,
            20,
        )
        .await
        .unwrap();

    assert_eq!(third_page.processes.len(), 4);

    // Verify that the pages don't overlap and together form the complete list
    let mut all_pages = Vec::new();
    all_pages.extend(first_page.processes);
    all_pages.extend(second_page.processes);
    all_pages.extend(third_page.processes);

    assert_eq!(all_pages.len(), 24);

    // Verify that the paginated results match the full list
    assert_eq!(all_pages, full_listing.processes);
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
            100,
            0,
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
            100,
            0,
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
            100,
            0,
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
            100,
            0,
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
            100,
            0,
        )
        .await
        .unwrap();

    assert_eq!(failing_listing.processes.len(), 9);
    assert_eq!(failing_listing.total_count, 9);
    assert_effective_state_distribution(&failing_listing.processes, 0, 9, 0, 0);

    // Test filtering by multiple states
    let multi_state_listing = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all().with_effective_states(&[
                FlowProcessEffectiveState::PausedManual,
                FlowProcessEffectiveState::StoppedAuto,
            ]),
            FlowProcessOrder::recent(),
            100,
            0,
        )
        .await
        .unwrap();

    assert_eq!(multi_state_listing.processes.len(), 6);
    assert_eq!(multi_state_listing.total_count, 6);
    assert_effective_state_distribution(&multi_state_listing.processes, 0, 0, 3, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_time_windows(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by last_failure_since
    let recent_failures = flow_process_state_query
        .list_processes(
            FlowProcessListFilter {
                scope: FlowScopeQuery::all(),
                for_flow_types: None,
                effective_state_in: None,
                last_attempt_between: None,
                last_failure_since: Some(
                    chrono::DateTime::parse_from_rfc3339("2025-09-08T07:00:00Z")
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                ),
                next_planned_before: None,
                next_planned_after: None,
                min_consecutive_failures: None,
                name_contains: None,
            },
            FlowProcessOrder::recent(),
            100,
            0,
        )
        .await
        .unwrap();

    // Should find processes that failed after 07:00 on 2025-09-08
    assert!(!recent_failures.processes.is_empty());
    // All results should have last_failure_at >= 2025-09-08T07:00:00Z
    for process in &recent_failures.processes {
        if let Some(last_failure) = process.last_failure_at() {
            assert!(
                last_failure
                    >= chrono::DateTime::parse_from_rfc3339("2025-09-08T07:00:00Z")
                        .unwrap()
                        .with_timezone(&chrono::Utc)
            );
        }
    }

    // Test filtering by next_planned_before
    let upcoming_soon = flow_process_state_query
        .list_processes(
            FlowProcessListFilter {
                scope: FlowScopeQuery::all(),
                for_flow_types: None,
                effective_state_in: None,
                last_attempt_between: None,
                last_failure_since: None,
                next_planned_before: Some(
                    chrono::DateTime::parse_from_rfc3339("2025-09-08T10:00:00Z")
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                ),
                next_planned_after: None,
                min_consecutive_failures: None,
                name_contains: None,
            },
            FlowProcessOrder::recent(),
            100,
            0,
        )
        .await
        .unwrap();

    // Should find processes scheduled before 10:00
    assert!(!upcoming_soon.processes.is_empty());
    for process in &upcoming_soon.processes {
        if let Some(next_planned) = process.next_planned_at() {
            assert!(
                next_planned
                    <= chrono::DateTime::parse_from_rfc3339("2025-09-08T10:00:00Z")
                        .unwrap()
                        .with_timezone(&chrono::Utc)
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_consecutive_failures(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by minimum consecutive failures
    let chronic_failures = flow_process_state_query
        .list_processes(
            FlowProcessListFilter {
                scope: FlowScopeQuery::all(),
                for_flow_types: None,
                effective_state_in: None,
                last_attempt_between: None,
                last_failure_since: None,
                next_planned_before: None,
                next_planned_after: None,
                min_consecutive_failures: Some(3),
                name_contains: None,
            },
            FlowProcessOrder::recent(),
            100,
            0,
        )
        .await
        .unwrap();

    // Should find processes with 3 or more consecutive failures
    assert!(!chronic_failures.processes.is_empty());
    for process in &chronic_failures.processes {
        assert!(process.consecutive_failures() >= 3);
    }

    // Test filtering by very high consecutive failures
    let severe_failures = flow_process_state_query
        .list_processes(
            FlowProcessListFilter {
                scope: FlowScopeQuery::all(),
                for_flow_types: None,
                effective_state_in: None,
                last_attempt_between: None,
                last_failure_since: None,
                next_planned_before: None,
                next_planned_after: None,
                min_consecutive_failures: Some(10),
                name_contains: None,
            },
            FlowProcessOrder::recent(),
            100,
            0,
        )
        .await
        .unwrap();

    // Should find only the most severe cases (10+ failures)
    for process in &severe_failures.processes {
        assert!(process.consecutive_failures() >= 10);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_filter_by_name_contains(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by name containing "acme"
    let acme_processes = flow_process_state_query
        .list_processes(
            FlowProcessListFilter {
                scope: FlowScopeQuery::all(),
                for_flow_types: None,
                effective_state_in: None,
                last_attempt_between: None,
                last_failure_since: None,
                next_planned_before: None,
                next_planned_after: None,
                min_consecutive_failures: None,
                name_contains: Some("acme"),
            },
            FlowProcessOrder::recent(),
            100,
            0,
        )
        .await
        .unwrap();

    // Should find processes with sort keys starting with "acme" (prefix matching)
    assert!(!acme_processes.processes.is_empty());
    for process in &acme_processes.processes {
        assert!(process.sort_key().to_lowercase().starts_with("acme"));
    }

    // Test filtering by name starting with "beta"
    let beta_processes = flow_process_state_query
        .list_processes(
            FlowProcessListFilter {
                scope: FlowScopeQuery::all(),
                for_flow_types: None,
                effective_state_in: None,
                last_attempt_between: None,
                last_failure_since: None,
                next_planned_before: None,
                next_planned_after: None,
                min_consecutive_failures: None,
                name_contains: Some("beta"),
            },
            FlowProcessOrder::recent(),
            100,
            0,
        )
        .await
        .unwrap();

    // Should find processes with sort keys starting with "beta" (prefix matching)
    assert!(!beta_processes.processes.is_empty());
    for process in &beta_processes.processes {
        assert!(process.sort_key().to_lowercase().starts_with("beta"));
    }
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
            100,
            0,
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
            FlowProcessListFilter {
                scope: FlowScopeQuery::all(),
                for_flow_types: Some(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]),
                effective_state_in: Some(&[
                    FlowProcessEffectiveState::Active,
                    FlowProcessEffectiveState::Failing,
                ]),
                last_attempt_between: None,
                last_failure_since: None,
                next_planned_before: None,
                next_planned_after: None,
                min_consecutive_failures: None,
                name_contains: Some("acme"),
            },
            FlowProcessOrder::recent(),
            100,
            0,
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

        // Check name contains
        assert!(process.sort_key().to_lowercase().contains("acme"));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_flow_type_distribution(
    processes: &[FlowProcessState],
    expected_ingest: usize,
    expected_transform: usize,
    expected_webhook: usize,
) {
    let ingest_count = processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_DATASET_INGEST)
        .count();
    let transform_count = processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_DATASET_TRANSFORM)
        .count();
    let webhook_count = processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_WEBHOOK_DELIVER)
        .count();

    assert_eq!(ingest_count, expected_ingest);
    assert_eq!(transform_count, expected_transform);
    assert_eq!(webhook_count, expected_webhook);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_effective_state_distribution(
    processes: &[FlowProcessState],
    expected_active: usize,
    expected_failing: usize,
    expected_paused: usize,
    expected_stopped: usize,
) {
    let active_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::Active)
        .count();
    let failing_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::Failing)
        .count();
    let paused_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::PausedManual)
        .count();
    let stopped_count = processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::StoppedAuto)
        .count();

    assert_eq!(active_count, expected_active);
    assert_eq!(failing_count, expected_failing);
    assert_eq!(paused_count, expected_paused);
    assert_eq!(stopped_count, expected_stopped);
}

fn assert_last_attempt_at_ordering(processes: &[FlowProcessState], desc: bool, context: &str) {
    // Check NULLS LAST behavior: non-null values should come before null values
    let mut found_null = false;
    for process in processes {
        match process.last_attempt_at() {
            Some(_) => {
                // If we already found a null, this violates NULLS LAST
                assert!(
                    !found_null,
                    "Found non-null last_attempt_at after null value when ordering {} in {}",
                    if desc { "DESC" } else { "ASC" },
                    context
                );
            }
            None => {
                found_null = true;
            }
        }
    }

    // Check ordering among non-null values
    let non_null_dates: Vec<_> = processes
        .iter()
        .filter_map(FlowProcessState::last_attempt_at)
        .collect();

    for i in 1..non_null_dates.len() {
        if desc {
            // DESC: newer dates should come first (dates[i-1] >= dates[i])
            assert!(
                non_null_dates[i - 1] >= non_null_dates[i],
                "Dates not in descending order in {}: {:?} should be >= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        } else {
            // ASC: older dates should come first (dates[i-1] <= dates[i])
            assert!(
                non_null_dates[i - 1] <= non_null_dates[i],
                "Dates not in ascending order in {}: {:?} should be <= {:?}",
                context,
                non_null_dates[i - 1],
                non_null_dates[i]
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
