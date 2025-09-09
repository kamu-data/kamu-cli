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

    // Verify ordering: recent first (by last_attempt_at DESC)
    assert_last_attempt_at_ordering(
        &listing.processes,
        true,
        "test_list_processes_from_csv_unfiltered",
    );

    // Verify that we have the expected distribution of flow types
    let ingest_count = listing
        .processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_DATASET_INGEST)
        .count();
    let transform_count = listing
        .processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_DATASET_TRANSFORM)
        .count();
    let webhook_count = listing
        .processes
        .iter()
        .filter(|p| p.flow_binding().flow_type == FLOW_TYPE_WEBHOOK_DELIVER)
        .count();

    assert_eq!(ingest_count, 6);
    assert_eq!(transform_count, 6);
    assert_eq!(webhook_count, 12);

    // Verify that we have the expected distribution of effective states
    let active_count = listing
        .processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::Active)
        .count();
    let failing_count = listing
        .processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::Failing)
        .count();
    let paused_count = listing
        .processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::PausedManual)
        .count();
    let stopped_count = listing
        .processes
        .iter()
        .filter(|p| p.effective_state() == FlowProcessEffectiveState::StoppedAuto)
        .count();

    assert_eq!(active_count, 9);
    assert_eq!(failing_count, 9);
    assert_eq!(paused_count, 3);
    assert_eq!(stopped_count, 3);

    // Test pagination: get first 10 processes
    let first_page = flow_process_state_query
        .list_processes(FlowProcessListFilter::all(), order, 10, 0)
        .await
        .unwrap();

    assert_eq!(first_page.processes.len(), 10);

    // Get second page
    let second_page = flow_process_state_query
        .list_processes(FlowProcessListFilter::all(), order, 10, 10)
        .await
        .unwrap();

    assert_eq!(second_page.processes.len(), 10);

    // Get third page (should have 4 remaining)
    let third_page = flow_process_state_query
        .list_processes(FlowProcessListFilter::all(), order, 10, 20)
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
    assert_eq!(all_pages, listing.processes);
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
