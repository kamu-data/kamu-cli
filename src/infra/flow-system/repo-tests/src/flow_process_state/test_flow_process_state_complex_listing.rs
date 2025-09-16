// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use dill::Catalog;
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_adapter_flow_webhook::FLOW_TYPE_WEBHOOK_DELIVER;
use kamu_flow_system::*;

use super::csv_flow_process_state_loader::CsvFlowProcessStateLoader;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_page_recent_activity(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Create dataset scope for "acme/orders"
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"acme/orders");
    let scope_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);

    // Setup ordering: LastAttemptAt DESC
    let ordering = FlowProcessOrder {
        field: FlowProcessOrderField::LastAttemptAt,
        desc: true,
    };

    // Filter: all types and states for the specific dataset scope
    let filter = FlowProcessListFilter::for_scope(scope_query);

    // Test first page (limit=3, offset=0)
    let first_page = flow_process_state_query
        .list_processes(
            filter,
            ordering,
            Some(PaginationOpts {
                limit: 3,
                offset: 0,
            }),
        )
        .await
        .unwrap();

    // Verify first page results
    assert_eq!(first_page.processes.len(), 3);
    assert_eq!(first_page.total_count, 4); // Total of 4 processes for acme/orders dataset (excludes derived datasets)

    // Verify ordering: processes should be ordered by last_attempt_at DESC
    // From CSV data, expected order based on last_attempt_at:
    // 1. Row 5 (WEBHOOK sub-slack-acme-orders): 2025-09-08T08:06:00Z
    // 2. Row 1 (INGEST acme/orders): 2025-09-08T08:05:00Z
    // 3. Row 6 (WEBHOOK sub-ops-acme-orders): 2025-09-08T07:41:00Z

    let first_process = &first_page.processes[0];
    assert_eq!(
        first_process.flow_binding().flow_type,
        FLOW_TYPE_WEBHOOK_DELIVER
    );

    let second_process = &first_page.processes[1];
    assert_eq!(
        second_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_INGEST
    );

    let third_process = &first_page.processes[2];
    assert_eq!(
        third_process.flow_binding().flow_type,
        FLOW_TYPE_WEBHOOK_DELIVER
    );

    // Verify timestamps are in descending order
    let first_attempt = first_process.last_attempt_at().unwrap();
    let second_attempt = second_process.last_attempt_at().unwrap();
    let third_attempt = third_process.last_attempt_at().unwrap();

    assert!(first_attempt >= second_attempt);
    assert!(second_attempt >= third_attempt);

    // Test second page (limit=3, offset=3)
    let scope_query2 = FlowScopeDataset::query_for_single_dataset(&dataset_id);
    let filter2 = FlowProcessListFilter::for_scope(scope_query2);
    let second_page = flow_process_state_query
        .list_processes(
            filter2,
            ordering,
            Some(PaginationOpts {
                limit: 3,
                offset: 3,
            }),
        )
        .await
        .unwrap();

    // Verify second page results
    assert_eq!(second_page.processes.len(), 1); // Only 1 remaining process
    assert_eq!(second_page.total_count, 4); // Same total count

    // Verify remaining process:
    // 4. Row 7 (WEBHOOK sub-arch-acme-orders): 2025-09-07T12:00:00Z

    let fourth_process = &second_page.processes[0];
    assert_eq!(
        fourth_process.flow_binding().flow_type,
        FLOW_TYPE_WEBHOOK_DELIVER
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_account_dashboard_triage(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Setup ordering: ConsecutiveFailures DESC, then LastAttemptAt DESC (implicit)
    let ordering = FlowProcessOrder {
        field: FlowProcessOrderField::ConsecutiveFailures,
        desc: true,
    };

    // Filter: all scopes, all types, but only stopped_auto and failing states
    let filter = FlowProcessListFilter::all().with_effective_states(&[
        FlowProcessEffectiveState::StoppedAuto,
        FlowProcessEffectiveState::Failing,
    ]);

    // Test first page only (limit=5, offset=0)
    let triage_page = flow_process_state_query
        .list_processes(
            filter,
            ordering,
            Some(PaginationOpts {
                limit: 5,
                offset: 0,
            }),
        )
        .await
        .unwrap();

    // Verify triage page results
    assert_eq!(triage_page.processes.len(), 5);

    // From CSV data, failing and stopped_auto processes with their
    // consecutive_failures: Row 24: WEBHOOK beta/catalog/legacy_partner
    // (stopped_auto, 10 failures) Row 16: WEBHOOK zeta/metrics/ops_webhook
    // (stopped_auto, 5 failures) Row 21: INGEST gamma/audit (failing, 4
    // failures) Row 19: WEBHOOK acme/logs/security_monitor (failing, 3
    // failures) Row 4: EXECUTE_TRANSFORM beta/catalog.daily (failing, 2
    // failures)

    let first_process = &triage_page.processes[0];
    assert_eq!(
        first_process.flow_binding().flow_type,
        FLOW_TYPE_WEBHOOK_DELIVER
    );
    assert_eq!(first_process.consecutive_failures(), 10);
    assert_eq!(
        first_process.effective_state(),
        FlowProcessEffectiveState::StoppedAuto
    );

    let second_process = &triage_page.processes[1];
    assert_eq!(
        second_process.flow_binding().flow_type,
        FLOW_TYPE_WEBHOOK_DELIVER
    );
    assert_eq!(second_process.consecutive_failures(), 5);
    assert_eq!(
        second_process.effective_state(),
        FlowProcessEffectiveState::StoppedAuto
    );

    let third_process = &triage_page.processes[2];
    assert_eq!(
        third_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_INGEST
    );
    assert_eq!(third_process.consecutive_failures(), 4);
    assert_eq!(
        third_process.effective_state(),
        FlowProcessEffectiveState::Failing
    );

    let fourth_process = &triage_page.processes[3];
    assert_eq!(
        fourth_process.flow_binding().flow_type,
        FLOW_TYPE_WEBHOOK_DELIVER
    );
    assert_eq!(fourth_process.consecutive_failures(), 3);
    assert_eq!(
        fourth_process.effective_state(),
        FlowProcessEffectiveState::Failing
    );

    let fifth_process = &triage_page.processes[4];
    assert_eq!(
        fifth_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_TRANSFORM
    );
    assert_eq!(fifth_process.consecutive_failures(), 2);
    assert_eq!(
        fifth_process.effective_state(),
        FlowProcessEffectiveState::Failing
    );

    // Verify ordering: consecutive failures should be in descending order
    for i in 1..triage_page.processes.len() {
        let prev_failures = triage_page.processes[i - 1].consecutive_failures();
        let curr_failures = triage_page.processes[i].consecutive_failures();
        assert!(
            prev_failures >= curr_failures,
            "Process {} has {} failures, but process {} has {} failures (should be descending)",
            i - 1,
            prev_failures,
            i,
            curr_failures
        );
    }

    // Verify all processes are in the required states
    for process in &triage_page.processes {
        let state = process.effective_state();
        assert!(
            state == FlowProcessEffectiveState::StoppedAuto
                || state == FlowProcessEffectiveState::Failing,
            "Process {:?} has state {:?}, but should be StoppedAuto or Failing",
            process.flow_binding(),
            state
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_account_dashboard_upcoming_updates(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Setup ordering: NextPlannedAt ASC, SortKey ASC (NULLS LAST)
    let ordering = FlowProcessOrder {
        field: FlowProcessOrderField::NextPlannedAt,
        desc: false, // ASC
    };

    // Filter: all scopes, only INGEST and EXECUTE_TRANSFORM types, all states
    let filter = FlowProcessListFilter::all()
        .for_flow_types(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]);

    // Test first page (limit=5, offset=0)
    let first_page = flow_process_state_query
        .list_processes(
            filter,
            ordering,
            Some(PaginationOpts {
                limit: 5,
                offset: 0,
            }),
        )
        .await
        .unwrap();

    // Verify first page results
    assert_eq!(first_page.processes.len(), 5);

    // From CSV data, INGEST and EXECUTE_TRANSFORM processes with next_planned_at
    // (ASC order): Row 18: EXECUTE_TRANSFORM acme/logs.processed
    // (2025-09-07T19:00:00Z) [calculated from NULL + rule] Row 3: INGEST
    // acme/logs (2025-09-07T21:00:00Z) [calculated from NULL + rule]
    // Row 11: INGEST zeta/metrics (2025-09-08T08:30:00Z)
    // Row 1: INGEST acme/orders (2025-09-08T09:00:00Z)
    // Row 17: INGEST beta/catalog (2025-09-08T09:30:00Z)

    let first_process = &first_page.processes[0];
    assert_eq!(
        first_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_TRANSFORM
    );
    assert_eq!(
        first_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-07T19:00:00+00:00"
    );

    let second_process = &first_page.processes[1];
    assert_eq!(
        second_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_INGEST
    );
    assert_eq!(
        second_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-07T21:00:00+00:00"
    );

    let third_process = &first_page.processes[2];
    assert_eq!(
        third_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_INGEST
    );
    assert_eq!(
        third_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T08:30:00+00:00"
    );

    let fourth_process = &first_page.processes[3];
    assert_eq!(
        fourth_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_INGEST
    );
    assert_eq!(
        fourth_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T09:00:00+00:00"
    );

    let fifth_process = &first_page.processes[4];
    assert_eq!(
        fifth_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_INGEST
    );
    assert_eq!(
        fifth_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T09:30:00+00:00"
    );

    // Verify ordering: next_planned_at should be in ascending order
    for i in 1..first_page.processes.len() {
        let prev_planned = first_page.processes[i - 1].next_planned_at();
        let curr_planned = first_page.processes[i].next_planned_at();

        // Both should have planned times in first page
        assert!(prev_planned.is_some() && curr_planned.is_some());
        assert!(
            prev_planned.unwrap() <= curr_planned.unwrap(),
            "Process {} has planned time {:?}, but process {} has planned time {:?} (should be \
             ascending)",
            i - 1,
            prev_planned,
            i,
            curr_planned
        );
    }

    // Test second page (limit=5, offset=5)
    let filter2 = FlowProcessListFilter::all()
        .for_flow_types(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]);
    let second_page = flow_process_state_query
        .list_processes(
            filter2,
            ordering,
            Some(PaginationOpts {
                limit: 5,
                offset: 5,
            }),
        )
        .await
        .unwrap();

    // Verify second page results (continuing from NextPlannedAt ASC order):
    // Process 5: EXECUTE_TRANSFORM acme/users.daily (2025-09-08T10:00:00Z)
    // Process 6: EXECUTE_TRANSFORM beta/catalog.daily (2025-09-08T10:15:00Z)
    // Process 7: EXECUTE_TRANSFORM acme/orders.daily/transform:enrichment
    // (2025-09-08T10:30:00Z) Process 8: INGEST acme/users
    // (2025-09-08T11:00:00Z) Process 9: EXECUTE_TRANSFORM
    // zeta/metrics.daily/transform:daily_rollup (2025-09-08T12:00:00Z)

    assert_eq!(second_page.processes.len(), 5);

    let sixth_process = &second_page.processes[0];
    assert_eq!(
        sixth_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_TRANSFORM
    );
    assert_eq!(
        sixth_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T10:00:00+00:00"
    );

    let seventh_process = &second_page.processes[1];
    assert_eq!(
        seventh_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_TRANSFORM
    );
    assert_eq!(
        seventh_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T10:15:00+00:00"
    );

    let eighth_process = &second_page.processes[2];
    assert_eq!(
        eighth_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_TRANSFORM
    );
    assert_eq!(
        eighth_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T10:30:00+00:00"
    );

    let ninth_process = &second_page.processes[3];
    assert_eq!(
        ninth_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_INGEST
    );
    assert_eq!(
        ninth_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T11:00:00+00:00"
    );

    let tenth_process = &second_page.processes[4];
    assert_eq!(
        tenth_process.flow_binding().flow_type,
        FLOW_TYPE_DATASET_TRANSFORM
    );
    assert_eq!(
        tenth_process.next_planned_at().unwrap().to_rfc3339(),
        "2025-09-08T12:00:00+00:00"
    );

    // Verify only INGEST and EXECUTE_TRANSFORM types
    for process in first_page
        .processes
        .iter()
        .chain(second_page.processes.iter())
    {
        let flow_type = &process.flow_binding().flow_type;
        assert!(
            flow_type == FLOW_TYPE_DATASET_INGEST || flow_type == FLOW_TYPE_DATASET_TRANSFORM,
            "Process {:?} has flow type {}, but should be INGEST or EXECUTE_TRANSFORM",
            process.flow_binding(),
            flow_type
        );
    }

    // Verify ordering: next_planned_at should be in ascending order across both
    // pages
    let all_processes: Vec<_> = first_page
        .processes
        .iter()
        .chain(second_page.processes.iter())
        .collect();
    for i in 1..all_processes.len() {
        let prev_planned = all_processes[i - 1].next_planned_at();
        let curr_planned = all_processes[i].next_planned_at();

        // Both should have planned times
        assert!(prev_planned.is_some() && curr_planned.is_some());
        assert!(
            prev_planned.unwrap() <= curr_planned.unwrap(),
            "Process {} has planned time {:?}, but process {} has planned time {:?} (should be \
             ascending)",
            i - 1,
            prev_planned,
            i,
            curr_planned
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
