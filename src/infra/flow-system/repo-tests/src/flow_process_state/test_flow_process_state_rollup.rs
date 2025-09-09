// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_flow_system::*;

use super::csv_flow_process_state_loader::CsvFlowProcessStateLoader;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_unfiltered(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, None)
        .await
        .unwrap();

    // Verify realistic data distribution (9 active, 9 failing, 3 paused_manual, 3
    // stopped_auto)
    assert_eq!(rollup.total, 24);
    assert_eq!(rollup.active, 9);
    assert_eq!(rollup.failing, 9);
    assert_eq!(rollup.paused, 3);
    assert_eq!(rollup.stopped, 3);
    assert_eq!(rollup.worst_consecutive_failures, 10);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_scope(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test 1: Filter by single dataset
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"acme/orders");
    let single_dataset_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);

    let rollup = flow_process_state_query
        .rollup_by_scope(single_dataset_query, None, None)
        .await
        .unwrap();

    // Should find processes for acme/orders dataset (rows 1, 5, 6, 7 = 4 total)
    // Note: Both Dataset scopes and WebhookSubscription scopes match when they have
    // the same dataset_id Row 14 is "acme/orders.daily" which is a different
    // dataset
    assert_eq!(rollup.total, 4);
    assert_eq!(rollup.active, 2); // rows 1, 5 (both active)
    assert_eq!(rollup.failing, 1); // row 6 (failing)
    assert_eq!(rollup.paused, 1); // row 7 (paused_manual)
    assert_eq!(rollup.stopped, 0);

    // Test 2: Filter by multiple datasets
    let dataset_id2 = odf::DatasetID::new_seeded_ed25519(b"acme/users");
    let dataset_id3 = odf::DatasetID::new_seeded_ed25519(b"beta/catalog");
    let multi_dataset_query =
        FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id, &dataset_id2, &dataset_id3]);

    let rollup = flow_process_state_query
        .rollup_by_scope(multi_dataset_query, None, None)
        .await
        .unwrap();

    // acme/orders (4): rows 1, 5, 6, 7 = 2 active, 1 failing, 1 paused, 0 stopped
    // acme/users (3): rows 8, 13, 20 = 1 active, 2 failing, 0 paused, 0 stopped
    // beta/catalog (4): rows 9, 10, 17, 24 = 0 active, 2 failing, 0 paused, 2
    // stopped
    assert_eq!(rollup.total, 11); // 4 + 3 + 4
    assert_eq!(rollup.active, 3); // 2 + 1 + 0
    assert_eq!(rollup.failing, 5); // 1 + 2 + 2
    assert_eq!(rollup.paused, 1); // 1 + 0 + 0
    assert_eq!(rollup.stopped, 2); // 0 + 0 + 2

    // Test 3: Filter by subscription scope for webhook processes of single dataset
    let subscription_query = FlowScopeSubscription::query_for_subscriptions_of_dataset(&dataset_id);

    let rollup = flow_process_state_query
        .rollup_by_scope(subscription_query, None, None)
        .await
        .unwrap();

    // Should find webhook subscription processes for acme/orders dataset (rows 5,
    // 6, 7 = 3 total)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 1); // row 5
    assert_eq!(rollup.failing, 1); // row 6
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 0);

    // Test 4: Filter by dataset scopes only (exclude webhook subscriptions)
    let dataset_only_query = FlowScopeDataset::query_for_single_dataset_only(&dataset_id);

    let rollup = flow_process_state_query
        .rollup_by_scope(dataset_only_query, None, None)
        .await
        .unwrap();

    // Should find only Dataset scope processes for acme/orders (row 1 = 1 total,
    // excludes webhook rows 5, 6, 7)
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 1); // row 1
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);

    // Test 5: Filter by webhook subscriptions regardless of dataset
    let all_webhooks_query = FlowScopeSubscription::query_for_all_subscriptions();

    let rollup = flow_process_state_query
        .rollup_by_scope(all_webhooks_query, None, None)
        .await
        .unwrap();

    // Should find all webhook subscription processes (rows 5, 6, 7, 8, 9, 10, 15,
    // 16, 19, 20, 22, 24 = 12 total)
    assert_eq!(rollup.total, 12);
    assert_eq!(rollup.active, 4); // rows 5, 15, 20, 22
    assert_eq!(rollup.failing, 4); // rows 6, 8, 9, 19
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 3); // rows 10, 16, 24

    // Test 6: Filter by webhook subscriptions for multiple datasets
    let multi_webhook_query =
        FlowScopeSubscription::query_for_subscriptions_of_multiple_datasets(&[
            &dataset_id,
            &dataset_id2,
        ]);

    let rollup = flow_process_state_query
        .rollup_by_scope(multi_webhook_query, None, None)
        .await
        .unwrap();

    // Should find webhook subscriptions for acme/orders and acme/users
    // acme/orders: rows 5, 6, 7 = 1 active, 1 failing, 1 paused, 0 stopped
    // acme/users: rows 8, 20 = 1 active, 1 failing, 0 paused, 0 stopped
    assert_eq!(rollup.total, 5); // 3 + 2
    assert_eq!(rollup.active, 2); // 1 + 1 (rows 5, 20)
    assert_eq!(rollup.failing, 2); // 1 + 1 (rows 6, 8)
    assert_eq!(rollup.paused, 1); // 1 + 0 (row 7)
    assert_eq!(rollup.stopped, 0); // 0 + 0

    // Test 7: Filter by system flows (should return empty since our CSV has no
    // system flows)
    let system_query = FlowScopeQuery::build_for_system_scope();

    let rollup = flow_process_state_query
        .rollup_by_scope(system_query, None, None)
        .await
        .unwrap();

    // Should find no system flows in our test data
    assert_eq!(rollup.total, 0);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.worst_consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_flow_type(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test 1: Filter by INGEST flow type only
    let ingest_flows = [FLOW_TYPE_DATASET_INGEST];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), Some(&ingest_flows), None)
        .await
        .unwrap();

    // Should find 6 ingest processes from CSV data (rows 1, 3, 11, 13, 17, 21)
    assert_eq!(rollup.total, 6);
    assert_eq!(rollup.active, 2); // rows 1, 11
    assert_eq!(rollup.failing, 3); // rows 13, 17, 21
    assert_eq!(rollup.paused, 1); // row 3
    assert_eq!(rollup.stopped, 0);

    // Test 2: Filter by TRANSFORM flow type only
    let transform_flows = [FLOW_TYPE_DATASET_TRANSFORM];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), Some(&transform_flows), None)
        .await
        .unwrap();

    // Should find 6 transform processes from CSV data (rows 2, 4, 12, 14, 18, 23)
    assert_eq!(rollup.total, 6);
    assert_eq!(rollup.active, 3); // rows 12, 14, 23
    assert_eq!(rollup.failing, 2); // rows 2, 4
    assert_eq!(rollup.paused, 1); // row 18
    assert_eq!(rollup.stopped, 0);

    // Test 3: Filter by WEBHOOK_DELIVER flow type only
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), Some(&webhook_flows), None)
        .await
        .unwrap();

    // Should find 12 webhook processes from CSV data (rows 5, 6, 7, 8, 9, 10, 15,
    // 16, 19, 20, 22, 24)
    assert_eq!(rollup.total, 12);
    assert_eq!(rollup.active, 4); // rows 5, 15, 20, 22
    assert_eq!(rollup.failing, 4); // rows 6, 8, 9, 19
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 3); // rows 10, 16, 24

    // Test 4: Filter by multiple flow types
    let multiple_flows = [FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), Some(&multiple_flows), None)
        .await
        .unwrap();

    // Should find 12 processes (6 ingest + 6 transform)
    assert_eq!(rollup.total, 12);
    assert_eq!(rollup.active, 5); // 2 + 3
    assert_eq!(rollup.failing, 5); // 3 + 2
    assert_eq!(rollup.paused, 2); // 1 + 1
    assert_eq!(rollup.stopped, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_effective_status(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test 1: Filter by Active status only
    let active_states = [FlowProcessEffectiveState::Active];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, Some(&active_states))
        .await
        .unwrap();

    // Should find only active processes (9 from realistic distribution)
    assert_eq!(rollup.total, 9);
    assert_eq!(rollup.active, 9);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);

    // Test 2: Filter by Failing status only
    let failing_states = [FlowProcessEffectiveState::Failing];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, Some(&failing_states))
        .await
        .unwrap();

    // Should find only failing processes (9 from realistic distribution)
    assert_eq!(rollup.total, 9);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 9);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);

    // Test 3: Filter by PausedManual status only
    let paused_states = [FlowProcessEffectiveState::PausedManual];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, Some(&paused_states))
        .await
        .unwrap();

    // Should find only manually paused processes (3 from realistic distribution)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 3);
    assert_eq!(rollup.stopped, 0);

    // Test 4: Filter by StoppedAuto status only
    let stopped_states = [FlowProcessEffectiveState::StoppedAuto];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, Some(&stopped_states))
        .await
        .unwrap();

    // Should find only auto-stopped processes (3 from realistic distribution)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 3);

    // Test 5: Filter by multiple statuses (problematic states)
    let problematic_states = [
        FlowProcessEffectiveState::Failing,
        FlowProcessEffectiveState::StoppedAuto,
    ];
    let rollup = flow_process_state_query
        .rollup_by_scope(FlowScopeQuery::all(), None, Some(&problematic_states))
        .await
        .unwrap();

    // Should find failing + stopped processes (9 + 3 = 12)
    assert_eq!(rollup.total, 12);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 9);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_combined_filters(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test dataset IDs for specific filters - using same seeding as CSV loader
    let dataset_id = odf::DatasetID::new_seeded_ed25519("acme/orders".as_bytes()); // acme/orders
    let dataset_id2 = odf::DatasetID::new_seeded_ed25519("acme/users".as_bytes()); // acme/users

    // Test 1: Scope filter + Flow type filter
    // Filter by single dataset AND webhook flows only
    let scope_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];

    let rollup = flow_process_state_query
        .rollup_by_scope(scope_query, Some(&webhook_flows), None)
        .await
        .unwrap();

    // Should find only webhook flows for acme/orders dataset
    // From CSV: rows 5, 6, 7 (all webhook flows for acme/orders)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 1); // row 5
    assert_eq!(rollup.failing, 1); // row 6
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 0);

    // Test 2: Scope filter + Effective status filter
    // Filter by single dataset AND failing status only
    let scope_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);
    let failing_states = [FlowProcessEffectiveState::Failing];

    let rollup = flow_process_state_query
        .rollup_by_scope(scope_query, None, Some(&failing_states))
        .await
        .unwrap();

    // Should find only failing processes for acme/orders dataset
    // From CSV: row 6 (webhook failing)
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 1); // row 6
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);

    // Test 3: Flow type filter + Effective status filter
    // Filter by webhook flows AND active status only
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];
    let active_states = [FlowProcessEffectiveState::Active];

    let rollup = flow_process_state_query
        .rollup_by_scope(
            FlowScopeQuery::all(),
            Some(&webhook_flows),
            Some(&active_states),
        )
        .await
        .unwrap();

    // Should find only active webhook processes across all datasets
    // From CSV: rows 5, 15, 20, 22 (all active webhook flows)
    assert_eq!(rollup.total, 4);
    assert_eq!(rollup.active, 4); // rows 5, 15, 20, 22
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);

    // Test 4: All three filters combined
    // Filter by specific dataset AND webhook flows AND active status
    let scope_query = FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id, &dataset_id2]);
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];
    let active_states = [FlowProcessEffectiveState::Active];

    let rollup = flow_process_state_query
        .rollup_by_scope(scope_query, Some(&webhook_flows), Some(&active_states))
        .await
        .unwrap();

    // Should find only active webhook processes for acme/orders and acme/users
    // datasets From CSV: rows 5 (acme/orders), 20 (acme/users) - both active
    // webhook flows
    assert_eq!(rollup.total, 2);
    assert_eq!(rollup.active, 2); // rows 5, 20
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);

    // Test 5: Filters that should return empty results
    // Filter by single dataset AND ingest flows AND stopped status
    // (This combination shouldn't exist in our test data)
    let scope_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);
    let ingest_flows = [FLOW_TYPE_DATASET_INGEST];
    let stopped_states = [FlowProcessEffectiveState::StoppedAuto];

    let rollup = flow_process_state_query
        .rollup_by_scope(scope_query, Some(&ingest_flows), Some(&stopped_states))
        .await
        .unwrap();

    // Should find no processes matching this combination
    assert_eq!(rollup.total, 0);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
