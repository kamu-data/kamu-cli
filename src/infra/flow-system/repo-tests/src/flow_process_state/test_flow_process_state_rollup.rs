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
        .rollup(FlowProcessListFilter::all())
        .await
        .unwrap();

    assert_eq!(rollup.total, 26);
    assert_eq!(rollup.active, 9);
    assert_eq!(rollup.failing, 9);
    assert_eq!(rollup.paused, 3);
    assert_eq!(rollup.stopped, 3);
    assert_eq!(rollup.unconfigured, 2);
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
        .rollup(FlowProcessListFilter::for_scope(single_dataset_query))
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
    assert_eq!(rollup.unconfigured, 0);

    // Test 2: Filter by multiple datasets
    let dataset_id2 = odf::DatasetID::new_seeded_ed25519(b"acme/users");
    let dataset_id3 = odf::DatasetID::new_seeded_ed25519(b"beta/catalog");
    let multi_dataset_query =
        FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id, &dataset_id2, &dataset_id3]);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::for_scope(multi_dataset_query))
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
    assert_eq!(rollup.unconfigured, 0);

    // Test 3: Filter by subscription scope for webhook processes of single dataset
    let subscription_query = FlowScopeSubscription::query_for_subscriptions_of_dataset(&dataset_id);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::for_scope(subscription_query))
        .await
        .unwrap();

    // Should find webhook subscription processes for acme/orders dataset (rows 5,
    // 6, 7 = 3 total)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 1); // row 5
    assert_eq!(rollup.failing, 1); // row 6
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 4: Filter by dataset scopes only (exclude webhook subscriptions)
    let dataset_only_query = FlowScopeDataset::query_for_single_dataset_only(&dataset_id);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::for_scope(dataset_only_query))
        .await
        .unwrap();

    // Should find only Dataset scope processes for acme/orders (row 1 = 1 total,
    // excludes webhook rows 5, 6, 7)
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 1); // row 1
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 5: Filter by webhook subscriptions regardless of dataset
    let all_webhooks_query = FlowScopeSubscription::query_for_all_subscriptions();

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::for_scope(all_webhooks_query))
        .await
        .unwrap();

    // Should find all webhook subscription processes (rows 5, 6, 7, 8, 9, 10, 15,
    // 16, 19, 20, 22, 24 = 12 total)
    assert_eq!(rollup.total, 12);
    assert_eq!(rollup.active, 4); // rows 5, 15, 20, 22
    assert_eq!(rollup.failing, 4); // rows 6, 8, 9, 19
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 3); // rows 10, 16, 24
    assert_eq!(rollup.unconfigured, 0);

    // Test 6: Filter by webhook subscriptions for multiple datasets
    let multi_webhook_query =
        FlowScopeSubscription::query_for_subscriptions_of_multiple_datasets(&[
            &dataset_id,
            &dataset_id2,
        ]);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::for_scope(multi_webhook_query))
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
    assert_eq!(rollup.unconfigured, 0);

    // Test 7: Filter by system flows (should return empty since our CSV has no
    // system flows)
    let system_query = FlowScopeQuery::build_for_system_scope();

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::for_scope(system_query))
        .await
        .unwrap();

    // Should find no system flows in our test data
    assert_eq!(rollup.total, 0);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);
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
        .rollup(FlowProcessListFilter::all().for_flow_types(&ingest_flows))
        .await
        .unwrap();

    // Should find 8 ingest processes from CSV data (rows 1, 3, 11, 13, 17, 21, 25,
    // 26)
    assert_eq!(rollup.total, 8);
    assert_eq!(rollup.active, 2); // rows 1, 11
    assert_eq!(rollup.failing, 3); // rows 13, 17, 21
    assert_eq!(rollup.paused, 1); // row 3
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 2); // rows 25, 26 (undefined -> unconfigured)

    // Test 2: Filter by TRANSFORM flow type only
    let transform_flows = [FLOW_TYPE_DATASET_TRANSFORM];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().for_flow_types(&transform_flows))
        .await
        .unwrap();

    // Should find 6 transform processes from CSV data (rows 2, 4, 12, 14, 18, 23)
    assert_eq!(rollup.total, 6);
    assert_eq!(rollup.active, 3); // rows 12, 14, 23
    assert_eq!(rollup.failing, 2); // rows 2, 4
    assert_eq!(rollup.paused, 1); // row 18
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 3: Filter by WEBHOOK_DELIVER flow type only
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().for_flow_types(&webhook_flows))
        .await
        .unwrap();

    // Should find 12 webhook processes from CSV data (rows 5, 6, 7, 8, 9, 10, 15,
    // 16, 19, 20, 22, 24)
    assert_eq!(rollup.total, 12);
    assert_eq!(rollup.active, 4); // rows 5, 15, 20, 22
    assert_eq!(rollup.failing, 4); // rows 6, 8, 9, 19
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 3); // rows 10, 16, 24
    assert_eq!(rollup.unconfigured, 0);

    // Test 4: Filter by multiple flow types
    let multiple_flows = [FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().for_flow_types(&multiple_flows))
        .await
        .unwrap();

    // Should find 14 processes (8 ingest + 6 transform)
    assert_eq!(rollup.total, 14);
    assert_eq!(rollup.active, 5); // 2 + 3
    assert_eq!(rollup.failing, 5); // 3 + 2
    assert_eq!(rollup.paused, 2); // 1 + 1
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 2); // 2 undefined ingest
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_effective_status(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test 1: Filter by Active status only
    let active_states = [FlowProcessEffectiveState::Active];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&active_states))
        .await
        .unwrap();

    // Should find only active processes (9 from realistic distribution)
    assert_eq!(rollup.total, 9);
    assert_eq!(rollup.active, 9);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 2: Filter by Failing status only
    let failing_states = [FlowProcessEffectiveState::Failing];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&failing_states))
        .await
        .unwrap();

    // Should find only failing processes (9 from realistic distribution)
    assert_eq!(rollup.total, 9);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 9);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 3: Filter by PausedManual status only
    let paused_states = [FlowProcessEffectiveState::PausedManual];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&paused_states))
        .await
        .unwrap();

    // Should find only manually paused processes (3 from realistic distribution)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 3);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 4: Filter by StoppedAuto status only
    let stopped_states = [FlowProcessEffectiveState::StoppedAuto];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&stopped_states))
        .await
        .unwrap();

    // Should find only auto-stopped processes (3 from realistic distribution)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 3);
    assert_eq!(rollup.unconfigured, 0);

    // Test 5: Filter by multiple statuses (problematic states)
    let problematic_states = [
        FlowProcessEffectiveState::Failing,
        FlowProcessEffectiveState::StoppedAuto,
    ];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&problematic_states))
        .await
        .unwrap();

    // Should find failing + stopped processes (9 + 3 = 12)
    assert_eq!(rollup.total, 12);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 9);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 3);
    assert_eq!(rollup.unconfigured, 0);
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
        .rollup(FlowProcessListFilter::for_scope(scope_query).for_flow_types(&webhook_flows))
        .await
        .unwrap();

    // Should find only webhook flows for acme/orders dataset
    // From CSV: rows 5, 6, 7 (all webhook flows for acme/orders)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 1); // row 5
    assert_eq!(rollup.failing, 1); // row 6
    assert_eq!(rollup.paused, 1); // row 7
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 2: Scope filter + Effective status filter
    // Filter by single dataset AND failing status only
    let scope_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);
    let failing_states = [FlowProcessEffectiveState::Failing];

    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::for_scope(scope_query).with_effective_states(&failing_states),
        )
        .await
        .unwrap();

    // Should find only failing processes for acme/orders dataset
    // From CSV: row 6 (webhook failing)
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 1); // row 6
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 3: Flow type filter + Effective status filter
    // Filter by webhook flows AND active status only
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];
    let active_states = [FlowProcessEffectiveState::Active];

    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::all()
                .for_flow_types(&webhook_flows)
                .with_effective_states(&active_states),
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
    assert_eq!(rollup.unconfigured, 0);

    // Test 4: All three filters combined
    // Filter by specific dataset AND webhook flows AND active status
    let scope_query = FlowScopeDataset::query_for_multiple_datasets(&[&dataset_id, &dataset_id2]);
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];
    let active_states = [FlowProcessEffectiveState::Active];

    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::for_scope(scope_query)
                .for_flow_types(&webhook_flows)
                .with_effective_states(&active_states),
        )
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
    assert_eq!(rollup.unconfigured, 0);

    // Test 5: Filters that should return empty results
    // Filter by single dataset AND ingest flows AND stopped status
    // (This combination shouldn't exist in our test data)
    let scope_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);
    let ingest_flows = [FLOW_TYPE_DATASET_INGEST];
    let stopped_states = [FlowProcessEffectiveState::StoppedAuto];

    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::for_scope(scope_query)
                .for_flow_types(&ingest_flows)
                .with_effective_states(&stopped_states),
        )
        .await
        .unwrap();

    // Should find no processes matching this combination
    assert_eq!(rollup.total, 0);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_unconfigured_edge_cases(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test 1: Filter by Unconfigured status only
    let unconfigured_states = [FlowProcessEffectiveState::Unconfigured];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&unconfigured_states))
        .await
        .unwrap();

    // Should find only unconfigured processes (2 from CSV: rows 19 and 26)
    assert_eq!(rollup.total, 2);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 2);

    // Test 2: Mixed filter - Active + Unconfigured (operational monitoring view)
    let operational_states = [
        FlowProcessEffectiveState::Active,
        FlowProcessEffectiveState::Unconfigured,
    ];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&operational_states))
        .await
        .unwrap();

    // Should find active + unconfigured processes (9 active + 2 unconfigured = 11)
    assert_eq!(rollup.total, 11);
    assert_eq!(rollup.active, 9);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 2);

    // Test 3: Unconfigured flows by type (ingest only)
    let ingest_flows = [FLOW_TYPE_DATASET_INGEST];
    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::all()
                .for_flow_types(&ingest_flows)
                .with_effective_states(&unconfigured_states),
        )
        .await
        .unwrap();

    // Should find only unconfigured ingest processes (2 from CSV: both rows 19 and
    // 26 are ingest)
    assert_eq!(rollup.total, 2);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 2);

    // Test 4: Unconfigured flows by type (webhook only)
    let webhook_flows = [FLOW_TYPE_WEBHOOK_DELIVER];
    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::all()
                .for_flow_types(&webhook_flows)
                .with_effective_states(&unconfigured_states),
        )
        .await
        .unwrap();

    // Should find no unconfigured webhook processes (all unconfigured are ingest)
    assert_eq!(rollup.total, 0);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 0);

    // Test 5: Complex multi-state filter excluding unconfigured (admin view)
    let managed_states = [
        FlowProcessEffectiveState::Active,
        FlowProcessEffectiveState::Failing,
        FlowProcessEffectiveState::PausedManual,
        FlowProcessEffectiveState::StoppedAuto,
    ];
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_effective_states(&managed_states))
        .await
        .unwrap();

    // Should find all managed processes (26 total - 2 unconfigured = 24)
    assert_eq!(rollup.total, 24);
    assert_eq!(rollup.active, 9);
    assert_eq!(rollup.failing, 9);
    assert_eq!(rollup.paused, 3);
    assert_eq!(rollup.stopped, 3);
    assert_eq!(rollup.unconfigured, 0);

    // Test 6: Dataset-specific unconfigured check
    let dataset_id = odf::DatasetID::new_seeded_ed25519("test/undefined-flow".as_bytes());
    let scope_query = FlowScopeDataset::query_for_single_dataset(&dataset_id);

    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::for_scope(scope_query)
                .with_effective_states(&unconfigured_states),
        )
        .await
        .unwrap();

    // Should find unconfigured processes for test/undefined-flow dataset (1 from
    // row 25)
    assert_eq!(rollup.total, 1);
    assert_eq!(rollup.active, 0);
    assert_eq!(rollup.failing, 0);
    assert_eq!(rollup.paused, 0);
    assert_eq!(rollup.stopped, 0);
    assert_eq!(rollup.unconfigured, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_last_attempt_between(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test with a time window that should match records #2 (07:40) and #11 (07:58)
    // Between 07:30 and 08:00
    let start = chrono::DateTime::parse_from_rfc3339("2025-09-08T07:30:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let end = chrono::DateTime::parse_from_rfc3339("2025-09-08T08:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_last_attempt_between(start, end))
        .await
        .unwrap();

    // Should match records with last_attempt_at in [07:30, 08:00):
    // #2 (07:40 failing), #4 (07:55 failing), #6 (07:41 failing),
    // #8 (07:42 failing), #11 (07:58 active), #13 (07:43 failing),
    // #16 (07:50 stopped), #19 (07:51 failing), #21 (07:52 failing),
    // #24 (07:52 stopped)
    assert_eq!(rollup.total, 10);
    assert_eq!(rollup.active, 1);
    assert_eq!(rollup.failing, 7);
    assert_eq!(rollup.stopped, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_last_failure_since(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by failures since 07:45
    let since = chrono::DateTime::parse_from_rfc3339("2025-09-08T07:45:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_last_failure_since(since))
        .await
        .unwrap();

    // Should match records with last_failure_at >= 07:45:
    // #4 (07:55), #16 (07:50), #19 (07:51), #21 (07:52), #24 (07:52)
    assert_eq!(rollup.total, 5);
    assert_eq!(rollup.failing, 3);
    assert_eq!(rollup.stopped, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_next_planned_before(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by next_planned_at before 10:00
    let before = chrono::DateTime::parse_from_rfc3339("2025-09-08T10:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_next_planned_before(before))
        .await
        .unwrap();

    // Should match records with next_planned_at < 10:00:
    // #1 (09:00), #11 (08:30), #17 (09:30)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 2);
    assert_eq!(rollup.failing, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_next_planned_after(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test filtering by next_planned_at after 11:30
    let after = chrono::DateTime::parse_from_rfc3339("2025-09-08T11:30:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_next_planned_after(after))
        .await
        .unwrap();

    // Should match records with next_planned_at > 11:30:
    // #12 (12:00), #19 (12:00), #21 (14:00), #23 (11:30 - excluded, not after)
    // Wait, #23 is exactly 11:30, so with "after" it should be excluded
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.active, 1);
    assert_eq!(rollup.failing, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_filtered_by_min_consecutive_failures(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test with min_consecutive_failures = 2
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_min_consecutive_failures(2))
        .await
        .unwrap();

    // Should match records with consecutive_failures >= 2:
    // #4 (2), #6 (2), #8 (2), #9 (2), #13 (2), #16 (5), #17 (2), #19 (3),
    // #21 (4), #24 (9)
    assert_eq!(rollup.total, 10);
    assert_eq!(rollup.failing, 8);
    assert_eq!(rollup.stopped, 2);

    // Test with higher threshold
    let rollup = flow_process_state_query
        .rollup(FlowProcessListFilter::all().with_min_consecutive_failures(5))
        .await
        .unwrap();

    // Should match: #16 (5), #24 (9)
    assert_eq!(rollup.total, 2);
    assert_eq!(rollup.stopped, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_combined_time_filters(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Combine time-based filters: last_failure_since and next_planned_before
    let since = chrono::DateTime::parse_from_rfc3339("2025-09-08T07:40:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let before = chrono::DateTime::parse_from_rfc3339("2025-09-08T11:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::all()
                .with_last_failure_since(since)
                .with_next_planned_before(before),
        )
        .await
        .unwrap();

    // Should match records with both conditions:
    // last_failure_at >= 07:40 AND next_planned_at < 11:00
    // #2 (07:40, 10:00), #4 (07:55, 10:15), #8 (07:42, 10:30)
    assert_eq!(rollup.total, 3);
    assert_eq!(rollup.failing, 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rollup_from_csv_combined_failures_and_time(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Combine min_consecutive_failures with time filter
    let since = chrono::DateTime::parse_from_rfc3339("2025-09-08T07:50:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    let rollup = flow_process_state_query
        .rollup(
            FlowProcessListFilter::all()
                .with_min_consecutive_failures(3)
                .with_last_failure_since(since),
        )
        .await
        .unwrap();

    // Should match records with consecutive_failures >= 3 AND last_failure_at >=
    // 07:50 #16 (5 failures, 07:50), #19 (3 failures, 07:51), #21 (4
    // failures, 07:52), #24 (9 failures, 07:52)
    assert_eq!(rollup.total, 4);
    assert_eq!(rollup.failing, 2);
    assert_eq!(rollup.stopped, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
