// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{Duration, Utc};
use futures::TryStreamExt;
use kamu_core::compact_service::CompactOptions;
use kamu_flow_system::*;
use kamu_flow_system_inmem::FlowEventStoreInMem;
use kamu_task_system::{TaskOutcome, TaskResult, TaskSystemEventStore};
use kamu_task_system_inmem::TaskSystemEventStoreInMemory;
use opendatafabric::{AccountName, DatasetID, FAKE_ACCOUNT_ID};

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_empty_filters_distingush_dataset() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let always_happy_filters = DatasetFlowFilters::default();

    let foo_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;
    let bar_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    assert_dataset_flow_expectaitons(
        flow_event_store.clone(),
        &foo_cases,
        always_happy_filters.clone(),
        FlowPaginationOpts {
            offset: 0,
            limit: 100,
        },
        6,
        vec![
            foo_cases.compacting_flow_ids.flow_id_finished,
            foo_cases.compacting_flow_ids.flow_id_running,
            foo_cases.compacting_flow_ids.flow_id_waiting,
            foo_cases.ingest_flow_ids.flow_id_finished,
            foo_cases.ingest_flow_ids.flow_id_running,
            foo_cases.ingest_flow_ids.flow_id_waiting,
        ],
    )
    .await;

    assert_dataset_flow_expectaitons(
        flow_event_store.clone(),
        &bar_cases,
        always_happy_filters.clone(),
        FlowPaginationOpts {
            offset: 0,
            limit: 100,
        },
        6,
        vec![
            bar_cases.compacting_flow_ids.flow_id_finished,
            bar_cases.compacting_flow_ids.flow_id_running,
            bar_cases.compacting_flow_ids.flow_id_waiting,
            bar_cases.ingest_flow_ids.flow_id_finished,
            bar_cases.ingest_flow_ids.flow_id_running,
            bar_cases.ingest_flow_ids.flow_id_waiting,
        ],
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_filter_by_status() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let foo_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                ..Default::default()
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                ..Default::default()
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_filter_by_flow_type() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let foo_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            DatasetFlowFilters {
                by_flow_type: Some(DatasetFlowType::Ingest),
                ..Default::default()
            },
            vec![
                foo_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            DatasetFlowFilters {
                by_flow_type: Some(DatasetFlowType::HardCompaction),
                ..Default::default()
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_finished,
                foo_cases.compacting_flow_ids.flow_id_running,
                foo_cases.compacting_flow_ids.flow_id_waiting,
            ],
        ),
        (
            DatasetFlowFilters {
                by_flow_type: Some(DatasetFlowType::ExecuteTransform),
                ..Default::default()
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_filter_by_initiator() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let foo_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(AccountName::new_unchecked(
                    "wasya",
                ))),
                ..Default::default()
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(AccountName::new_unchecked(
                    "petya",
                ))),
                ..Default::default()
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_filter_combinations() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let foo_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                by_flow_type: Some(DatasetFlowType::Ingest),
                by_initiator: Some(InitiatorFilter::System),
            },
            vec![foo_cases.ingest_flow_ids.flow_id_finished],
        ),
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                by_flow_type: Some(DatasetFlowType::HardCompaction),
                by_initiator: Some(InitiatorFilter::Account(AccountName::new_unchecked(
                    "petya",
                ))),
            },
            vec![foo_cases.compacting_flow_ids.flow_id_waiting],
        ),
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                by_flow_type: Some(DatasetFlowType::Ingest),
                by_initiator: Some(InitiatorFilter::System),
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_pagination() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let foo_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            FlowPaginationOpts {
                offset: 0,
                limit: 2,
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_finished,
                foo_cases.compacting_flow_ids.flow_id_running,
            ],
        ),
        (
            FlowPaginationOpts {
                offset: 2,
                limit: 3,
            },
            vec![
                foo_cases.compacting_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            FlowPaginationOpts {
                offset: 4,
                limit: 2,
            },
            vec![
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            FlowPaginationOpts {
                offset: 5,
                limit: 2,
            },
            vec![foo_cases.ingest_flow_ids.flow_id_waiting],
        ),
        (
            FlowPaginationOpts {
                offset: 6,
                limit: 5,
            },
            vec![],
        ),
    ];

    for (pagination, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            Default::default(),
            pagination,
            6,
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_pagination_with_filters() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let foo_cases =
        make_dataset_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            FlowPaginationOpts {
                offset: 0,
                limit: 2,
            },
            DatasetFlowFilters {
                by_flow_type: Some(DatasetFlowType::Ingest),
                ..Default::default()
            },
            3,
            vec![
                foo_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            FlowPaginationOpts {
                offset: 1,
                limit: 2,
            },
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            2,
            vec![foo_cases.ingest_flow_ids.flow_id_waiting],
        ),
        (
            FlowPaginationOpts {
                offset: 1,
                limit: 2,
            },
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            2,
            vec![foo_cases.ingest_flow_ids.flow_id_finished],
        ),
    ];

    for (pagination, filters, expected_total_count, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            pagination,
            expected_total_count,
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unfiltered_system_flows() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let system_case =
        make_system_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    assert_system_flow_expectaitons(
        flow_event_store.clone(),
        SystemFlowFilters::default(),
        FlowPaginationOpts {
            offset: 0,
            limit: 100,
        },
        3,
        vec![
            system_case.gc_flow_ids.flow_id_finished,
            system_case.gc_flow_ids.flow_id_running,
            system_case.gc_flow_ids.flow_id_waiting,
        ],
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_system_flows_filtered_by_flow_type() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let system_case =
        make_system_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![(
        SystemFlowFilters {
            by_flow_type: Some(SystemFlowType::GC),
            ..Default::default()
        },
        vec![
            system_case.gc_flow_ids.flow_id_finished,
            system_case.gc_flow_ids.flow_id_running,
            system_case.gc_flow_ids.flow_id_waiting,
        ],
    )];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectaitons(
            flow_event_store.clone(),
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_system_flows_filtered_by_flow_status() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let system_case =
        make_system_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            SystemFlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_waiting],
        ),
        (
            SystemFlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_running],
        ),
        (
            SystemFlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_finished],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectaitons(
            flow_event_store.clone(),
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_system_flows_filtered_by_initiator() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let system_case =
        make_system_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            SystemFlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_finished],
        ),
        (
            SystemFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(AccountName::new_unchecked(
                    "wasya",
                ))),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_running],
        ),
        (
            SystemFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(AccountName::new_unchecked(
                    "unrelated-user",
                ))),
                ..Default::default()
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectaitons(
            flow_event_store.clone(),
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_system_flows_complex_filter() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let system_case =
        make_system_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            SystemFlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                by_initiator: Some(InitiatorFilter::System),
                by_flow_type: Some(SystemFlowType::GC),
            },
            vec![system_case.gc_flow_ids.flow_id_finished],
        ),
        (
            SystemFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(AccountName::new_unchecked(
                    "petya",
                ))),
                by_flow_status: Some(FlowStatus::Waiting),
                by_flow_type: None,
            },
            vec![system_case.gc_flow_ids.flow_id_waiting],
        ),
        (
            SystemFlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                by_initiator: Some(InitiatorFilter::System),
                by_flow_type: Some(SystemFlowType::GC),
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectaitons(
            flow_event_store.clone(),
            filters,
            FlowPaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_system_flow_pagination() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let system_case =
        make_system_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            FlowPaginationOpts {
                offset: 0,
                limit: 2,
            },
            vec![
                system_case.gc_flow_ids.flow_id_finished,
                system_case.gc_flow_ids.flow_id_running,
            ],
        ),
        (
            FlowPaginationOpts {
                offset: 1,
                limit: 2,
            },
            vec![
                system_case.gc_flow_ids.flow_id_running,
                system_case.gc_flow_ids.flow_id_waiting,
            ],
        ),
        (
            FlowPaginationOpts {
                offset: 2,
                limit: 2,
            },
            vec![system_case.gc_flow_ids.flow_id_waiting],
        ),
        (
            FlowPaginationOpts {
                offset: 3,
                limit: 5,
            },
            vec![],
        ),
    ];

    for (pagination, expected_flow_ids) in cases {
        assert_system_flow_expectaitons(
            flow_event_store.clone(),
            Default::default(),
            pagination,
            3,
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_system_flow_pagination_with_filters() {
    let (flow_event_store, task_event_store) = make_event_stores();

    let system_case =
        make_system_test_case(flow_event_store.clone(), task_event_store.clone()).await;

    let cases = vec![
        (
            FlowPaginationOpts {
                offset: 0,
                limit: 2,
            },
            SystemFlowFilters {
                by_flow_type: Some(SystemFlowType::GC),
                ..Default::default()
            },
            3,
            vec![
                system_case.gc_flow_ids.flow_id_finished,
                system_case.gc_flow_ids.flow_id_running,
            ],
        ),
        (
            FlowPaginationOpts {
                offset: 0,
                limit: 2,
            },
            SystemFlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            1,
            vec![system_case.gc_flow_ids.flow_id_waiting],
        ),
        (
            FlowPaginationOpts {
                offset: 1,
                limit: 2,
            },
            SystemFlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            1,
            vec![],
        ),
    ];

    for (pagination, filters, expected_total_count, expected_flow_ids) in cases {
        assert_system_flow_expectaitons(
            flow_event_store.clone(),
            filters,
            pagination,
            expected_total_count,
            expected_flow_ids,
        )
        .await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

fn make_event_stores() -> (Arc<dyn FlowEventStore>, Arc<dyn TaskSystemEventStore>) {
    (
        Arc::new(FlowEventStoreInMem::new()),
        Arc::new(TaskSystemEventStoreInMemory::new()),
    )
}

/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetTestCase {
    dataset_id: DatasetID,
    ingest_flow_ids: TestFlowIDs,
    compacting_flow_ids: TestFlowIDs,
}

struct SystemTestCase {
    gc_flow_ids: TestFlowIDs,
}

struct TestFlowIDs {
    flow_id_waiting: FlowID,  // Initiator: petya
    flow_id_running: FlowID,  // Initiator: wasya
    flow_id_finished: FlowID, // Initiator: system
}

async fn make_dataset_test_case(
    flow_event_store: Arc<dyn FlowEventStore>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
) -> DatasetTestCase {
    let (_, dataset_id) = DatasetID::new_generated_ed25519();

    DatasetTestCase {
        dataset_id: dataset_id.clone(),
        ingest_flow_ids: make_dataset_test_flows(
            &dataset_id,
            DatasetFlowType::Ingest,
            flow_event_store.clone(),
            task_event_store.clone(),
        )
        .await,
        compacting_flow_ids: make_dataset_test_flows(
            &dataset_id,
            DatasetFlowType::HardCompaction,
            flow_event_store,
            task_event_store,
        )
        .await,
    }
}

async fn make_system_test_case(
    flow_event_store: Arc<dyn FlowEventStore>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
) -> SystemTestCase {
    SystemTestCase {
        gc_flow_ids: make_system_test_flows(SystemFlowType::GC, flow_event_store, task_event_store)
            .await,
    }
}

async fn make_dataset_test_flows(
    dataset_id: &DatasetID,
    dataset_flow_type: DatasetFlowType,
    flow_event_store: Arc<dyn FlowEventStore>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
) -> TestFlowIDs {
    let flow_generator =
        DatasetFlowGenerator::new(dataset_id, flow_event_store.clone(), task_event_store);

    let wasya_manual_trigger = FlowTrigger::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: FAKE_ACCOUNT_ID.to_string(),
        initiator_account_name: AccountName::new_unchecked("wasya"),
    });

    let petya_manual_trigger = FlowTrigger::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: FAKE_ACCOUNT_ID.to_string(),
        initiator_account_name: AccountName::new_unchecked("petya"),
    });

    let automatic_trigger = FlowTrigger::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id_waiting = flow_generator
        .make_new_flow(dataset_flow_type, FlowStatus::Waiting, petya_manual_trigger)
        .await;
    let flow_id_running = flow_generator
        .make_new_flow(dataset_flow_type, FlowStatus::Running, wasya_manual_trigger)
        .await;
    let flow_id_finished = flow_generator
        .make_new_flow(dataset_flow_type, FlowStatus::Finished, automatic_trigger)
        .await;

    TestFlowIDs {
        flow_id_waiting,
        flow_id_running,
        flow_id_finished,
    }
}

async fn make_system_test_flows(
    system_flow_type: SystemFlowType,
    flow_event_store: Arc<dyn FlowEventStore>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
) -> TestFlowIDs {
    let flow_generator = SystemFlowGenerator::new(flow_event_store.clone(), task_event_store);

    let wasya_manual_trigger = FlowTrigger::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: FAKE_ACCOUNT_ID.to_string(),
        initiator_account_name: AccountName::new_unchecked("wasya"),
    });

    let petya_manual_trigger = FlowTrigger::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: FAKE_ACCOUNT_ID.to_string(),
        initiator_account_name: AccountName::new_unchecked("petya"),
    });

    let automatic_trigger = FlowTrigger::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id_waiting = flow_generator
        .make_new_flow(system_flow_type, FlowStatus::Waiting, petya_manual_trigger)
        .await;
    let flow_id_running = flow_generator
        .make_new_flow(system_flow_type, FlowStatus::Running, wasya_manual_trigger)
        .await;
    let flow_id_finished = flow_generator
        .make_new_flow(system_flow_type, FlowStatus::Finished, automatic_trigger)
        .await;

    TestFlowIDs {
        flow_id_waiting,
        flow_id_running,
        flow_id_finished,
    }
}

async fn assert_dataset_flow_expectaitons(
    flow_event_store: Arc<dyn FlowEventStore>,
    dataset_test_case: &DatasetTestCase,
    filters: DatasetFlowFilters,
    pagination: FlowPaginationOpts,
    expected_total_count: usize,
    expected_flow_ids: Vec<FlowID>,
) {
    let total_flows_count = flow_event_store
        .get_count_flows_by_dataset(&dataset_test_case.dataset_id, &filters)
        .await
        .unwrap();
    assert_eq!(expected_total_count, total_flows_count);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids_by_dataset(&dataset_test_case.dataset_id, filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

async fn assert_system_flow_expectaitons(
    flow_event_store: Arc<dyn FlowEventStore>,
    filters: SystemFlowFilters,
    pagination: FlowPaginationOpts,
    expected_total_count: usize,
    expected_flow_ids: Vec<FlowID>,
) {
    let total_flows_count = flow_event_store
        .get_count_system_flows(&filters)
        .await
        .unwrap();
    assert_eq!(expected_total_count, total_flows_count);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_system_flow_ids(filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

/////////////////////////////////////////////////////////////////////////////////////////

struct DatasetFlowGenerator<'a> {
    dataset_id: &'a DatasetID,
    flow_event_store: Arc<dyn FlowEventStore>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
}

impl<'a> DatasetFlowGenerator<'a> {
    fn new(
        dataset_id: &'a DatasetID,
        flow_event_store: Arc<dyn FlowEventStore>,
        task_event_store: Arc<dyn TaskSystemEventStore>,
    ) -> Self {
        Self {
            dataset_id,
            flow_event_store,
            task_event_store,
        }
    }

    async fn make_new_flow(
        &self,
        flow_type: DatasetFlowType,
        expected_status: FlowStatus,
        initial_trigger: FlowTrigger,
    ) -> FlowID {
        let flow_id = self.flow_event_store.new_flow_id();

        let creation_moment = Utc::now();

        let mut flow = Flow::new(
            creation_moment,
            flow_id,
            FlowKeyDataset {
                dataset_id: self.dataset_id.clone(),
                flow_type,
                options: CompactOptions::default(),
            }
            .into(),
            initial_trigger,
        );

        drive_flow_to_status(&mut flow, self.task_event_store.as_ref(), expected_status).await;

        flow.save(self.flow_event_store.as_ref()).await.unwrap();

        flow_id
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct SystemFlowGenerator {
    flow_event_store: Arc<dyn FlowEventStore>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
}

impl SystemFlowGenerator {
    fn new(
        flow_event_store: Arc<dyn FlowEventStore>,
        task_event_store: Arc<dyn TaskSystemEventStore>,
    ) -> Self {
        Self {
            flow_event_store,
            task_event_store,
        }
    }

    async fn make_new_flow(
        &self,
        flow_type: SystemFlowType,
        expected_status: FlowStatus,
        initial_trigger: FlowTrigger,
    ) -> FlowID {
        let flow_id = self.flow_event_store.new_flow_id();

        let creation_moment = Utc::now();

        let mut flow = Flow::new(
            creation_moment,
            flow_id,
            FlowKey::System(FlowKeySystem { flow_type }),
            initial_trigger,
        );

        drive_flow_to_status(&mut flow, self.task_event_store.as_ref(), expected_status).await;

        flow.save(self.flow_event_store.as_ref()).await.unwrap();

        flow_id
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn drive_flow_to_status(
    flow: &mut Flow,
    task_event_store: &dyn TaskSystemEventStore,
    expected_status: FlowStatus,
) {
    let start_moment = Utc::now();

    flow.set_relevant_start_condition(
        start_moment + Duration::try_seconds(1).unwrap(),
        FlowStartCondition::Schedule(FlowStartConditionSchedule {
            wake_up_at: start_moment + Duration::try_minutes(1).unwrap(),
        }),
    )
    .unwrap();

    if expected_status != FlowStatus::Waiting {
        let task_id = task_event_store.new_task_id().await.unwrap();
        flow.on_task_scheduled(start_moment + Duration::try_minutes(5).unwrap(), task_id)
            .unwrap();
        flow.on_task_running(start_moment + Duration::try_minutes(7).unwrap(), task_id)
            .unwrap();

        if expected_status == FlowStatus::Finished {
            flow.on_task_finished(
                start_moment + Duration::try_minutes(10).unwrap(),
                task_id,
                TaskOutcome::Success(TaskResult::Empty),
            )
            .unwrap();
        } else if expected_status != FlowStatus::Running {
            panic!("Not expecting flow status {expected_status:?}");
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
