// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::{Duration, SubsecRound, Utc};
use database_common::PaginationOpts;
use dill::Catalog;
use futures::TryStreamExt;
use kamu_flow_system::*;
use kamu_task_system::{TaskError, TaskID, TaskOutcome, TaskResult};
use opendatafabric::{AccountID, DatasetID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_empty_filters_distingush_dataset(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let always_happy_filters = DatasetFlowFilters::default();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let bar_cases = make_dataset_test_case(flow_event_store.clone()).await;

    assert_dataset_flow_expectaitons(
        flow_event_store.clone(),
        &foo_cases,
        always_happy_filters.clone(),
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        6,
        vec![
            foo_cases.compaction_flow_ids.flow_id_finished,
            foo_cases.compaction_flow_ids.flow_id_running,
            foo_cases.compaction_flow_ids.flow_id_waiting,
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
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        6,
        vec![
            bar_cases.compaction_flow_ids.flow_id_finished,
            bar_cases.compaction_flow_ids.flow_id_running,
            bar_cases.compaction_flow_ids.flow_id_waiting,
            bar_cases.ingest_flow_ids.flow_id_finished,
            bar_cases.ingest_flow_ids.flow_id_running,
            bar_cases.ingest_flow_ids.flow_id_waiting,
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_by_status(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_by_flow_type(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

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
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_waiting,
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
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_by_initiator(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let wasya_filter = HashSet::from_iter([AccountID::new_seeded_ed25519(b"wasya")]);
    let petya_filter = HashSet::from_iter([AccountID::new_seeded_ed25519(b"petya")]);

    let cases = vec![
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(wasya_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(petya_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_by_initiator_with_multiple_variants(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let wasya_patya_filter = HashSet::from_iter([
        AccountID::new_seeded_ed25519(b"wasya"),
        AccountID::new_seeded_ed25519(b"petya"),
    ]);
    let mut wasya_patya_unrelated_filter = wasya_patya_filter.clone();
    wasya_patya_unrelated_filter.insert(AccountID::new_seeded_ed25519(b"unrelated_user"));

    let cases = vec![
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(wasya_patya_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        // should return the same amount even if some non existing user was provided
        (
            DatasetFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(wasya_patya_unrelated_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectaitons(
            flow_event_store.clone(),
            &foo_cases,
            filters,
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_combinations(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let petya_filter = HashSet::from_iter([AccountID::new_seeded_ed25519(b"petya")]);

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
                by_initiator: Some(InitiatorFilter::Account(petya_filter)),
            },
            vec![foo_cases.compaction_flow_ids.flow_id_waiting],
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
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_by_datasets(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let bar_cases = make_dataset_test_case(flow_event_store.clone()).await;
    make_system_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            vec![foo_cases.dataset_id.clone()],
            vec![
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            vec![foo_cases.dataset_id.clone(), bar_cases.dataset_id.clone()],
            vec![
                bar_cases.compaction_flow_ids.flow_id_finished,
                bar_cases.compaction_flow_ids.flow_id_running,
                bar_cases.compaction_flow_ids.flow_id_waiting,
                bar_cases.ingest_flow_ids.flow_id_finished,
                bar_cases.ingest_flow_ids.flow_id_running,
                bar_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (vec![DatasetID::new_seeded_ed25519(b"wrong")], vec![]),
    ];

    for (dataset_ids, expected_flow_ids) in cases {
        assert_multiple_dataset_flow_expectations(
            flow_event_store.clone(),
            dataset_ids,
            DatasetFlowFilters::default(),
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_by_datasets_and_status(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let bar_cases = make_dataset_test_case(flow_event_store.clone()).await;
    make_system_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            vec![foo_cases.dataset_id.clone()],
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            vec![foo_cases.dataset_id.clone(), bar_cases.dataset_id.clone()],
            vec![
                bar_cases.compaction_flow_ids.flow_id_waiting,
                bar_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (vec![DatasetID::new_seeded_ed25519(b"wrong")], vec![]),
    ];

    for (dataset_ids, expected_flow_ids) in cases {
        assert_multiple_dataset_flow_expectations(
            flow_event_store.clone(),
            dataset_ids,
            DatasetFlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_filter_by_datasets_with_pagination(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let bar_cases = make_dataset_test_case(flow_event_store.clone()).await;
    make_system_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            vec![foo_cases.dataset_id.clone()],
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
            PaginationOpts {
                offset: 2,
                limit: 3,
            },
        ),
        (
            vec![foo_cases.dataset_id.clone(), bar_cases.dataset_id.clone()],
            vec![
                bar_cases.compaction_flow_ids.flow_id_running,
                bar_cases.compaction_flow_ids.flow_id_waiting,
                bar_cases.ingest_flow_ids.flow_id_finished,
                bar_cases.ingest_flow_ids.flow_id_running,
                bar_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.compaction_flow_ids.flow_id_running,
            ],
            PaginationOpts {
                offset: 1,
                limit: 7,
            },
        ),
        (
            vec![DatasetID::new_seeded_ed25519(b"wrong")],
            vec![],
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
        ),
    ];

    for (dataset_ids, expected_flow_ids, pagination) in cases {
        assert_multiple_dataset_flow_expectations(
            flow_event_store.clone(),
            dataset_ids,
            DatasetFlowFilters::default(),
            pagination,
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_pagination(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            PaginationOpts {
                offset: 0,
                limit: 2,
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.compaction_flow_ids.flow_id_running,
            ],
        ),
        (
            PaginationOpts {
                offset: 2,
                limit: 3,
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            PaginationOpts {
                offset: 4,
                limit: 2,
            },
            vec![
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            PaginationOpts {
                offset: 5,
                limit: 2,
            },
            vec![foo_cases.ingest_flow_ids.flow_id_waiting],
        ),
        (
            PaginationOpts {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_pagination_with_filters(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            PaginationOpts {
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
            PaginationOpts {
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
            PaginationOpts {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_get_flow_initiators(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let res: HashSet<_> = flow_event_store
        .get_unique_flow_initiator_ids_by_dataset(&foo_cases.dataset_id)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        res,
        HashSet::from([
            AccountID::new_seeded_ed25519(b"petya"),
            AccountID::new_seeded_ed25519(b"wasya"),
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_unfiltered_system_flows(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    assert_system_flow_expectaitons(
        flow_event_store.clone(),
        SystemFlowFilters::default(),
        PaginationOpts {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flows_filtered_by_flow_type(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

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
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flows_filtered_by_flow_status(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

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
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flows_filtered_by_initiator(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    let wasya_filter = HashSet::from_iter([AccountID::new_seeded_ed25519(b"wasya")]);
    let unrelated_user_filter =
        HashSet::from_iter([AccountID::new_seeded_ed25519(b"unrelated-user")]);

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
                by_initiator: Some(InitiatorFilter::Account(wasya_filter)),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_running],
        ),
        (
            SystemFlowFilters {
                by_initiator: Some(InitiatorFilter::Account(unrelated_user_filter)),
                ..Default::default()
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectaitons(
            flow_event_store.clone(),
            filters,
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flows_complex_filter(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;
    let petya_filter = HashSet::from_iter([AccountID::new_seeded_ed25519(b"petya")]);

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
                by_initiator: Some(InitiatorFilter::Account(petya_filter)),
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
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
            expected_flow_ids.len(),
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flow_pagination(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            PaginationOpts {
                offset: 0,
                limit: 2,
            },
            vec![
                system_case.gc_flow_ids.flow_id_finished,
                system_case.gc_flow_ids.flow_id_running,
            ],
        ),
        (
            PaginationOpts {
                offset: 1,
                limit: 2,
            },
            vec![
                system_case.gc_flow_ids.flow_id_running,
                system_case.gc_flow_ids.flow_id_waiting,
            ],
        ),
        (
            PaginationOpts {
                offset: 2,
                limit: 2,
            },
            vec![system_case.gc_flow_ids.flow_id_waiting],
        ),
        (
            PaginationOpts {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flow_pagination_with_filters(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            PaginationOpts {
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
            PaginationOpts {
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
            PaginationOpts {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_all_flows_unpaged(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    assert_all_flow_expectaitons(
        flow_event_store.clone(),
        AllFlowFilters::default(),
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        9,
        vec![
            system_case.gc_flow_ids.flow_id_finished,
            system_case.gc_flow_ids.flow_id_running,
            system_case.gc_flow_ids.flow_id_waiting,
            foo_cases.compaction_flow_ids.flow_id_finished,
            foo_cases.compaction_flow_ids.flow_id_running,
            foo_cases.compaction_flow_ids.flow_id_waiting,
            foo_cases.ingest_flow_ids.flow_id_finished,
            foo_cases.ingest_flow_ids.flow_id_running,
            foo_cases.ingest_flow_ids.flow_id_waiting,
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_all_flows_pagination(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let system_case = make_system_test_case(flow_event_store.clone()).await;

    let cases = vec![
        (
            PaginationOpts {
                offset: 0,
                limit: 2,
            },
            vec![
                system_case.gc_flow_ids.flow_id_finished,
                system_case.gc_flow_ids.flow_id_running,
            ],
        ),
        (
            PaginationOpts {
                offset: 2,
                limit: 2,
            },
            vec![
                system_case.gc_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_finished,
            ],
        ),
        (
            PaginationOpts {
                offset: 7,
                limit: 2,
            },
            vec![
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            PaginationOpts {
                offset: 8,
                limit: 2,
            },
            vec![foo_cases.ingest_flow_ids.flow_id_waiting],
        ),
        (
            PaginationOpts {
                offset: 9,
                limit: 1,
            },
            vec![],
        ),
    ];

    for (pagination, expected_flow_ids) in cases {
        assert_all_flow_expectaitons(
            flow_event_store.clone(),
            AllFlowFilters::default(),
            pagination,
            9,
            expected_flow_ids,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_all_flows_filters(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    assert_all_flow_expectaitons(
        flow_event_store.clone(),
        AllFlowFilters {
            by_flow_status: Some(FlowStatus::Waiting),
            by_initiator: None,
        },
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        3,
        vec![
            system_case.gc_flow_ids.flow_id_waiting,
            foo_cases.compaction_flow_ids.flow_id_waiting,
            foo_cases.ingest_flow_ids.flow_id_waiting,
        ],
    )
    .await;

    assert_all_flow_expectaitons(
        flow_event_store.clone(),
        AllFlowFilters {
            by_flow_status: Some(FlowStatus::Running),
            by_initiator: None,
        },
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        3,
        vec![
            system_case.gc_flow_ids.flow_id_running,
            foo_cases.compaction_flow_ids.flow_id_running,
            foo_cases.ingest_flow_ids.flow_id_running,
        ],
    )
    .await;

    assert_all_flow_expectaitons(
        flow_event_store.clone(),
        AllFlowFilters {
            by_flow_status: Some(FlowStatus::Finished),
            by_initiator: None,
        },
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        3,
        vec![
            system_case.gc_flow_ids.flow_id_finished,
            foo_cases.compaction_flow_ids.flow_id_finished,
            foo_cases.ingest_flow_ids.flow_id_finished,
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const EMPTY_STATS: FlowRunStats = FlowRunStats {
    last_attempt_time: None,
    last_success_time: None,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_run_stats(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let (_, dataset_id) = DatasetID::new_generated_ed25519();

    // No stats initially
    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::Ingest,
        }))
        .await
        .unwrap();
    assert_eq!(stats, EMPTY_STATS);

    // Schedule flow

    let flow_generator = DatasetFlowGenerator::new(&dataset_id, flow_event_store.clone());
    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id = flow_generator
        .make_new_flow(
            DatasetFlowType::Ingest,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    // Stats hasn't changed

    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::Ingest,
        }))
        .await
        .unwrap();
    assert_eq!(stats, EMPTY_STATS);

    // Flow starts running
    flow_generator.start_running_flow(flow_id).await;

    // still no change
    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::Ingest,
        }))
        .await
        .unwrap();
    assert_eq!(stats, EMPTY_STATS);

    // Flow successeds
    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Success(TaskResult::Empty))
        .await;

    // Finally, stats got updated
    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::Ingest,
        }))
        .await
        .unwrap();
    assert_matches!(
        stats,
        FlowRunStats {
            last_success_time: Some(success_time),
            last_attempt_time: Some(attempt_time)
        } if success_time == attempt_time
    );

    // Make another flow of the same type with the same dataset

    let flow_id = flow_generator
        .make_new_flow(
            DatasetFlowType::Ingest,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    // Still storing old stats
    let new_stats = flow_event_store
        .get_flow_run_stats(&FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::Ingest,
        }))
        .await
        .unwrap();
    assert_eq!(new_stats, stats);

    // Flow starts running
    flow_generator.start_running_flow(flow_id).await;

    // Still storing old stats
    let new_stats = flow_event_store
        .get_flow_run_stats(&FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::Ingest,
        }))
        .await
        .unwrap();
    assert_eq!(new_stats, stats);

    // Now finish the flow with failure
    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Failed(TaskError::Empty))
        .await;

    // Stats got updated: success stayed as previously, attempt refreshed
    let new_stats = flow_event_store
        .get_flow_run_stats(&FlowKey::Dataset(FlowKeyDataset {
            dataset_id: dataset_id.clone(),
            flow_type: DatasetFlowType::Ingest,
        }))
        .await
        .unwrap();
    assert_matches!(
        new_stats,
        FlowRunStats {
            last_success_time: Some(success_time),
            last_attempt_time: Some(attempt_time)
        } if success_time < attempt_time && success_time == stats.last_attempt_time.unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flow_run_stats(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    // No stats initially
    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::System(FlowKeySystem {
            flow_type: SystemFlowType::GC,
        }))
        .await
        .unwrap();
    assert_eq!(stats, EMPTY_STATS);

    // Schedule flow

    let flow_generator = SystemFlowGenerator::new(flow_event_store.clone());
    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id = flow_generator
        .make_new_flow(
            SystemFlowType::GC,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    // Stats hasn't changed

    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::System(FlowKeySystem {
            flow_type: SystemFlowType::GC,
        }))
        .await
        .unwrap();
    assert_eq!(stats, EMPTY_STATS);

    // Flow starts running
    flow_generator.start_running_flow(flow_id).await;

    // still no change
    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::System(FlowKeySystem {
            flow_type: SystemFlowType::GC,
        }))
        .await
        .unwrap();
    assert_eq!(stats, EMPTY_STATS);

    // Flow successeds
    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Success(TaskResult::Empty))
        .await;

    // Finally, stats got updated
    let stats = flow_event_store
        .get_flow_run_stats(&FlowKey::System(FlowKeySystem {
            flow_type: SystemFlowType::GC,
        }))
        .await
        .unwrap();
    assert_matches!(
        stats,
        FlowRunStats {
            last_success_time: Some(success_time),
            last_attempt_time: Some(attempt_time)
        } if success_time == attempt_time
    );

    // Make another flow of the same type

    let flow_id = flow_generator
        .make_new_flow(
            SystemFlowType::GC,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    // Still storing old stats
    let new_stats = flow_event_store
        .get_flow_run_stats(&FlowKey::System(FlowKeySystem {
            flow_type: SystemFlowType::GC,
        }))
        .await
        .unwrap();
    assert_eq!(new_stats, stats);

    // Flow starts running
    flow_generator.start_running_flow(flow_id).await;

    // Still storing old stats
    let new_stats = flow_event_store
        .get_flow_run_stats(&FlowKey::System(FlowKeySystem {
            flow_type: SystemFlowType::GC,
        }))
        .await
        .unwrap();
    assert_eq!(new_stats, stats);

    // Now finish the flow with failure
    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Failed(TaskError::Empty))
        .await;

    // Stats got updated: success stayed as previously, attempt refreshed
    let new_stats = flow_event_store
        .get_flow_run_stats(&FlowKey::System(FlowKeySystem {
            flow_type: SystemFlowType::GC,
        }))
        .await
        .unwrap();
    assert_matches!(
        new_stats,
        FlowRunStats {
            last_success_time: Some(success_time),
            last_attempt_time: Some(attempt_time)
        } if success_time < attempt_time && success_time == stats.last_attempt_time.unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pending_flow_dataset_single_type_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let (_, dataset_id) = DatasetID::new_generated_ed25519();

    let flow_key = FlowKey::dataset(dataset_id.clone(), DatasetFlowType::Ingest);

    // No pending yet
    let res = flow_event_store
        .try_get_pending_flow(&flow_key)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flow
    let flow_generator = DatasetFlowGenerator::new(&dataset_id, flow_event_store.clone());
    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id = flow_generator
        .make_new_flow(
            DatasetFlowType::Ingest,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    // Got pending
    let res = flow_event_store
        .try_get_pending_flow(&flow_key)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    // Flow starts running
    flow_generator.start_running_flow(flow_id).await;

    // Got pending
    let res = flow_event_store
        .try_get_pending_flow(&flow_key)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Success(TaskResult::Empty))
        .await;

    // No more pending
    let res = flow_event_store
        .try_get_pending_flow(&flow_key)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pending_flow_dataset_multiple_types_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let (_, dataset_id) = DatasetID::new_generated_ed25519();

    let flow_key_ingest = FlowKey::dataset(dataset_id.clone(), DatasetFlowType::Ingest);
    let flow_key_compact = FlowKey::dataset(dataset_id.clone(), DatasetFlowType::HardCompaction);

    // No pending yet
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_compact)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flows
    let flow_generator = DatasetFlowGenerator::new(&dataset_id, flow_event_store.clone());
    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id_ingest = flow_generator
        .make_new_flow(
            DatasetFlowType::Ingest,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;
    let flow_id_compact = flow_generator
        .make_new_flow(
            DatasetFlowType::HardCompaction,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_key_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_compact)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_compact));

    // Flows start running
    flow_generator.start_running_flow(flow_id_ingest).await;
    flow_generator.start_running_flow(flow_id_compact).await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_key_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_compact)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_compact));

    // Ingest finishes with success
    flow_generator
        .finish_running_flow(flow_id_ingest, TaskOutcome::Success(TaskResult::Empty))
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_key_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_compact)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_compact));

    // Compact finishes with failure
    flow_generator
        .finish_running_flow(flow_id_compact, TaskOutcome::Failed(TaskError::Empty))
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_key_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_compact)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pending_flow_multiple_datasets_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let dataset_foo_id = DatasetID::new_seeded_ed25519(b"foo");
    let dataset_bar_id = DatasetID::new_seeded_ed25519(b"bar");

    let flow_key_foo_ingest = FlowKey::dataset(dataset_foo_id.clone(), DatasetFlowType::Ingest);
    let flow_key_bar_ingest = FlowKey::dataset(dataset_bar_id.clone(), DatasetFlowType::Ingest);
    let flow_key_foo_compact =
        FlowKey::dataset(dataset_foo_id.clone(), DatasetFlowType::HardCompaction);

    // No pending yet
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_foo_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_bar_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_foo_compact)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flows
    let foo_flow_generator = DatasetFlowGenerator::new(&dataset_foo_id, flow_event_store.clone());
    let bar_flow_generator = DatasetFlowGenerator::new(&dataset_bar_id, flow_event_store.clone());
    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id_foo_ingest = foo_flow_generator
        .make_new_flow(
            DatasetFlowType::Ingest,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;
    let flow_id_bar_ingest = bar_flow_generator
        .make_new_flow(
            DatasetFlowType::Ingest,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;
    let flow_id_foo_compacting = foo_flow_generator
        .make_new_flow(
            DatasetFlowType::HardCompaction,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_key_foo_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_foo_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_bar_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_bar_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_foo_compact)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_foo_compacting));

    // Foo flows run & finish
    foo_flow_generator
        .start_running_flow(flow_id_foo_ingest)
        .await;
    foo_flow_generator
        .start_running_flow(flow_id_foo_compacting)
        .await;
    foo_flow_generator
        .finish_running_flow(flow_id_foo_ingest, TaskOutcome::Success(TaskResult::Empty))
        .await;
    foo_flow_generator
        .finish_running_flow(
            flow_id_foo_compacting,
            TaskOutcome::Success(TaskResult::Empty),
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_key_foo_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_bar_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_bar_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&flow_key_foo_compact)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pending_flow_system_flow_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_flow_key = FlowKey::system(SystemFlowType::GC);

    // No pending yet

    let res = flow_event_store
        .try_get_pending_flow(&system_flow_key)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flow

    let flow_generator = SystemFlowGenerator::new(flow_event_store.clone());
    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id = flow_generator
        .make_new_flow(
            SystemFlowType::GC,
            FlowStatus::Waiting,
            automatic_trigger.clone(),
            None,
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&system_flow_key)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    // Run flow
    flow_generator.start_running_flow(flow_id).await;

    let res = flow_event_store
        .try_get_pending_flow(&system_flow_key)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    // Finish flow
    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Success(TaskResult::Empty))
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&system_flow_key)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_concurrent_modification(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let flow_id = event_store.new_flow_id().await.unwrap();
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");
    let flow_key = FlowKey::dataset(dataset_id.clone(), DatasetFlowType::Ingest);

    // Nothing stored yet, but prev stored event id sent => CM
    let res = event_store
        .save_events(
            &flow_id,
            Some(EventID::new(15)),
            vec![FlowEventInitiated {
                event_time: Utc::now(),
                flow_key: flow_key.clone(),
                flow_id,
                trigger: FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
                    trigger_time: Utc::now(),
                }),
                config_snapshot: None,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Nothing stored yet, no storage expectation => OK
    let res = event_store
        .save_events(
            &flow_id,
            None,
            vec![FlowEventInitiated {
                event_time: Utc::now(),
                flow_key,
                flow_id,
                trigger: FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
                    trigger_time: Utc::now(),
                }),
                config_snapshot: None,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Ok(_));

    // Something stored, but no expectation => CM
    let res = event_store
        .save_events(
            &flow_id,
            None,
            vec![FlowEventAborted {
                event_time: Utc::now(),
                flow_id,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, but expectation is wrong => CM
    let res = event_store
        .save_events(
            &flow_id,
            Some(EventID::new(15)),
            vec![FlowEventAborted {
                event_time: Utc::now(),
                flow_id,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, and expectation is correct
    let res = event_store
        .save_events(
            &flow_id,
            Some(EventID::new(1)),
            vec![FlowEventAborted {
                event_time: Utc::now(),
                flow_id,
            }
            .into()],
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_flow_activation_visibility_at_different_stages_through_success_path(
    catalog: &Catalog,
) {
    let event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let flow_id = event_store.new_flow_id().await.unwrap();
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");
    let flow_key = FlowKey::dataset(dataset_id, DatasetFlowType::Ingest);

    let start_moment = Utc::now().trunc_subsecs(6);
    let activation_moment = start_moment + Duration::minutes(1);

    let last_event_id = event_store
        .save_events(
            &flow_id,
            None,
            vec![FlowEventInitiated {
                event_time: start_moment,
                flow_key,
                flow_id,
                trigger: FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
                    trigger_time: start_moment,
                }),
                config_snapshot: None,
            }
            .into()],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert!(maybe_nearest_activation_time.is_none());

    let last_event_id = event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![FlowEventStartConditionUpdated {
                flow_id,
                event_time: Utc::now(),
                start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                    wake_up_at: activation_moment,
                }),
                last_trigger_index: 0,
            }
            .into()],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert!(maybe_nearest_activation_time.is_none());
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment)
            .await
            .unwrap(),
        vec![]
    );

    let last_event_id = event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![FlowEventScheduledForActivation {
                flow_id,
                event_time: Utc::now(),
                scheduled_for_activation_at: activation_moment,
            }
            .into()],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert_eq!(maybe_nearest_activation_time, Some(activation_moment));
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment)
            .await
            .unwrap(),
        vec![flow_id]
    );

    let last_event_id = event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![FlowEventTaskScheduled {
                flow_id,
                event_time: activation_moment + Duration::milliseconds(100),
                task_id: TaskID::new(1),
            }
            .into()],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert!(maybe_nearest_activation_time.is_none());
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment)
            .await
            .unwrap(),
        vec![]
    );

    let last_event_id = event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![FlowEventTaskRunning {
                flow_id,
                event_time: activation_moment + Duration::milliseconds(500),
                task_id: TaskID::new(1),
            }
            .into()],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert!(maybe_nearest_activation_time.is_none());
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment)
            .await
            .unwrap(),
        vec![]
    );

    event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![FlowEventTaskFinished {
                flow_id,
                event_time: activation_moment + Duration::milliseconds(1500),
                task_id: TaskID::new(1),
                task_outcome: TaskOutcome::Success(TaskResult::Empty),
            }
            .into()],
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_flow_activation_visibility_when_aborted_before_activation(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let flow_id = event_store.new_flow_id().await.unwrap();
    let dataset_id = DatasetID::new_seeded_ed25519(b"foo");
    let flow_key = FlowKey::dataset(dataset_id, DatasetFlowType::Ingest);

    let start_moment = Utc::now().trunc_subsecs(6);
    let activation_moment = start_moment + Duration::minutes(1);
    let abortion_moment = start_moment + Duration::seconds(30);

    let last_event_id = event_store
        .save_events(
            &flow_id,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_moment,
                    flow_key,
                    flow_id,
                    trigger: FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
                        trigger_time: start_moment,
                    }),
                    config_snapshot: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id,
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment,
                    }),
                    last_trigger_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id,
                    event_time: Utc::now(),
                    scheduled_for_activation_at: activation_moment,
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert_eq!(maybe_nearest_activation_time, Some(activation_moment));
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment)
            .await
            .unwrap(),
        vec![flow_id]
    );

    event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![FlowEventAborted {
                event_time: abortion_moment,
                flow_id,
            }
            .into()],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert!(maybe_nearest_activation_time.is_none());
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment)
            .await
            .unwrap(),
        vec![]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_flow_activation_multiple_flows(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let dataset_id_foo = DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_bar = DatasetID::new_seeded_ed25519(b"bar");
    let dataset_id_baz = DatasetID::new_seeded_ed25519(b"baz");

    let flow_id_foo = event_store.new_flow_id().await.unwrap();
    let flow_id_bar = event_store.new_flow_id().await.unwrap();
    let flow_id_baz = event_store.new_flow_id().await.unwrap();

    let flow_key_foo = FlowKey::dataset(dataset_id_foo, DatasetFlowType::Ingest);
    let flow_key_bar = FlowKey::dataset(dataset_id_bar, DatasetFlowType::Ingest);
    let flow_key_baz = FlowKey::dataset(dataset_id_baz, DatasetFlowType::Ingest);

    let start_moment = Utc::now().trunc_subsecs(6);
    let activation_moment_1 = start_moment + Duration::minutes(1);
    let activation_moment_2 = start_moment + Duration::minutes(2);

    event_store
        .save_events(
            &flow_id_foo,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_moment,
                    flow_key: flow_key_foo,
                    flow_id: flow_id_foo,
                    trigger: FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
                        trigger_time: start_moment,
                    }),
                    config_snapshot: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id: flow_id_foo,
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment_1,
                    }),
                    last_trigger_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id: flow_id_foo,
                    event_time: Utc::now(),
                    scheduled_for_activation_at: activation_moment_1,
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    event_store
        .save_events(
            &flow_id_bar,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_moment,
                    flow_key: flow_key_bar,
                    flow_id: flow_id_bar,
                    trigger: FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
                        trigger_time: start_moment,
                    }),
                    config_snapshot: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id: flow_id_bar,
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment_1,
                    }),
                    last_trigger_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id: flow_id_bar,
                    event_time: Utc::now(),
                    scheduled_for_activation_at: activation_moment_1,
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    event_store
        .save_events(
            &flow_id_baz,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_moment,
                    flow_key: flow_key_baz,
                    flow_id: flow_id_baz,
                    trigger: FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
                        trigger_time: start_moment,
                    }),
                    config_snapshot: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id: flow_id_baz,
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment_2,
                    }),
                    last_trigger_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id: flow_id_baz,
                    event_time: Utc::now(),
                    scheduled_for_activation_at: activation_moment_2,
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert_eq!(maybe_nearest_activation_time, Some(activation_moment_1));
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment_1)
            .await
            .unwrap(),
        vec![flow_id_foo, flow_id_bar]
    );
    assert_eq!(
        event_store
            .get_flows_scheduled_for_activation_at(activation_moment_2)
            .await
            .unwrap(),
        vec![flow_id_baz]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetTestCase {
    dataset_id: DatasetID,
    ingest_flow_ids: TestFlowIDs,
    compaction_flow_ids: TestFlowIDs,
}

struct SystemTestCase {
    gc_flow_ids: TestFlowIDs,
}

struct TestFlowIDs {
    flow_id_waiting: FlowID,  // Initiator: petya
    flow_id_running: FlowID,  // Initiator: wasya
    flow_id_finished: FlowID, // Initiator: system
}

async fn make_dataset_test_case(flow_event_store: Arc<dyn FlowEventStore>) -> DatasetTestCase {
    let (_, dataset_id) = DatasetID::new_generated_ed25519();

    DatasetTestCase {
        dataset_id: dataset_id.clone(),
        ingest_flow_ids: make_dataset_test_flows(
            &dataset_id,
            DatasetFlowType::Ingest,
            flow_event_store.clone(),
        )
        .await,
        compaction_flow_ids: make_dataset_test_flows(
            &dataset_id,
            DatasetFlowType::HardCompaction,
            flow_event_store,
        )
        .await,
    }
}

async fn make_system_test_case(flow_event_store: Arc<dyn FlowEventStore>) -> SystemTestCase {
    SystemTestCase {
        gc_flow_ids: make_system_test_flows(SystemFlowType::GC, flow_event_store).await,
    }
}

async fn make_dataset_test_flows(
    dataset_id: &DatasetID,
    dataset_flow_type: DatasetFlowType,
    flow_event_store: Arc<dyn FlowEventStore>,
) -> TestFlowIDs {
    let flow_generator = DatasetFlowGenerator::new(dataset_id, flow_event_store.clone());

    let wasya_manual_trigger = FlowTriggerType::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: AccountID::new_seeded_ed25519(b"wasya"),
    });

    let petya_manual_trigger = FlowTriggerType::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: AccountID::new_seeded_ed25519(b"petya"),
    });

    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id_waiting = flow_generator
        .make_new_flow(
            dataset_flow_type,
            FlowStatus::Waiting,
            petya_manual_trigger,
            None,
        )
        .await;
    let flow_id_running = flow_generator
        .make_new_flow(
            dataset_flow_type,
            FlowStatus::Running,
            wasya_manual_trigger,
            None,
        )
        .await;
    let flow_id_finished = flow_generator
        .make_new_flow(
            dataset_flow_type,
            FlowStatus::Finished,
            automatic_trigger,
            None,
        )
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
) -> TestFlowIDs {
    let flow_generator = SystemFlowGenerator::new(flow_event_store.clone());

    let wasya_manual_trigger = FlowTriggerType::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: AccountID::new_seeded_ed25519(b"wasya"),
    });

    let petya_manual_trigger = FlowTriggerType::Manual(FlowTriggerManual {
        trigger_time: Utc::now(),
        initiator_account_id: AccountID::new_seeded_ed25519(b"petya"),
    });

    let automatic_trigger = FlowTriggerType::AutoPolling(FlowTriggerAutoPolling {
        trigger_time: Utc::now(),
    });

    let flow_id_waiting = flow_generator
        .make_new_flow(
            system_flow_type,
            FlowStatus::Waiting,
            petya_manual_trigger,
            None,
        )
        .await;
    let flow_id_running = flow_generator
        .make_new_flow(
            system_flow_type,
            FlowStatus::Running,
            wasya_manual_trigger,
            None,
        )
        .await;
    let flow_id_finished = flow_generator
        .make_new_flow(
            system_flow_type,
            FlowStatus::Finished,
            automatic_trigger,
            None,
        )
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
    pagination: PaginationOpts,
    expected_total_count: usize,
    expected_flow_ids: Vec<FlowID>,
) {
    let total_flows_count = flow_event_store
        .get_count_flows_by_dataset(&dataset_test_case.dataset_id, &filters)
        .await
        .unwrap();
    assert_eq!(expected_total_count, total_flows_count);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids_by_dataset(&dataset_test_case.dataset_id, &filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

async fn assert_multiple_dataset_flow_expectations(
    flow_event_store: Arc<dyn FlowEventStore>,
    dataset_ids: Vec<DatasetID>,
    filters: DatasetFlowFilters,
    pagination: PaginationOpts,
    expected_flow_ids: Vec<FlowID>,
) {
    let flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids_by_datasets(HashSet::from_iter(dataset_ids), &filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

async fn assert_system_flow_expectaitons(
    flow_event_store: Arc<dyn FlowEventStore>,
    filters: SystemFlowFilters,
    pagination: PaginationOpts,
    expected_total_count: usize,
    expected_flow_ids: Vec<FlowID>,
) {
    let total_flows_count = flow_event_store
        .get_count_system_flows(&filters)
        .await
        .unwrap();
    assert_eq!(expected_total_count, total_flows_count);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_system_flow_ids(&filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

async fn assert_all_flow_expectaitons(
    flow_event_store: Arc<dyn FlowEventStore>,
    filters: AllFlowFilters,
    pagination: PaginationOpts,
    expected_total_count: usize,
    expected_flow_ids: Vec<FlowID>,
) {
    let total_flows_count = flow_event_store
        .get_count_all_flows(&filters)
        .await
        .unwrap();
    assert_eq!(expected_total_count, total_flows_count);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids(&filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetFlowGenerator<'a> {
    dataset_id: &'a DatasetID,
    flow_event_store: Arc<dyn FlowEventStore>,
}

impl<'a> DatasetFlowGenerator<'a> {
    fn new(dataset_id: &'a DatasetID, flow_event_store: Arc<dyn FlowEventStore>) -> Self {
        Self {
            dataset_id,
            flow_event_store,
        }
    }

    async fn make_new_flow(
        &self,
        flow_type: DatasetFlowType,
        expected_status: FlowStatus,
        initial_trigger: FlowTriggerType,
        config_snapshot: Option<FlowConfigurationRule>,
    ) -> FlowID {
        let flow_id = self.flow_event_store.new_flow_id().await.unwrap();

        let creation_moment = Utc::now();

        let mut flow = Flow::new(
            creation_moment,
            flow_id,
            FlowKeyDataset {
                dataset_id: self.dataset_id.clone(),
                flow_type,
            }
            .into(),
            initial_trigger,
            config_snapshot,
        );

        drive_flow_to_status(&mut flow, expected_status);

        flow.save(self.flow_event_store.as_ref()).await.unwrap();

        flow_id
    }

    async fn start_running_flow(&self, flow_id: FlowID) {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .unwrap();

        assert_eq!(flow.status(), FlowStatus::Waiting);

        drive_flow_to_status(&mut flow, FlowStatus::Running);

        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }

    async fn finish_running_flow(&self, flow_id: FlowID, outcome: TaskOutcome) {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .unwrap();

        assert_eq!(flow.status(), FlowStatus::Running);

        let flow_id: u64 = flow.flow_id.into();

        flow.on_task_finished(
            flow.timing.running_since.unwrap() + Duration::minutes(10),
            TaskID::new(flow_id * 2 + 1),
            outcome,
        )
        .unwrap();

        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SystemFlowGenerator {
    flow_event_store: Arc<dyn FlowEventStore>,
}

impl SystemFlowGenerator {
    fn new(flow_event_store: Arc<dyn FlowEventStore>) -> Self {
        Self { flow_event_store }
    }

    async fn make_new_flow(
        &self,
        flow_type: SystemFlowType,
        expected_status: FlowStatus,
        initial_trigger_type: FlowTriggerType,
        config_snapshot: Option<FlowConfigurationRule>,
    ) -> FlowID {
        let flow_id = self.flow_event_store.new_flow_id().await.unwrap();

        let creation_moment = Utc::now();

        let mut flow = Flow::new(
            creation_moment,
            flow_id,
            FlowKey::System(FlowKeySystem { flow_type }),
            initial_trigger_type,
            config_snapshot,
        );

        drive_flow_to_status(&mut flow, expected_status);

        flow.save(self.flow_event_store.as_ref()).await.unwrap();

        flow_id
    }

    async fn start_running_flow(&self, flow_id: FlowID) {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .unwrap();

        assert_eq!(flow.status(), FlowStatus::Waiting);

        drive_flow_to_status(&mut flow, FlowStatus::Running);

        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }

    async fn finish_running_flow(&self, flow_id: FlowID, outcome: TaskOutcome) {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .unwrap();

        assert_eq!(flow.status(), FlowStatus::Running);

        let flow_id: u64 = flow.flow_id.into();

        flow.on_task_finished(
            flow.timing.running_since.unwrap() + Duration::minutes(10),
            TaskID::new(flow_id * 2 + 1),
            outcome,
        )
        .unwrap();

        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn drive_flow_to_status(flow: &mut Flow, expected_status: FlowStatus) {
    let start_moment = Utc::now();

    flow.set_relevant_start_condition(
        start_moment + Duration::seconds(1),
        FlowStartCondition::Schedule(FlowStartConditionSchedule {
            wake_up_at: start_moment + Duration::minutes(1),
        }),
    )
    .unwrap();

    flow.schedule_for_activation(
        start_moment + Duration::seconds(1),
        start_moment + Duration::minutes(1),
    )
    .unwrap();

    if expected_status != FlowStatus::Waiting {
        // Derived task id from flow id just to ensure unique values
        let flow_id: u64 = flow.flow_id.into();
        let task_id = TaskID::new(flow_id * 2 + 1);

        flow.on_task_scheduled(start_moment + Duration::minutes(5), task_id)
            .unwrap();
        flow.on_task_running(start_moment + Duration::minutes(7), task_id)
            .unwrap();

        if expected_status == FlowStatus::Finished {
            flow.on_task_finished(
                start_moment + Duration::minutes(10),
                task_id,
                TaskOutcome::Success(TaskResult::Empty),
            )
            .unwrap();
        } else if expected_status != FlowStatus::Running {
            panic!("Not expecting flow status {expected_status:?}");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
