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
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
    compaction_dataset_binding,
    ingest_dataset_binding,
};
use kamu_flow_system::*;
use kamu_task_system::{TaskError, TaskID, TaskOutcome, TaskResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_flow_empty_filters_distingush_dataset(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let always_happy_filters = FlowFilters::default();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let bar_cases = make_dataset_test_case(flow_event_store.clone()).await;

    assert_dataset_flow_expectations(
        flow_event_store.clone(),
        &foo_cases,
        always_happy_filters.clone(),
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        6,
        vec![
            foo_cases.compaction_flow_ids.flow_id_waiting,
            foo_cases.ingest_flow_ids.flow_id_waiting,
            foo_cases.compaction_flow_ids.flow_id_running,
            foo_cases.ingest_flow_ids.flow_id_running,
            foo_cases.compaction_flow_ids.flow_id_finished,
            foo_cases.ingest_flow_ids.flow_id_finished,
        ],
    )
    .await;

    assert_dataset_flow_expectations(
        flow_event_store.clone(),
        &bar_cases,
        always_happy_filters.clone(),
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        6,
        vec![
            bar_cases.compaction_flow_ids.flow_id_waiting,
            bar_cases.ingest_flow_ids.flow_id_waiting,
            bar_cases.compaction_flow_ids.flow_id_running,
            bar_cases.ingest_flow_ids.flow_id_running,
            bar_cases.compaction_flow_ids.flow_id_finished,
            bar_cases.ingest_flow_ids.flow_id_finished,
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
            FlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            FlowFilters {
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
        assert_dataset_flow_expectations(
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
            FlowFilters {
                by_flow_types: Some(vec![FLOW_TYPE_DATASET_INGEST.to_string()]),
                ..Default::default()
            },
            vec![
                foo_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
        (
            FlowFilters {
                by_flow_types: Some(vec![FLOW_TYPE_DATASET_COMPACT.to_string()]),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_finished,
            ],
        ),
        (
            FlowFilters {
                by_flow_types: Some(vec![FLOW_TYPE_DATASET_TRANSFORM.to_string()]),
                ..Default::default()
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectations(
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

    let wasya_filter = HashSet::from_iter([odf::AccountID::new_seeded_ed25519(b"wasya")]);
    let petya_filter = HashSet::from_iter([odf::AccountID::new_seeded_ed25519(b"petya")]);

    let cases = vec![
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::Account(wasya_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::Account(petya_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            FlowFilters {
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
        assert_dataset_flow_expectations(
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
        odf::AccountID::new_seeded_ed25519(b"wasya"),
        odf::AccountID::new_seeded_ed25519(b"petya"),
    ]);
    let mut wasya_patya_unrelated_filter = wasya_patya_filter.clone();
    wasya_patya_unrelated_filter.insert(odf::AccountID::new_seeded_ed25519(b"unrelated_user"));

    let cases = vec![
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::Account(wasya_patya_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        // should return the same amount even if some non existing user was provided
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::Account(wasya_patya_unrelated_filter)),
                ..Default::default()
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectations(
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
    let petya_filter = HashSet::from_iter([odf::AccountID::new_seeded_ed25519(b"petya")]);

    let cases = vec![
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                by_flow_types: Some(vec![FLOW_TYPE_DATASET_INGEST.to_string()]),
                by_initiator: Some(InitiatorFilter::System),
            },
            vec![foo_cases.ingest_flow_ids.flow_id_finished],
        ),
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                by_flow_types: Some(vec![FLOW_TYPE_DATASET_COMPACT.to_string()]),
                by_initiator: Some(InitiatorFilter::Account(petya_filter)),
            },
            vec![foo_cases.compaction_flow_ids.flow_id_waiting],
        ),
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                by_flow_types: Some(vec![FLOW_TYPE_DATASET_INGEST.to_string()]),
                by_initiator: Some(InitiatorFilter::System),
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_dataset_flow_expectations(
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
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
        (
            vec![foo_cases.dataset_id.clone(), bar_cases.dataset_id.clone()],
            vec![
                bar_cases.compaction_flow_ids.flow_id_waiting,
                bar_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
                bar_cases.compaction_flow_ids.flow_id_running,
                bar_cases.ingest_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
                bar_cases.compaction_flow_ids.flow_id_finished,
                bar_cases.ingest_flow_ids.flow_id_finished,
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
        (vec![odf::DatasetID::new_seeded_ed25519(b"wrong")], vec![]),
    ];

    for (dataset_ids, expected_flow_ids) in cases {
        assert_multiple_dataset_flow_expectations(
            flow_event_store.clone(),
            dataset_ids,
            FlowFilters::default(),
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
        (vec![odf::DatasetID::new_seeded_ed25519(b"wrong")], vec![]),
    ];

    for (dataset_ids, expected_flow_ids) in cases {
        assert_multiple_dataset_flow_expectations(
            flow_event_store.clone(),
            dataset_ids,
            FlowFilters {
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

    // Expected order:
    // bar  compact waiting
    // bar  ingest  waiting     <- (foo+bar) offset: 1
    // foo  compact waiting
    // foo  ingest  waiting
    // bar  compact running
    // bar  ingest  running
    // foo  compact running     <- (foo) offset: 2
    // foo  ingest  running     <- (foo+bar) offset: 1, limit: 7
    // bar  compact finished
    // bar  ingest  finished
    // foo  compact finished    <- (foo) offset: 2, limit: 3
    // foo  ingest  finished
    let cases = vec![
        (
            vec![foo_cases.dataset_id.clone()],
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_finished,
            ],
            PaginationOpts {
                offset: 2,
                limit: 3,
            },
        ),
        (
            vec![foo_cases.dataset_id.clone(), bar_cases.dataset_id.clone()],
            vec![
                bar_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
                bar_cases.compaction_flow_ids.flow_id_running,
                bar_cases.ingest_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
            PaginationOpts {
                offset: 1,
                limit: 7,
            },
        ),
        (
            vec![odf::DatasetID::new_seeded_ed25519(b"wrong")],
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
            FlowFilters::default(),
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
                foo_cases.compaction_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_waiting,
            ],
        ),
        (
            PaginationOpts {
                offset: 2,
                limit: 3,
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_running,
                foo_cases.ingest_flow_ids.flow_id_running,
                foo_cases.compaction_flow_ids.flow_id_finished,
            ],
        ),
        (
            PaginationOpts {
                offset: 4,
                limit: 2,
            },
            vec![
                foo_cases.compaction_flow_ids.flow_id_finished,
                foo_cases.ingest_flow_ids.flow_id_finished,
            ],
        ),
        (
            PaginationOpts {
                offset: 5,
                limit: 2,
            },
            vec![foo_cases.ingest_flow_ids.flow_id_finished],
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
        assert_dataset_flow_expectations(
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
            FlowFilters {
                by_flow_types: Some(vec![FLOW_TYPE_DATASET_INGEST.to_string()]),
                ..Default::default()
            },
            3,
            vec![
                foo_cases.ingest_flow_ids.flow_id_waiting,
                foo_cases.ingest_flow_ids.flow_id_running,
            ],
        ),
        (
            PaginationOpts {
                offset: 1,
                limit: 2,
            },
            FlowFilters {
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
            FlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            2,
            vec![foo_cases.ingest_flow_ids.flow_id_finished],
        ),
    ];

    for (pagination, filters, expected_total_count, expected_flow_ids) in cases {
        assert_dataset_flow_expectations(
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
        .list_scoped_flow_initiators(FlowScopeDataset::query_for_single_dataset(
            &foo_cases.dataset_id,
        ))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        res,
        HashSet::from([
            odf::AccountID::new_seeded_ed25519(b"petya"),
            odf::AccountID::new_seeded_ed25519(b"wasya"),
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_unfiltered_system_flows(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    assert_system_flow_expectations(
        flow_event_store.clone(),
        FlowFilters::default(),
        PaginationOpts {
            offset: 0,
            limit: 100,
        },
        3,
        vec![
            system_case.gc_flow_ids.flow_id_waiting,
            system_case.gc_flow_ids.flow_id_running,
            system_case.gc_flow_ids.flow_id_finished,
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_system_flows_filtered_by_flow_type(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let system_case = make_system_test_case(flow_event_store.clone()).await;

    let cases = vec![(
        FlowFilters {
            by_flow_types: Some(vec![FLOW_TYPE_SYSTEM_GC.to_string()]),
            ..Default::default()
        },
        vec![
            system_case.gc_flow_ids.flow_id_waiting,
            system_case.gc_flow_ids.flow_id_running,
            system_case.gc_flow_ids.flow_id_finished,
        ],
    )];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectations(
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
            FlowFilters {
                by_flow_status: Some(FlowStatus::Waiting),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_waiting],
        ),
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_running],
        ),
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_finished],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectations(
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

    let wasya_filter = HashSet::from_iter([odf::AccountID::new_seeded_ed25519(b"wasya")]);
    let unrelated_user_filter =
        HashSet::from_iter([odf::AccountID::new_seeded_ed25519(b"unrelated-user")]);

    let cases = vec![
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_finished],
        ),
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::Account(wasya_filter)),
                ..Default::default()
            },
            vec![system_case.gc_flow_ids.flow_id_running],
        ),
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::Account(unrelated_user_filter)),
                ..Default::default()
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectations(
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
    let petya_filter = HashSet::from_iter([odf::AccountID::new_seeded_ed25519(b"petya")]);

    let cases = vec![
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Finished),
                by_initiator: Some(InitiatorFilter::System),
                by_flow_types: Some(vec![FLOW_TYPE_SYSTEM_GC.to_string()]),
            },
            vec![system_case.gc_flow_ids.flow_id_finished],
        ),
        (
            FlowFilters {
                by_initiator: Some(InitiatorFilter::Account(petya_filter)),
                by_flow_status: Some(FlowStatus::Waiting),
                by_flow_types: None,
            },
            vec![system_case.gc_flow_ids.flow_id_waiting],
        ),
        (
            FlowFilters {
                by_flow_status: Some(FlowStatus::Running),
                by_initiator: Some(InitiatorFilter::System),
                by_flow_types: Some(vec![FLOW_TYPE_SYSTEM_GC.to_string()]),
            },
            vec![],
        ),
    ];

    for (filters, expected_flow_ids) in cases {
        assert_system_flow_expectations(
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
                system_case.gc_flow_ids.flow_id_waiting,
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
                system_case.gc_flow_ids.flow_id_finished,
            ],
        ),
        (
            PaginationOpts {
                offset: 2,
                limit: 2,
            },
            vec![system_case.gc_flow_ids.flow_id_finished],
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
        assert_system_flow_expectations(
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
            FlowFilters {
                by_flow_types: Some(vec![FLOW_TYPE_SYSTEM_GC.to_string()]),
                ..Default::default()
            },
            3,
            vec![
                system_case.gc_flow_ids.flow_id_waiting,
                system_case.gc_flow_ids.flow_id_running,
            ],
        ),
        (
            PaginationOpts {
                offset: 0,
                limit: 2,
            },
            FlowFilters {
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
            FlowFilters {
                by_initiator: Some(InitiatorFilter::System),
                ..Default::default()
            },
            1,
            vec![],
        ),
    ];

    for (pagination, filters, expected_total_count, expected_flow_ids) in cases {
        assert_system_flow_expectations(
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

    assert_all_flow_expectations(
        flow_event_store.clone(),
        FlowFilters::default(),
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
        assert_all_flow_expectations(
            flow_event_store.clone(),
            FlowFilters::default(),
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

    assert_all_flow_expectations(
        flow_event_store.clone(),
        FlowFilters {
            by_flow_types: None,
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

    assert_all_flow_expectations(
        flow_event_store.clone(),
        FlowFilters {
            by_flow_types: None,
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

    assert_all_flow_expectations(
        flow_event_store.clone(),
        FlowFilters {
            by_flow_types: None,
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

pub async fn test_pending_flow_dataset_single_type_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();

    let flow_binding = ingest_dataset_binding(&dataset_id);

    // No pending yet
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flow
    let flow_generator = DatasetFlowGenerator::new(&dataset_id, flow_event_store.clone());
    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    let flow_id = flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_INGEST,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;

    // Got pending
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    // Flow starts running
    flow_generator.start_running_flow(flow_id).await;

    // Got pending
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Success(TaskResult::empty()))
        .await;

    // No more pending
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pending_flow_dataset_multiple_types_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();

    let flow_binding_ingest = ingest_dataset_binding(&dataset_id);
    let flow_binding_compact = compaction_dataset_binding(&dataset_id);

    // No pending yet
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_compact)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flows
    let flow_generator = DatasetFlowGenerator::new(&dataset_id, flow_event_store.clone());
    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    let flow_id_ingest = flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_INGEST,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;
    let flow_id_compact = flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_COMPACT,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_compact)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_compact));

    // Flows start running
    flow_generator.start_running_flow(flow_id_ingest).await;
    flow_generator.start_running_flow(flow_id_compact).await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_compact)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_compact));

    // Ingest finishes with success
    flow_generator
        .finish_running_flow(flow_id_ingest, TaskOutcome::Success(TaskResult::empty()))
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_compact)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_compact));

    // Compact finishes with failure
    flow_generator
        .finish_running_flow(
            flow_id_compact,
            TaskOutcome::Failed(TaskError::empty_recoverable()),
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&flow_binding_compact)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pending_flow_multiple_datasets_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let dataset_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let binding_foo_ingest = ingest_dataset_binding(&dataset_foo_id);
    let binding_bar_ingest = ingest_dataset_binding(&dataset_bar_id);
    let binding_foo_compact = compaction_dataset_binding(&dataset_foo_id);

    // No pending yet
    let res = flow_event_store
        .try_get_pending_flow(&binding_foo_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&binding_bar_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&binding_foo_compact)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flows
    let foo_flow_generator = DatasetFlowGenerator::new(&dataset_foo_id, flow_event_store.clone());
    let bar_flow_generator = DatasetFlowGenerator::new(&dataset_bar_id, flow_event_store.clone());
    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    let flow_id_foo_ingest = foo_flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_INGEST,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;
    let flow_id_bar_ingest = bar_flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_INGEST,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;
    let flow_id_foo_compacting = foo_flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_COMPACT,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&binding_foo_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_foo_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&binding_bar_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_bar_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&binding_foo_compact)
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
        .finish_running_flow(
            flow_id_foo_ingest,
            TaskOutcome::Success(TaskResult::empty()),
        )
        .await;
    foo_flow_generator
        .finish_running_flow(
            flow_id_foo_compacting,
            TaskOutcome::Success(TaskResult::empty()),
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&binding_foo_ingest)
        .await
        .unwrap();
    assert!(res.is_none());
    let res = flow_event_store
        .try_get_pending_flow(&binding_bar_ingest)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id_bar_ingest));
    let res = flow_event_store
        .try_get_pending_flow(&binding_foo_compact)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pending_flow_system_flow_crud(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let binding_gc = FlowBinding::new(FLOW_TYPE_SYSTEM_GC, FlowScope::make_system_scope());

    // No pending yet

    let res = flow_event_store
        .try_get_pending_flow(&binding_gc)
        .await
        .unwrap();
    assert!(res.is_none());

    // Schedule flow

    let flow_generator = SystemFlowGenerator::new(flow_event_store.clone());
    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    let flow_id = flow_generator
        .make_new_flow(
            FLOW_TYPE_SYSTEM_GC,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
        )
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&binding_gc)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    // Run flow
    flow_generator.start_running_flow(flow_id).await;

    let res = flow_event_store
        .try_get_pending_flow(&binding_gc)
        .await
        .unwrap();
    assert_eq!(res, Some(flow_id));

    // Finish flow
    flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Success(TaskResult::empty()))
        .await;

    let res = flow_event_store
        .try_get_pending_flow(&binding_gc)
        .await
        .unwrap();
    assert!(res.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_event_store_concurrent_modification(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let flow_id = event_store.new_flow_id().await.unwrap();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let flow_binding = ingest_dataset_binding(&dataset_id);

    // Nothing stored yet, but prev stored event id sent => CM
    let res = event_store
        .save_events(
            &flow_id,
            Some(EventID::new(15)),
            vec![
                FlowEventInitiated {
                    event_time: Utc::now(),
                    flow_binding: flow_binding.clone(),
                    flow_id,
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: Utc::now(),
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
            ],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Nothing stored yet, no storage expectation => OK
    let res = event_store
        .save_events(
            &flow_id,
            None,
            vec![
                FlowEventInitiated {
                    event_time: Utc::now(),
                    flow_binding: flow_binding.clone(),
                    flow_id,
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: Utc::now(),
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
            ],
        )
        .await;
    assert_matches!(res, Ok(_));

    // Something stored, but no expectation => CM
    let res = event_store
        .save_events(
            &flow_id,
            None,
            vec![
                FlowEventAborted {
                    event_time: Utc::now(),
                    flow_id,
                    flow_binding: flow_binding.clone(),
                }
                .into(),
            ],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, but expectation is wrong => CM
    let res = event_store
        .save_events(
            &flow_id,
            Some(EventID::new(15)),
            vec![
                FlowEventAborted {
                    event_time: Utc::now(),
                    flow_id,
                    flow_binding: flow_binding.clone(),
                }
                .into(),
            ],
        )
        .await;
    assert_matches!(res, Err(SaveEventsError::ConcurrentModification(_)));

    // Something stored, and expectation is correct
    let res = event_store
        .save_events(
            &flow_id,
            Some(EventID::new(1)),
            vec![
                FlowEventAborted {
                    event_time: Utc::now(),
                    flow_id,
                    flow_binding,
                }
                .into(),
            ],
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
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let flow_binding = ingest_dataset_binding(&dataset_id);

    let start_moment = Utc::now().trunc_subsecs(6);
    let activation_moment = start_moment + Duration::minutes(1);

    let last_event_id = event_store
        .save_events(
            &flow_id,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_moment,
                    flow_binding: flow_binding.clone(),
                    flow_id,
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: start_moment,
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    let maybe_nearest_activation_time = event_store.nearest_flow_activation_moment().await.unwrap();
    assert!(maybe_nearest_activation_time.is_none());

    let last_event_id = event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![
                FlowEventStartConditionUpdated {
                    flow_id,
                    flow_binding: flow_binding.clone(),
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment,
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
            ],
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
            vec![
                FlowEventScheduledForActivation {
                    flow_id,
                    flow_binding: flow_binding.clone(),
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

    let last_event_id = event_store
        .save_events(
            &flow_id,
            Some(last_event_id),
            vec![
                FlowEventTaskScheduled {
                    flow_id,
                    flow_binding: flow_binding.clone(),
                    event_time: activation_moment + Duration::milliseconds(100),
                    task_id: TaskID::new(1),
                }
                .into(),
            ],
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
            vec![
                FlowEventTaskRunning {
                    flow_id,
                    flow_binding: flow_binding.clone(),
                    event_time: activation_moment + Duration::milliseconds(500),
                    task_id: TaskID::new(1),
                }
                .into(),
            ],
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
            vec![
                FlowEventTaskFinished {
                    flow_id,
                    flow_binding,
                    event_time: activation_moment + Duration::milliseconds(1500),
                    task_id: TaskID::new(1),
                    task_outcome: TaskOutcome::Success(TaskResult::empty()),
                    next_attempt_at: None,
                }
                .into(),
            ],
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_flow_activation_visibility_when_aborted_before_activation(catalog: &Catalog) {
    let event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let flow_id = event_store.new_flow_id().await.unwrap();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let flow_binding = ingest_dataset_binding(&dataset_id);

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
                    flow_binding: flow_binding.clone(),
                    flow_id,
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: start_moment,
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id,
                    flow_binding: flow_binding.clone(),
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment,
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id,
                    flow_binding: flow_binding.clone(),
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
            vec![
                FlowEventAborted {
                    event_time: abortion_moment,
                    flow_id,
                    flow_binding,
                }
                .into(),
            ],
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

    let dataset_id_foo = odf::DatasetID::new_seeded_ed25519(b"foo");
    let dataset_id_bar = odf::DatasetID::new_seeded_ed25519(b"bar");
    let dataset_id_baz = odf::DatasetID::new_seeded_ed25519(b"baz");

    let flow_id_foo = event_store.new_flow_id().await.unwrap();
    let flow_id_bar = event_store.new_flow_id().await.unwrap();
    let flow_id_baz = event_store.new_flow_id().await.unwrap();

    let flow_binding_foo = ingest_dataset_binding(&dataset_id_foo);
    let flow_binding_bar = ingest_dataset_binding(&dataset_id_bar);
    let flow_binding_baz = ingest_dataset_binding(&dataset_id_baz);

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
                    flow_binding: flow_binding_foo.clone(),
                    flow_id: flow_id_foo,
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: start_moment,
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id: flow_id_foo,
                    flow_binding: flow_binding_foo.clone(),
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment_1,
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id: flow_id_foo,
                    flow_binding: flow_binding_foo,
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
                    flow_binding: flow_binding_bar.clone(),
                    flow_id: flow_id_bar,
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: start_moment,
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id: flow_id_bar,
                    flow_binding: flow_binding_bar.clone(),
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment_1,
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id: flow_id_bar,
                    flow_binding: flow_binding_bar,
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
                    flow_binding: flow_binding_baz.clone(),
                    flow_id: flow_id_baz,
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: start_moment,
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    flow_id: flow_id_baz,
                    flow_binding: flow_binding_baz.clone(),
                    event_time: Utc::now(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: activation_moment_2,
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    flow_id: flow_id_baz,
                    flow_binding: flow_binding_baz,
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

pub async fn test_get_all_scope_pending_flows(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    // Create a dataset and schedule multiple flows
    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let flow_generator = DatasetFlowGenerator::new(&dataset_id, flow_event_store.clone());

    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    // Schedule two flows for the dataset

    let flow_id_1 = flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_INGEST,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;

    let flow_id_2 = flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_COMPACT,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;

    let flow_id_3 = flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_RESET,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            None,
        )
        .await;

    // Start running both flows
    flow_generator.start_running_flow(flow_id_1).await;
    flow_generator.start_running_flow(flow_id_2).await;

    // Finish the compaction flow (no longer pending)
    flow_generator
        .finish_running_flow(flow_id_2, TaskOutcome::Success(TaskResult::empty()))
        .await;

    // Call the method under test
    let pending_flows = flow_event_store
        .try_get_all_scope_pending_flows(&FlowScopeDataset::make_scope(&dataset_id))
        .await
        .unwrap();

    // Expect no flow_id_2, which finished
    assert_eq!(pending_flows, vec![flow_id_3, flow_id_1]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_flows_for_multiple_datasets(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    let foo_cases = make_dataset_test_case(flow_event_store.clone()).await;
    let bar_cases = make_dataset_test_case(flow_event_store.clone()).await;
    // Create a bit more flows to be sure we filter correctly
    make_dataset_test_case(flow_event_store.clone()).await;

    let mut dataset_ids = vec![&foo_cases.dataset_id, &bar_cases.dataset_id];
    dataset_ids.sort();

    let mut expected_flow_ids = [
        foo_cases.ingest_flow_ids.as_vec(),
        foo_cases.compaction_flow_ids.as_vec(),
        bar_cases.ingest_flow_ids.as_vec(),
        bar_cases.compaction_flow_ids.as_vec(),
    ]
    .concat();

    let flow_scope_query = FlowScopeDataset::query_for_multiple_datasets(&dataset_ids);

    let total_count = flow_event_store
        .get_count_flows_matching_scope_query(&flow_scope_query, &FlowFilters::default())
        .await
        .unwrap();

    assert_eq!(total_count, expected_flow_ids.len());

    let mut total_flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids_matching_scope_query(
            flow_scope_query,
            &FlowFilters::default(),
            PaginationOpts {
                offset: 0,
                limit: 100,
            },
        )
        .try_collect()
        .await
        .unwrap();

    expected_flow_ids.sort();
    total_flow_ids.sort();

    assert_eq!(total_flow_ids.len(), expected_flow_ids.len());
    assert_eq!(total_flow_ids, expected_flow_ids);

    // Test dataset having flows
    let mut flow_scopes = Vec::new();
    for dataset_id in &dataset_ids {
        flow_scopes.push(FlowScopeDataset::make_scope(dataset_id));
    }
    flow_scopes.push(FlowScopeDataset::make_scope(
        &odf::DatasetID::new_seeded_ed25519(b"empty"),
    ));

    let filtered_flow_scopes = flow_event_store
        .filter_flow_scopes_having_flows(&flow_scopes)
        .await
        .unwrap();

    let mut result: Vec<odf::DatasetID> = filtered_flow_scopes
        .iter()
        .map(|scope| FlowScopeDataset::new(scope).dataset_id())
        .collect();
    result.sort();

    assert_eq!(result.len(), dataset_ids.len());
    for (i, dataset_id) in dataset_ids.iter().enumerate() {
        assert_eq!(result[i], **dataset_id);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_flow_through_retry_attempts(catalog: &Catalog) {
    let flow_event_store = catalog.get_one::<dyn FlowEventStore>().unwrap();

    // Create a dataset
    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let flow_generator = DatasetFlowGenerator::new(&dataset_id, flow_event_store.clone());

    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    // Schedule two flows for the dataset

    let flow_id = flow_generator
        .make_new_flow(
            FLOW_TYPE_DATASET_INGEST,
            FlowStatus::Waiting,
            automatic_cause.clone(),
            None,
            Some(RetryPolicy {
                max_attempts: 3,
                min_delay_seconds: 30,
                backoff_type: RetryBackoffType::Fixed,
            }),
        )
        .await;

    // Run the flow 3 times with an error result (main attempt + 2 retry attemps)
    for _ in 0..1 {
        // Start running the flow
        flow_generator.start_running_flow(flow_id).await;

        // Simulate a failure
        let flow = flow_generator
            .finish_running_flow(flow_id, TaskOutcome::Failed(TaskError::empty_recoverable()))
            .await;

        // Check flow status and timing properties
        assert_eq!(flow.status(), FlowStatus::Retrying);
        assert!(flow.timing.scheduled_for_activation_at.is_some());
        assert!(flow.timing.last_attempt_finished_at.is_some());
        assert!(flow.timing.running_since.is_none());
        assert!(flow.timing.awaiting_executor_since.is_none());
        assert_eq!(
            flow.timing.scheduled_for_activation_at.unwrap(),
            flow.timing.last_attempt_finished_at.unwrap() + Duration::seconds(30) // retry period
        );

        // The flow should still be pending, because of retrying status
        let pending_flows = flow_event_store
            .try_get_all_scope_pending_flows(&FlowScopeDataset::make_scope(&dataset_id))
            .await
            .unwrap();
        assert_eq!(pending_flows.len(), 1);
        assert_eq!(pending_flows[0], flow_id);
    }

    // Run the flow for the last retry attempt, and succeeed
    flow_generator.start_running_flow(flow_id).await;
    let final_flow = flow_generator
        .finish_running_flow(flow_id, TaskOutcome::Success(TaskResult::empty()))
        .await;

    // Check final flow status and timing properties
    assert_eq!(final_flow.status(), FlowStatus::Finished);
    assert!(final_flow.timing.scheduled_for_activation_at.is_some());
    assert!(final_flow.timing.last_attempt_finished_at.is_some());
    assert!(final_flow.timing.running_since.is_some());
    assert!(final_flow.timing.awaiting_executor_since.is_some());
    assert!(
        final_flow.timing.scheduled_for_activation_at.unwrap()
            < final_flow.timing.last_attempt_finished_at.unwrap()
    );

    // The flow should no longer be pending
    let pending_flows = flow_event_store
        .try_get_all_scope_pending_flows(&FlowScopeDataset::make_scope(&dataset_id))
        .await
        .unwrap();
    assert!(pending_flows.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetTestCase {
    dataset_id: odf::DatasetID,
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

impl TestFlowIDs {
    fn as_vec(&self) -> Vec<FlowID> {
        vec![
            self.flow_id_waiting,
            self.flow_id_running,
            self.flow_id_finished,
        ]
    }
}

async fn make_dataset_test_case(flow_event_store: Arc<dyn FlowEventStore>) -> DatasetTestCase {
    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let ingest_flow_ids = make_dataset_test_flows(
        &dataset_id,
        FLOW_TYPE_DATASET_INGEST,
        flow_event_store.clone(),
    )
    .await;
    let compaction_flow_ids =
        make_dataset_test_flows(&dataset_id, FLOW_TYPE_DATASET_COMPACT, flow_event_store).await;

    DatasetTestCase {
        dataset_id: dataset_id.clone(),
        ingest_flow_ids,
        compaction_flow_ids,
    }
}

async fn make_system_test_case(flow_event_store: Arc<dyn FlowEventStore>) -> SystemTestCase {
    SystemTestCase {
        gc_flow_ids: make_system_test_flows(FLOW_TYPE_SYSTEM_GC, flow_event_store).await,
    }
}

async fn make_dataset_test_flows(
    dataset_id: &odf::DatasetID,
    flow_type: &str,
    flow_event_store: Arc<dyn FlowEventStore>,
) -> TestFlowIDs {
    let flow_generator = DatasetFlowGenerator::new(dataset_id, flow_event_store.clone());

    let wasya_manual_cause = FlowActivationCause::Manual(FlowActivationCauseManual {
        activation_time: Utc::now(),
        initiator_account_id: odf::AccountID::new_seeded_ed25519(b"wasya"),
    });

    let petya_manual_cause = FlowActivationCause::Manual(FlowActivationCauseManual {
        activation_time: Utc::now(),
        initiator_account_id: odf::AccountID::new_seeded_ed25519(b"petya"),
    });

    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    let flow_id_waiting = flow_generator
        .make_new_flow(
            flow_type,
            FlowStatus::Waiting,
            petya_manual_cause,
            None,
            None,
        )
        .await;
    let flow_id_running = flow_generator
        .make_new_flow(
            flow_type,
            FlowStatus::Running,
            wasya_manual_cause,
            None,
            None,
        )
        .await;
    let flow_id_finished = flow_generator
        .make_new_flow(flow_type, FlowStatus::Finished, automatic_cause, None, None)
        .await;

    TestFlowIDs {
        flow_id_waiting,
        flow_id_running,
        flow_id_finished,
    }
}

async fn make_system_test_flows(
    flow_type: &str,
    flow_event_store: Arc<dyn FlowEventStore>,
) -> TestFlowIDs {
    let flow_generator = SystemFlowGenerator::new(flow_event_store.clone());

    let wasya_manual_cause = FlowActivationCause::Manual(FlowActivationCauseManual {
        activation_time: Utc::now(),
        initiator_account_id: odf::AccountID::new_seeded_ed25519(b"wasya"),
    });

    let petya_manual_cause = FlowActivationCause::Manual(FlowActivationCauseManual {
        activation_time: Utc::now(),
        initiator_account_id: odf::AccountID::new_seeded_ed25519(b"petya"),
    });

    let automatic_cause = FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
        activation_time: Utc::now(),
    });

    let flow_id_waiting = flow_generator
        .make_new_flow(flow_type, FlowStatus::Waiting, petya_manual_cause, None)
        .await;
    let flow_id_running = flow_generator
        .make_new_flow(flow_type, FlowStatus::Running, wasya_manual_cause, None)
        .await;
    let flow_id_finished = flow_generator
        .make_new_flow(flow_type, FlowStatus::Finished, automatic_cause, None)
        .await;

    TestFlowIDs {
        flow_id_waiting,
        flow_id_running,
        flow_id_finished,
    }
}

async fn assert_dataset_flow_expectations(
    flow_event_store: Arc<dyn FlowEventStore>,
    dataset_test_case: &DatasetTestCase,
    filters: FlowFilters,
    pagination: PaginationOpts,
    expected_total_count: usize,
    expected_flow_ids: Vec<FlowID>,
) {
    let flow_scope_query =
        FlowScopeDataset::query_for_single_dataset(&dataset_test_case.dataset_id);

    let total_flows_count = flow_event_store
        .get_count_flows_matching_scope_query(&flow_scope_query, &filters)
        .await
        .unwrap();
    assert_eq!(expected_total_count, total_flows_count);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids_matching_scope_query(flow_scope_query, &filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

async fn assert_multiple_dataset_flow_expectations(
    flow_event_store: Arc<dyn FlowEventStore>,
    dataset_ids: Vec<odf::DatasetID>,
    filters: FlowFilters,
    pagination: PaginationOpts,
    expected_flow_ids: Vec<FlowID>,
) {
    let dataset_id_refs: Vec<_> = dataset_ids.iter().collect();
    let flow_scope_query = FlowScopeDataset::query_for_multiple_datasets(&dataset_id_refs);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids_matching_scope_query(flow_scope_query, &filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

async fn assert_system_flow_expectations(
    flow_event_store: Arc<dyn FlowEventStore>,
    filters: FlowFilters,
    pagination: PaginationOpts,
    expected_total_count: usize,
    expected_flow_ids: Vec<FlowID>,
) {
    let flow_scope_query = FlowScopeQuery::build_for_system_scope();

    let total_flows_count = flow_event_store
        .get_count_flows_matching_scope_query(&flow_scope_query, &filters)
        .await
        .unwrap();
    assert_eq!(expected_total_count, total_flows_count);

    let flow_ids: Vec<_> = flow_event_store
        .get_all_flow_ids_matching_scope_query(flow_scope_query, &filters, pagination)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(flow_ids, expected_flow_ids);
}

async fn assert_all_flow_expectations(
    flow_event_store: Arc<dyn FlowEventStore>,
    filters: FlowFilters,
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
    dataset_id: &'a odf::DatasetID,
    flow_event_store: Arc<dyn FlowEventStore>,
}

impl<'a> DatasetFlowGenerator<'a> {
    fn new(dataset_id: &'a odf::DatasetID, flow_event_store: Arc<dyn FlowEventStore>) -> Self {
        Self {
            dataset_id,
            flow_event_store,
        }
    }

    async fn make_new_flow(
        &self,
        flow_type: &str,
        expected_status: FlowStatus,
        initial_activation_cause: FlowActivationCause,
        config_snapshot: Option<FlowConfigurationRule>,
        retry_policy: Option<RetryPolicy>,
    ) -> FlowID {
        let flow_id = self.flow_event_store.new_flow_id().await.unwrap();

        let creation_moment = Utc::now();

        let mut flow = Flow::new(
            creation_moment,
            flow_id,
            FlowBinding::new(flow_type, FlowScopeDataset::make_scope(self.dataset_id)),
            initial_activation_cause,
            config_snapshot,
            retry_policy,
        );

        drive_flow_to_status(&mut flow, expected_status);

        flow.save(self.flow_event_store.as_ref()).await.unwrap();

        flow_id
    }

    async fn start_running_flow(&self, flow_id: FlowID) -> Flow {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .unwrap();

        assert_matches!(flow.status(), FlowStatus::Waiting | FlowStatus::Retrying);

        drive_flow_to_status(&mut flow, FlowStatus::Running);

        flow.save(self.flow_event_store.as_ref()).await.unwrap();
        flow
    }

    async fn finish_running_flow(&self, flow_id: FlowID, outcome: TaskOutcome) -> Flow {
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
        flow
    }

    #[allow(dead_code)]
    async fn abort_flow(&self, flow_id: FlowID) {
        let mut flow = Flow::load(flow_id, self.flow_event_store.as_ref())
            .await
            .unwrap();
        assert_ne!(flow.status(), FlowStatus::Finished);

        flow.abort(Utc::now()).unwrap();

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
        flow_type: &str,
        expected_status: FlowStatus,
        initial_activation_cause: FlowActivationCause,
        config_snapshot: Option<FlowConfigurationRule>,
    ) -> FlowID {
        let flow_id = self.flow_event_store.new_flow_id().await.unwrap();

        let creation_moment = Utc::now();

        let mut flow = Flow::new(
            creation_moment,
            flow_id,
            FlowBinding::new(flow_type, FlowScope::make_system_scope()),
            initial_activation_cause,
            config_snapshot,
            None,
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

        flow.set_relevant_start_condition(
            start_moment + Duration::minutes(5),
            FlowStartCondition::Executor(FlowStartConditionExecutor { task_id }),
        )
        .unwrap();

        flow.on_task_scheduled(start_moment + Duration::minutes(5), task_id)
            .unwrap();
        flow.on_task_running(start_moment + Duration::minutes(7), task_id)
            .unwrap();

        if expected_status == FlowStatus::Finished {
            flow.on_task_finished(
                start_moment + Duration::minutes(10),
                task_id,
                TaskOutcome::Success(TaskResult::empty()),
            )
            .unwrap();
        } else if expected_status != FlowStatus::Running {
            panic!("Not expecting flow status {expected_status:?}");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
