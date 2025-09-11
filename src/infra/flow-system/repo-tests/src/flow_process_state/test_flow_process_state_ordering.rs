// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use kamu_flow_system::*;

use super::csv_flow_process_state_loader::CsvFlowProcessStateLoader;
use crate::flow_process_state::helpers::{
    assert_consecutive_failures_ordering,
    assert_effective_state_ordering,
    assert_flow_type_ordering,
    assert_last_attempt_at_ordering,
    assert_last_failure_at_ordering,
    assert_next_planned_at_ordering,
};

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

pub async fn test_list_processes_ordering_next_planned_at_nulls_last(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // ordering by next_planned_at ASC - should have non-nulls first, nulls last
    let listing_asc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::NextPlannedAt,
                desc: false,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify ASC ordering with NULLS LAST
    assert_next_planned_at_ordering(&listing_asc.processes, false, "ASC ordering test");

    // ordering by next_planned_at DESC - should have non-nulls first
    // (farthest in future first), nulls last
    let listing_desc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::NextPlannedAt,
                desc: true,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify DESC ordering with NULLS LAST
    assert_next_planned_at_ordering(&listing_desc.processes, true, "DESC ordering test");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_ordering_last_failure_at_nulls_last(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // ordering by last_failure_at ASC - should have non-nulls first, nulls last
    let listing_asc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastFailureAt,
                desc: false,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify ASC ordering with NULLS LAST
    assert_last_failure_at_ordering(&listing_asc.processes, false, "ASC ordering test");

    // ordering by last_failure_at DESC - should have non-nulls first
    // (most recent failures first), nulls last
    let listing_desc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::LastFailureAt,
                desc: true,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify DESC ordering with NULLS LAST
    assert_last_failure_at_ordering(&listing_desc.processes, true, "DESC ordering test");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_ordering_consecutive_failures(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // ordering by consecutive_failures ASC - should have fewer failures first
    let listing_asc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::ConsecutiveFailures,
                desc: false,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify ASC ordering
    assert_consecutive_failures_ordering(&listing_asc.processes, false, "ASC ordering test");

    // ordering by consecutive_failures DESC - should have more failures first
    // (chronic issues first for triage)
    let listing_desc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::ConsecutiveFailures,
                desc: true,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify DESC ordering
    assert_consecutive_failures_ordering(&listing_desc.processes, true, "DESC ordering test");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_ordering_effective_state(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // ordering by effective_state ASC - should have lower priority states first
    let listing_asc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::EffectiveState,
                desc: false,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify ASC ordering
    assert_effective_state_ordering(&listing_asc.processes, false, "ASC ordering test");

    // ordering by effective_state DESC - should have higher priority states first
    // (critical states first for severity bucketing)
    let listing_desc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::EffectiveState,
                desc: true,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify DESC ordering
    assert_effective_state_ordering(&listing_desc.processes, true, "DESC ordering test");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_ordering_flow_type(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // ordering by flow_type ASC - should have alphabetical order
    let listing_asc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::FlowType,
                desc: false,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify ASC ordering
    assert_flow_type_ordering(&listing_asc.processes, false, "ASC ordering test");

    // ordering by flow_type DESC - should have reverse alphabetical order
    let listing_desc = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder {
                field: FlowProcessOrderField::FlowType,
                desc: true,
            },
            50, // Large limit to get all
            0,
        )
        .await
        .unwrap();

    // Verify DESC ordering
    assert_flow_type_ordering(&listing_desc.processes, true, "DESC ordering test");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
