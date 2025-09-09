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
use crate::flow_process_state::helpers::assert_last_attempt_at_ordering;

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
