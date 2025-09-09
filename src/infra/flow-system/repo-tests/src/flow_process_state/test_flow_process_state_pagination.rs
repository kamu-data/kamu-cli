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
