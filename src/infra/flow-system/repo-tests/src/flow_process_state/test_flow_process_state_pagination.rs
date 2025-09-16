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
        .list_processes(filter, order, None)
        .await
        .unwrap();

    // Test pagination: get first 10 processes
    let first_page = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 10,
                offset: 0,
            }),
        )
        .await
        .unwrap();

    assert_eq!(first_page.processes.len(), 10);

    // Get second page
    let second_page = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 10,
                offset: 10,
            }),
        )
        .await
        .unwrap();

    assert_eq!(second_page.processes.len(), 10);

    // Get third page (should have 4 remaining)
    let third_page = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 10,
                offset: 20,
            }),
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

pub async fn test_list_processes_pagination_edge_cases(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test: limit = 0 should return empty results
    let empty_result = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 0,
                offset: 0,
            }),
        )
        .await
        .unwrap();

    assert_eq!(empty_result.processes.len(), 0);

    // Test: offset beyond available data should return empty results
    let beyond_offset = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 10,
                offset: 1000, // Way beyond the 24 available records
            }),
        )
        .await
        .unwrap();

    assert_eq!(beyond_offset.processes.len(), 0);

    // Test: limit larger than available data should return all remaining
    let large_limit = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 1000, // Much larger than the 24 available records
                offset: 0,
            }),
        )
        .await
        .unwrap();

    assert_eq!(large_limit.processes.len(), 24);

    // Test: offset at the last record
    let last_record = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 10,
                offset: 23, // Should return 1 record (24th record)
            }),
        )
        .await
        .unwrap();

    assert_eq!(last_record.processes.len(), 1);

    // Test: offset exactly at the end
    let at_end = flow_process_state_query
        .list_processes(
            FlowProcessListFilter::all(),
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: 10,
                offset: 24, // Should return 0 records
            }),
        )
        .await
        .unwrap();

    assert_eq!(at_end.processes.len(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_pagination_with_filters(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test pagination with filter that reduces result set
    let filter =
        FlowProcessListFilter::all().with_effective_states(&[FlowProcessEffectiveState::Active]);

    // Get full filtered list first
    let full_filtered = flow_process_state_query
        .list_processes(filter, FlowProcessOrder::recent(), None)
        .await
        .unwrap();

    let filtered_count = full_filtered.processes.len();
    assert!(filtered_count > 0, "Should have some active processes");
    assert!(
        filtered_count < 24,
        "Should have fewer than total processes"
    );

    // Test pagination on filtered results
    let page_size = 3;
    let filter1 =
        FlowProcessListFilter::all().with_effective_states(&[FlowProcessEffectiveState::Active]);
    let first_page = flow_process_state_query
        .list_processes(
            filter1,
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: page_size,
                offset: 0,
            }),
        )
        .await
        .unwrap();

    let filter2 =
        FlowProcessListFilter::all().with_effective_states(&[FlowProcessEffectiveState::Active]);
    let second_page = flow_process_state_query
        .list_processes(
            filter2,
            FlowProcessOrder::recent(),
            Some(PaginationOpts {
                limit: page_size,
                offset: page_size,
            }),
        )
        .await
        .unwrap();

    // Verify no overlap between pages by checking that no process appears in both
    for first_process in &first_page.processes {
        for second_process in &second_page.processes {
            assert_ne!(
                first_process, second_process,
                "Pages should not have overlapping records"
            );
        }
    }

    // Verify all results are from the filtered set
    for process in &first_page.processes {
        assert_eq!(process.effective_state(), FlowProcessEffectiveState::Active);
    }
    for process in &second_page.processes {
        assert_eq!(process.effective_state(), FlowProcessEffectiveState::Active);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_pagination_different_orderings(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test pagination consistency across different ordering methods
    let orderings = vec![
        FlowProcessOrder::recent(),
        FlowProcessOrder {
            field: FlowProcessOrderField::ConsecutiveFailures,
            desc: true,
        },
        FlowProcessOrder {
            field: FlowProcessOrderField::NextPlannedAt,
            desc: false,
        },
    ];

    for order in orderings {
        // Get full list with this ordering
        let full_list = flow_process_state_query
            .list_processes(FlowProcessListFilter::all(), order, None)
            .await
            .unwrap();

        // Test pagination maintains order consistency
        let page_size = 8;
        let mut paginated_results = Vec::new();

        let mut offset = 0;
        loop {
            let page = flow_process_state_query
                .list_processes(
                    FlowProcessListFilter::all(),
                    order,
                    Some(PaginationOpts {
                        limit: page_size,
                        offset,
                    }),
                )
                .await
                .unwrap();

            if page.processes.is_empty() {
                break;
            }

            paginated_results.extend(page.processes);
            offset += page_size;
        }

        // Verify paginated results match full list
        assert_eq!(
            paginated_results.len(),
            full_list.processes.len(),
            "Paginated results should match full list length for order {order:?}",
        );

        for (i, (paginated, full)) in paginated_results
            .iter()
            .zip(full_list.processes.iter())
            .enumerate()
        {
            assert_eq!(
                paginated, full,
                "Record {i} should match between paginated and full results for order {order:?}",
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_processes_pagination_boundary_conditions(catalog: &Catalog) {
    let mut csv_loader = CsvFlowProcessStateLoader::new(catalog);
    csv_loader.populate_from_csv().await;

    let flow_process_state_query = catalog.get_one::<dyn FlowProcessStateQuery>().unwrap();

    // Test various boundary conditions for limit and offset
    let test_cases = vec![
        // (limit, offset, expected_len_description)
        (1, 0, "single record from start"),
        (1, 23, "single record from end"),
        (23, 0, "almost all records"),
        (24, 0, "exactly all records"),
        (25, 0, "more than available"),
        (5, 19, "partial last page"),
        (10, 15, "page crossing boundary"),
    ];

    for (limit, offset, description) in test_cases {
        let result = flow_process_state_query
            .list_processes(
                FlowProcessListFilter::all(),
                FlowProcessOrder::recent(),
                Some(PaginationOpts { limit, offset }),
            )
            .await
            .unwrap();

        let expected_len = if offset >= 24 {
            0
        } else {
            std::cmp::min(limit, 24 - offset)
        };

        assert_eq!(
            result.processes.len(),
            expected_len,
            "Failed for case: {description} (limit={limit}, offset={offset})",
        );

        // Verify all returned records are unique
        for i in 0..result.processes.len() {
            for j in (i + 1)..result.processes.len() {
                assert_ne!(
                    result.processes[i], result.processes[j],
                    "All returned records should be unique for case: {description}",
                );
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
