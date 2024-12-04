// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{EntityListing, EntityStreamer, PaginationOpts};
use futures::TryStreamExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestPaginationOpts {
    total_entity_count: usize,
    expected_entities_call_count: usize,
    start_offset: usize,
    page_limit: usize,
    expected_entities: Vec<TestEntity>,
}

macro_rules! test_pagination {
    ($test_pagination_opts: expr) => {
        let TestPaginationOpts {
            total_entity_count,
            expected_entities_call_count,
            start_offset,
            page_limit,
            expected_entities,
        } = $test_pagination_opts;

        let entity_source = entity_source(total_entity_count, expected_entities_call_count);
        let streamer = EntityStreamer::new(start_offset, page_limit);

        let stream = streamer.into_stream(
            || async {
                let arguments = entity_source.init_arguments().await;
                Ok(arguments)
            },
            |_, pagination| {
                let entity_source = entity_source.clone();
                async move {
                    let listing = entity_source.entities(pagination).await;
                    Ok(listing)
                }
            },
        );

        let actual_entries = stream.try_collect::<Vec<_>>().await.unwrap();

        pretty_assertions::assert_eq!(expected_entities, actual_entries);
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pagination_less_than_a_page() {
    test_pagination!(TestPaginationOpts {
        total_entity_count: 3,
        start_offset: 0,
        page_limit: 5,
        expected_entities_call_count: 1,
        expected_entities: vec![
            TestEntity { id: 0 },
            TestEntity { id: 1 },
            TestEntity { id: 2 },
        ],
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pagination_fits_on_one_page() {
    test_pagination!(TestPaginationOpts {
        total_entity_count: 5,
        start_offset: 0,
        page_limit: 5,
        expected_entities_call_count: 1,
        expected_entities: vec![
            TestEntity { id: 0 },
            TestEntity { id: 1 },
            TestEntity { id: 2 },
            TestEntity { id: 3 },
            TestEntity { id: 4 },
        ],
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pagination_more_than_a_page() {
    test_pagination!(TestPaginationOpts {
        total_entity_count: 7,
        start_offset: 0,
        page_limit: 5,
        expected_entities_call_count: 2,
        expected_entities: vec![
            TestEntity { id: 0 },
            TestEntity { id: 1 },
            TestEntity { id: 2 },
            TestEntity { id: 3 },
            TestEntity { id: 4 },
            TestEntity { id: 5 },
            TestEntity { id: 6 },
        ],
    });
}

#[tokio::test]
async fn test_pagination_fits_on_few_pages() {
    test_pagination!(TestPaginationOpts {
        total_entity_count: 10,
        start_offset: 0,
        page_limit: 5,
        expected_entities_call_count: 2,
        expected_entities: vec![
            TestEntity { id: 0 },
            TestEntity { id: 1 },
            TestEntity { id: 2 },
            TestEntity { id: 3 },
            TestEntity { id: 4 },
            TestEntity { id: 5 },
            TestEntity { id: 6 },
            TestEntity { id: 7 },
            TestEntity { id: 8 },
            TestEntity { id: 9 },
        ],
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pagination_start_offset_in_the_page_middle() {
    test_pagination!(TestPaginationOpts {
        total_entity_count: 10,
        start_offset: 5,
        page_limit: 10,
        expected_entities_call_count: 1,
        expected_entities: vec![
            TestEntity { id: 5 },
            TestEntity { id: 6 },
            TestEntity { id: 7 },
            TestEntity { id: 8 },
            TestEntity { id: 9 },
        ],
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_pagination_start_offset_is_greater_than_the_total_entity_count() {
    test_pagination!(TestPaginationOpts {
        total_entity_count: 10,
        start_offset: 11,
        page_limit: 10,
        expected_entities_call_count: 1,
        expected_entities: vec![],
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn entity_source(
    total_entities_count: usize,
    expected_entities_call_count: usize,
) -> Arc<MockEntitySource> {
    let mut entity_source = MockEntitySource::new();

    entity_source
        .expect_init_arguments()
        .times(1)
        .returning(|| NoArgs);

    entity_source
        .expect_entities()
        .times(expected_entities_call_count)
        .returning(move |pagination| {
            let result = (0..)
                .skip(pagination.offset)
                .take(pagination.safe_limit(total_entities_count))
                .map(|id| TestEntity { id })
                .collect::<Vec<_>>();

            EntityListing {
                list: result,
                total_count: total_entities_count,
            }
        });

    Arc::new(entity_source)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct NoArgs;

#[derive(Debug, PartialEq)]
struct TestEntity {
    id: usize,
}

#[async_trait::async_trait]
trait EntitySource {
    async fn init_arguments(&self) -> NoArgs;

    async fn entities(&self, pagination: PaginationOpts) -> EntityListing<TestEntity>;
}

mockall::mock! {
    pub EntitySource {}

    #[async_trait::async_trait]
    impl EntitySource for EntitySource {
        async fn init_arguments(&self) -> NoArgs;

        async fn entities(&self, pagination: PaginationOpts) -> EntityListing<TestEntity>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
