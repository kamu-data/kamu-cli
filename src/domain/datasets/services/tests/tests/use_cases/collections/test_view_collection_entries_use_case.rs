// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use database_common::PaginationOpts;
use kamu_datasets::{
    COLLECTION_ENTRIES_MAX_DEPTH_LIMIT,
    CollectionEntryRecord,
    CollectionPath,
    ExtraDataFieldFilter,
    ReadCheckedDataset,
    ResolvedDataset,
    ViewCollectionEntriesError,
    ViewCollectionEntriesUseCase,
};
use kamu_datasets_services::ViewCollectionEntriesUseCaseImpl;
use nonempty::NonEmpty;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::collection_base_use_case_harness::CollectionBaseUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_empty() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let collection = harness.create_collection(Vec::new()).await;

    assert_eq!(
        harness.view(&collection, ViewOpts::default()).await,
        (Vec::new(), 0)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_current_state_sorted_by_path() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let [foo, bar, baz] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add("/c", &foo),
                harness.add("/a", &bar),
                harness.add("/b", &baz),
            ],
        )
        .await;
    harness
        .update_success(
            &collection,
            vec![
                harness.add("/a", &foo),
                harness.mv("/b", "/d"),
                harness.remove("/c"),
            ],
        )
        .await;

    assert_eq!(
        harness.view(&collection, ViewOpts::default()).await,
        (
            vec![harness.record("/a", &foo), harness.record("/d", &baz)],
            2
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_as_of_block() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let [foo, bar, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    let first = harness
        .update_success(&collection, vec![harness.add("/a", &foo)])
        .await;
    harness
        .update_success(
            &collection,
            vec![harness.add("/a", &bar), harness.add("/b", &bar)],
        )
        .await;

    assert_eq!(
        harness
            .view(
                &collection,
                ViewOpts {
                    as_of: Some(first.new_head),
                    ..Default::default()
                }
            )
            .await,
        (vec![harness.record("/a", &foo)], 1)
    );
    assert_eq!(
        harness.view(&collection, ViewOpts::default()).await,
        (
            vec![harness.record("/a", &bar), harness.record("/b", &bar)],
            2
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_path_prefix() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add("/0a", &foo),
                harness.add("/0b/1a", &foo),
                harness.add("/0b/1b/2a", &foo),
                harness.add("/0b/moved", &foo),
            ],
        )
        .await;
    harness
        .update_success(&collection, vec![harness.mv("/0b/moved", "/0c/moved")])
        .await;

    assert_eq!(
        harness.view_paths(&collection, Some("/0b/"), None).await,
        ["/0b/1a", "/0b/1b/2a"]
    );
    assert_eq!(
        harness.view_paths(&collection, Some("/0c/"), None).await,
        ["/0c/moved"]
    );
    assert_eq!(
        harness
            .view_paths(&collection, Some("/0b/1b/2a/"), None)
            .await,
        Vec::<String>::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_max_depth() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add("/0a", &foo),
                harness.add("/0b/1a", &foo),
                harness.add("/0b/1b/2a", &foo),
                harness.add("/0b/1b/2b", &foo),
                harness.add("/0b/1c/2a", &foo),
            ],
        )
        .await;

    // Each directory deeper than `max_depth` is represented by its first entry
    assert_eq!(
        harness.view_paths(&collection, None, Some(1)).await,
        ["/0a", "/0b/1a"]
    );
    assert_eq!(
        harness.view_paths(&collection, None, Some(2)).await,
        ["/0a", "/0b/1a", "/0b/1b/2a", "/0b/1c/2a"]
    );
    assert_eq!(
        harness.view_paths(&collection, Some("/0b/"), Some(2)).await,
        ["/0b/1a", "/0b/1b/2a", "/0b/1c/2a"]
    );

    let res = harness
        .view_res(
            &collection,
            ViewOpts {
                max_depth: Some(COLLECTION_ENTRIES_MAX_DEPTH_LIMIT + 1),
                ..Default::default()
            },
        )
        .await;
    assert_matches!(res, Err(ViewCollectionEntriesError::MaxDepthTooLarge(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_pagination() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            ["/e", "/d", "/c", "/b", "/a"]
                .into_iter()
                .map(|path| harness.add(path, &foo))
                .collect(),
        )
        .await;

    assert_eq!(
        harness
            .view(
                &collection,
                ViewOpts {
                    pagination: Some(PaginationOpts {
                        offset: 1,
                        limit: 2,
                    }),
                    ..Default::default()
                }
            )
            .await,
        (
            vec![harness.record("/b", &foo), harness.record("/c", &foo)],
            5
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_extra_data_filter() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness
        .create_collection(vec![
            odf::schema::DataField::string("tag").optional(),
            odf::schema::DataField::list("labels", odf::schema::DataType::string()).optional(),
        ])
        .await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add_with("/a", &foo, json!({"tag": "x", "labels": ["l1", "l2"]})),
                harness.add_with("/b", &foo, json!({"tag": "y", "labels": ["l2"]})),
                harness.add_with("/c", &foo, json!({"tag": "z", "labels": ["l3"]})),
            ],
        )
        .await;

    assert_eq!(
        harness
            .view_filtered_paths(&collection, vec![("tag", vec!["x", "y"], false)])
            .await,
        ["/a", "/b"]
    );
    assert_eq!(
        harness
            .view_filtered_paths(&collection, vec![("labels", vec!["l1", "l3"], true)])
            .await,
        ["/a", "/c"]
    );
    // Filters on different fields are combined with AND
    assert_eq!(
        harness
            .view_filtered_paths(
                &collection,
                vec![("tag", vec!["x", "y"], false), ("labels", vec!["l1"], true)]
            )
            .await,
        ["/a"]
    );

    let res = harness
        .view_res(
            &collection,
            ViewOpts {
                filter: vec![("unknown", vec!["x"], false)],
                ..Default::default()
            },
        )
        .await;
    assert_matches!(
        res,
        Err(ViewCollectionEntriesError::UnknownExtraDataFieldFilterNames(e))
            if e.field_names == ["unknown"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_collection_extra_data_filter_sees_only_current_values() {
    let harness = ViewCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness
        .create_collection(vec![odf::schema::DataField::string("tag").optional()])
        .await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add_with("/a", &foo, json!({"tag": "x"})),
                harness.add_with("/b", &foo, json!({"tag": "x"})),
            ],
        )
        .await;
    harness
        .update_success(
            &collection,
            vec![
                harness.mv_with("/a", "/a", json!({"tag": "y"})),
                harness.remove("/b"),
            ],
        )
        .await;

    assert_eq!(
        harness
            .view_filtered_paths(&collection, vec![("tag", vec!["x"], false)])
            .await,
        Vec::<String>::new()
    );
    assert_eq!(
        harness
            .view_filtered_paths(&collection, vec![("tag", vec!["y"], false)])
            .await,
        ["/a"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct ViewOpts {
    as_of: Option<odf::Multihash>,
    path_prefix: Option<&'static str>,
    max_depth: Option<usize>,
    /// `(field, values, is_array)`; empty means no filter
    filter: Vec<(&'static str, Vec<&'static str>, bool)>,
    pagination: Option<PaginationOpts>,
}

#[oop::extend(CollectionBaseUseCaseHarness, collection_base_use_case_harness)]
struct ViewCollectionEntriesUseCaseHarness {
    collection_base_use_case_harness: CollectionBaseUseCaseHarness,
    view_use_case: Arc<dyn ViewCollectionEntriesUseCase>,
}

impl ViewCollectionEntriesUseCaseHarness {
    fn new() -> Self {
        let collection_base_use_case_harness = CollectionBaseUseCaseHarness::new();

        let catalog = dill::CatalogBuilder::new_chained(collection_base_use_case_harness.catalog())
            .add::<ViewCollectionEntriesUseCaseImpl>()
            .build();

        Self {
            collection_base_use_case_harness,
            view_use_case: catalog.get_one().unwrap(),
        }
    }

    async fn view_res(
        &self,
        collection: &ResolvedDataset,
        opts: ViewOpts,
    ) -> Result<(Vec<CollectionEntryRecord>, usize), ViewCollectionEntriesError> {
        let filter = NonEmpty::from_vec(
            opts.filter
                .into_iter()
                .map(|(field_name, values, is_array)| ExtraDataFieldFilter {
                    field_name: field_name.to_string(),
                    values: NonEmpty::from_vec(values.into_iter().map(String::from).collect())
                        .unwrap(),
                    is_array,
                })
                .collect(),
        );

        let listing = self
            .view_use_case
            .execute(
                ReadCheckedDataset::from_ref(collection),
                opts.as_of,
                opts.path_prefix
                    .map(|prefix| CollectionPath::new(prefix.to_string())),
                opts.max_depth,
                filter,
                opts.pagination,
            )
            .await?;

        Ok((
            listing.list.into_iter().map(Into::into).collect(),
            listing.total_count,
        ))
    }

    /// Listed entries and the total count before pagination
    async fn view(
        &self,
        collection: &ResolvedDataset,
        opts: ViewOpts,
    ) -> (Vec<CollectionEntryRecord>, usize) {
        self.view_res(collection, opts).await.unwrap()
    }

    async fn view_paths(
        &self,
        collection: &ResolvedDataset,
        path_prefix: Option<&'static str>,
        max_depth: Option<usize>,
    ) -> Vec<String> {
        Self::paths(
            self.view(
                collection,
                ViewOpts {
                    path_prefix,
                    max_depth,
                    ..Default::default()
                },
            )
            .await,
        )
    }

    async fn view_filtered_paths(
        &self,
        collection: &ResolvedDataset,
        filter: Vec<(&'static str, Vec<&'static str>, bool)>,
    ) -> Vec<String> {
        Self::paths(
            self.view(
                collection,
                ViewOpts {
                    filter,
                    ..Default::default()
                },
            )
            .await,
        )
    }

    fn paths((entries, _): (Vec<CollectionEntryRecord>, usize)) -> Vec<String> {
        entries
            .into_iter()
            .map(|entry| entry.path.into_inner())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
