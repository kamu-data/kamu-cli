// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use kamu_datasets::{
    CollectionEntryRecord,
    CollectionPath,
    FindCollectionEntriesUseCase,
    ReadCheckedDataset,
    ResolvedDataset,
};
use kamu_datasets_services::FindCollectionEntriesUseCaseImpl;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::collection_base_use_case_harness::CollectionBaseUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_collection_entry_in_empty_collection() {
    let harness = FindCollectionEntriesUseCaseHarness::new();
    let [foo, _, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    assert_eq!(harness.find_by_path(&collection, None, "/a").await, None);
    assert_eq!(harness.find_by_ref(&collection, &foo).await, None);
    assert_eq!(
        harness.find_by_refs(&collection, &[&foo], None).await,
        Vec::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_collection_entry_by_path() {
    let harness = FindCollectionEntriesUseCaseHarness::new();
    let [foo, bar, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    let first = harness
        .update_success(
            &collection,
            vec![harness.add("/a", &foo), harness.add("/b", &foo)],
        )
        .await;
    harness
        .update_success(
            &collection,
            vec![harness.add("/a", &bar), harness.remove("/b")],
        )
        .await;

    assert_eq!(
        harness.find_by_path(&collection, None, "/a").await,
        Some(harness.record("/a", &bar))
    );
    assert_eq!(harness.find_by_path(&collection, None, "/b").await, None);
    assert_eq!(harness.find_by_path(&collection, None, "/c").await, None);

    // State as of an earlier block
    assert_eq!(
        harness
            .find_by_path(&collection, Some(first.new_head.clone()), "/a")
            .await,
        Some(harness.record("/a", &foo))
    );
    assert_eq!(
        harness
            .find_by_path(&collection, Some(first.new_head), "/b")
            .await,
        Some(harness.record("/b", &foo))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_collection_entry_by_ref() {
    let harness = FindCollectionEntriesUseCaseHarness::new();
    let [foo, bar, baz] = harness.linked_datasets();
    let collection = harness
        .create_collection(vec![odf::schema::DataField::string("tag").optional()])
        .await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add_with("/a", &foo, json!({"tag": "x"})),
                harness.add("/b", &bar),
                harness.add("/c", &baz),
            ],
        )
        .await;
    harness
        .update_success(
            &collection,
            vec![
                harness.mv_with("/a", "/moved", json!({"tag": "y"})),
                harness.add("/b", &baz),
            ],
        )
        .await;

    // Follows moves and corrections of the entry
    assert_eq!(
        harness.find_by_ref(&collection, &foo).await,
        Some(harness.record_with("/moved", &foo, json!({"tag": "y"})))
    );
    // Replaced at its path by another dataset
    assert_eq!(harness.find_by_ref(&collection, &bar).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_collection_entries_by_multiple_refs() {
    let harness = FindCollectionEntriesUseCaseHarness::new();
    let [foo, bar, baz] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    harness
        .update_success(
            &collection,
            vec![
                harness.add("/c", &foo),
                harness.add("/a", &foo),
                harness.add("/b", &bar),
            ],
        )
        .await;

    // The same dataset can be linked from several paths; results are sorted by path
    assert_eq!(
        harness.find_by_refs(&collection, &[&foo, &bar], None).await,
        vec![
            harness.record("/a", &foo),
            harness.record("/b", &bar),
            harness.record("/c", &foo),
        ]
    );
    assert_eq!(
        harness.find_by_refs(&collection, &[&baz], None).await,
        Vec::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_collection_entries_by_refs_as_of_event_time() {
    let harness = FindCollectionEntriesUseCaseHarness::new();
    let [foo, bar, _] = harness.linked_datasets();
    let collection = harness.create_collection(Vec::new()).await;

    let t1 = Utc.with_ymd_and_hms(2050, 1, 1, 0, 0, 0).unwrap();
    let t2 = Utc.with_ymd_and_hms(2050, 1, 2, 0, 0, 0).unwrap();

    harness
        .update_success_at(&collection, Some(t1), vec![harness.add("/a", &foo)])
        .await;
    harness
        .update_success_at(
            &collection,
            Some(t2),
            vec![harness.mv("/a", "/b"), harness.add("/c", &bar)],
        )
        .await;

    assert_eq!(
        harness
            .find_by_refs(&collection, &[&foo, &bar], Some(t1))
            .await,
        vec![harness.record("/a", &foo)]
    );
    assert_eq!(
        harness
            .find_by_refs(&collection, &[&foo, &bar], Some(t2))
            .await,
        vec![harness.record("/b", &foo), harness.record("/c", &bar)]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(CollectionBaseUseCaseHarness, collection_base_use_case_harness)]
struct FindCollectionEntriesUseCaseHarness {
    collection_base_use_case_harness: CollectionBaseUseCaseHarness,
    find_use_case: Arc<dyn FindCollectionEntriesUseCase>,
}

impl FindCollectionEntriesUseCaseHarness {
    fn new() -> Self {
        let collection_base_use_case_harness = CollectionBaseUseCaseHarness::new();

        let catalog = dill::CatalogBuilder::new_chained(collection_base_use_case_harness.catalog())
            .add::<FindCollectionEntriesUseCaseImpl>()
            .build();

        Self {
            collection_base_use_case_harness,
            find_use_case: catalog.get_one().unwrap(),
        }
    }

    async fn find_by_path(
        &self,
        collection: &ResolvedDataset,
        as_of: Option<odf::Multihash>,
        path: &str,
    ) -> Option<CollectionEntryRecord> {
        self.find_use_case
            .execute_find_by_path(
                ReadCheckedDataset::from_ref(collection),
                as_of,
                CollectionPath::new(path.to_string()),
            )
            .await
            .unwrap()
            .map(Into::into)
    }

    async fn find_by_ref(
        &self,
        collection: &ResolvedDataset,
        reference: &odf::DatasetID,
    ) -> Option<CollectionEntryRecord> {
        self.find_use_case
            .execute_find_by_ref(ReadCheckedDataset::from_ref(collection), None, reference)
            .await
            .unwrap()
            .map(Into::into)
    }

    async fn find_by_refs(
        &self,
        collection: &ResolvedDataset,
        refs: &[&odf::DatasetID],
        as_of_event_time: Option<DateTime<Utc>>,
    ) -> Vec<CollectionEntryRecord> {
        self.find_use_case
            .execute_find_multi_by_refs(
                ReadCheckedDataset::from_ref(collection),
                None,
                refs,
                as_of_event_time,
            )
            .await
            .unwrap()
            .into_iter()
            .map(Into::into)
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
