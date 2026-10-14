// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_datasets::{
    CollectionEntry,
    CollectionEntryRecord,
    CollectionPath,
    CollectionUpdateOperation,
    DatasetSnapshots,
    ResolvedDataset,
    UpdateCollectionEntriesResult,
    UpdateCollectionEntriesSuccess,
    UpdateCollectionEntriesUseCase,
    UpdateCollectionEntriesUseCaseError,
    WriteCheckedDataset,
};
use kamu_datasets_services::UpdateCollectionEntriesUseCaseImpl;
use odf::metadata::OperationType as Op;

use crate::tests::use_cases::push_source_dataset_base_use_case_harness::PushSourceDatasetBaseUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Collection datasets populated through [`UpdateCollectionEntriesUseCase`], with
/// direct access to their changelog and projected state for assertions
#[oop::extend(
    PushSourceDatasetBaseUseCaseHarness,
    push_source_dataset_base_use_case_harness
)]
pub struct CollectionBaseUseCaseHarness {
    push_source_dataset_base_use_case_harness: PushSourceDatasetBaseUseCaseHarness,
    catalog: dill::Catalog,
    update_use_case: Arc<dyn UpdateCollectionEntriesUseCase>,
}

impl CollectionBaseUseCaseHarness {
    pub fn new() -> Self {
        let push_source_dataset_base_use_case_harness = PushSourceDatasetBaseUseCaseHarness::new();

        let catalog =
            dill::CatalogBuilder::new_chained(push_source_dataset_base_use_case_harness.catalog())
                .add::<UpdateCollectionEntriesUseCaseImpl>()
                .build();

        Self {
            push_source_dataset_base_use_case_harness,
            update_use_case: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    /// Datasets the entries link to; the use cases do not require them to exist
    pub fn linked_datasets(&self) -> [odf::DatasetID; 3] {
        [b"foo", b"bar", b"baz"].map(|seed| odf::DatasetID::new_seeded_ed25519(seed))
    }

    pub fn add(&self, path: &str, reference: &odf::DatasetID) -> CollectionUpdateOperation {
        self.add_with(path, reference, serde_json::json!({}))
    }

    pub fn add_with(
        &self,
        path: &str,
        reference: &odf::DatasetID,
        extra_data: serde_json::Value,
    ) -> CollectionUpdateOperation {
        CollectionUpdateOperation::add(
            CollectionPath::new(path.to_string()),
            reference.clone(),
            PushSourceDatasetBaseUseCaseHarness::extra_data(extra_data),
        )
    }

    pub fn mv(&self, path_from: &str, path_to: &str) -> CollectionUpdateOperation {
        CollectionUpdateOperation::r#move(
            CollectionPath::new(path_from.to_string()),
            CollectionPath::new(path_to.to_string()),
            None,
        )
    }

    pub fn mv_with(
        &self,
        path_from: &str,
        path_to: &str,
        extra_data: serde_json::Value,
    ) -> CollectionUpdateOperation {
        CollectionUpdateOperation::r#move(
            CollectionPath::new(path_from.to_string()),
            CollectionPath::new(path_to.to_string()),
            Some(PushSourceDatasetBaseUseCaseHarness::extra_data(extra_data)),
        )
    }

    pub fn remove(&self, path: &str) -> CollectionUpdateOperation {
        CollectionUpdateOperation::remove(CollectionPath::new(path.to_string()))
    }

    pub fn record(&self, path: &str, reference: &odf::DatasetID) -> CollectionEntryRecord {
        self.record_with(path, reference, serde_json::json!({}))
    }

    pub fn record_with(
        &self,
        path: &str,
        reference: &odf::DatasetID,
        extra_data: serde_json::Value,
    ) -> CollectionEntryRecord {
        CollectionEntryRecord {
            path: CollectionPath::new(path.to_string()),
            reference: reference.clone(),
            extra_data: PushSourceDatasetBaseUseCaseHarness::extra_data(extra_data),
        }
    }

    pub async fn create_collection(
        &self,
        extra_columns: Vec<odf::schema::DataField>,
    ) -> ResolvedDataset {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("collection"));
        self.create_dataset(DatasetSnapshots::collection(alias, extra_columns, Vec::new()).unwrap())
            .await
    }

    pub async fn update(
        &self,
        collection: &ResolvedDataset,
        source_event_time: Option<DateTime<Utc>>,
        operations: Vec<CollectionUpdateOperation>,
        expected_head: Option<odf::Multihash>,
    ) -> Result<UpdateCollectionEntriesResult, UpdateCollectionEntriesUseCaseError> {
        self.update_use_case
            .execute(
                WriteCheckedDataset::from_ref(collection),
                source_event_time,
                operations,
                expected_head,
            )
            .await
    }

    pub async fn update_success(
        &self,
        collection: &ResolvedDataset,
        operations: Vec<CollectionUpdateOperation>,
    ) -> UpdateCollectionEntriesSuccess {
        self.update_success_at(collection, None, operations).await
    }

    pub async fn update_success_at(
        &self,
        collection: &ResolvedDataset,
        source_event_time: Option<DateTime<Utc>>,
        operations: Vec<CollectionUpdateOperation>,
    ) -> UpdateCollectionEntriesSuccess {
        let res = self
            .update(collection, source_event_time, operations, None)
            .await
            .unwrap();
        let UpdateCollectionEntriesResult::Success(success) = res else {
            panic!("Expected a successful update, got: {res:?}");
        };
        success
    }

    /// Raw changelog records as `(offset, op, path, ref)`, ordered by offset
    pub async fn ledger(
        &self,
        collection: &ResolvedDataset,
    ) -> Vec<(u64, Op, String, odf::DatasetID)> {
        let mut ledger: Vec<_> = self
            .records(collection, None)
            .await
            .into_iter()
            .map(|record| {
                (
                    record["offset"].as_u64().unwrap(),
                    Op::try_from(u8::try_from(record["op"].as_u64().unwrap()).unwrap()).unwrap(),
                    record["path"].as_str().unwrap().to_string(),
                    odf::DatasetID::from_did_str(record["ref"].as_str().unwrap()).unwrap(),
                )
            })
            .collect();

        ledger.sort_by_key(|(offset, ..)| *offset);
        ledger
    }

    /// Current entries projected from the whole changelog, ordered by path
    pub async fn entries(&self, collection: &ResolvedDataset) -> Vec<CollectionEntryRecord> {
        let mut entries: Vec<CollectionEntryRecord> = self
            .records(collection, Some(&["path"]))
            .await
            .into_iter()
            .map(|record| CollectionEntry::from_json(record).unwrap().into())
            .collect();

        entries.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
        entries
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
