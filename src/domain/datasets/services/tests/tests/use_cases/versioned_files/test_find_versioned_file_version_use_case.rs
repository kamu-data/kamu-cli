// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_datasets::{
    FileVersion,
    FindVersionedFileVersionUseCase,
    ReadCheckedDataset,
    ResolvedDataset,
};
use kamu_datasets_services::FindVersionedFileVersionUseCaseImpl;
use pretty_assertions::assert_eq;
use serde_json::json;

use super::versioned_file_base_use_case_harness::VersionedFileBaseUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_versioned_file_version_in_empty_file() {
    let harness = FindVersionedFileVersionUseCaseHarness::new();
    let file = harness.create_versioned_file(Vec::new()).await;

    assert_eq!(harness.find(&file, None, None).await, None);
    assert_eq!(harness.find(&file, Some(1), None).await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_versioned_file_version() {
    let harness = FindVersionedFileVersionUseCaseHarness::new();
    let file = harness
        .create_versioned_file(vec![odf::schema::DataField::string("tag").optional()])
        .await;

    let first = harness.upload(&file, b"foo").await;
    harness.upload(&file, b"bar").await;
    harness.update_extra_data(&file, json!({"tag": "x"})).await;

    let bar = VersionedFileBaseUseCaseHarness::content_hash(b"bar");
    let foo = VersionedFileBaseUseCaseHarness::content_hash(b"foo");

    // Latest
    assert_eq!(
        harness.find(&file, None, None).await,
        Some((3, bar.clone(), json!({"tag": "x"})))
    );
    // By version
    assert_eq!(
        harness.find(&file, Some(2), None).await,
        Some((2, bar, json!({"tag": null})))
    );
    assert_eq!(harness.find(&file, Some(4), None).await, None);
    // Latest as of a block
    assert_eq!(
        harness.find(&file, None, Some(first.new_head)).await,
        Some((1, foo, json!({"tag": null})))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(VersionedFileBaseUseCaseHarness, versioned_file_base_use_case_harness)]
struct FindVersionedFileVersionUseCaseHarness {
    versioned_file_base_use_case_harness: VersionedFileBaseUseCaseHarness,
    find_use_case: Arc<dyn FindVersionedFileVersionUseCase>,
}

impl FindVersionedFileVersionUseCaseHarness {
    fn new() -> Self {
        let versioned_file_base_use_case_harness = VersionedFileBaseUseCaseHarness::new();

        let catalog =
            dill::CatalogBuilder::new_chained(versioned_file_base_use_case_harness.catalog())
                .add::<FindVersionedFileVersionUseCaseImpl>()
                .build();

        Self {
            versioned_file_base_use_case_harness,
            find_use_case: catalog.get_one().unwrap(),
        }
    }

    async fn find(
        &self,
        file: &ResolvedDataset,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<odf::Multihash>,
    ) -> Option<(FileVersion, odf::Multihash, serde_json::Value)> {
        self.find_use_case
            .execute(
                ReadCheckedDataset::from_ref(file),
                as_of_version,
                as_of_block_hash,
            )
            .await
            .unwrap()
            .as_ref()
            .map(VersionedFileBaseUseCaseHarness::summary)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
