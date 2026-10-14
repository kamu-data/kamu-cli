// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use kamu_datasets::{
    FileVersion,
    ReadCheckedDataset,
    ResolvedDataset,
    ViewVersionedFileHistoryUseCase,
};
use kamu_datasets_services::ViewVersionedFileHistoryUseCaseImpl;
use pretty_assertions::assert_eq;

use super::versioned_file_base_use_case_harness::VersionedFileBaseUseCaseHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_versioned_file_history_empty() {
    let harness = ViewVersionedFileHistoryUseCaseHarness::new();
    let file = harness.create_versioned_file(Vec::new()).await;

    assert_eq!(harness.history(&file, None, None).await, (Vec::new(), 0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_versioned_file_history_newest_first() {
    let harness = ViewVersionedFileHistoryUseCaseHarness::new();
    let file = harness.create_versioned_file(Vec::new()).await;

    for content in [b"foo".as_slice(), b"bar", b"baz"] {
        harness.upload(&file, content).await;
    }

    assert_eq!(harness.history(&file, None, None).await, (vec![3, 2, 1], 3));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_view_versioned_file_history_max_version_and_pagination() {
    let harness = ViewVersionedFileHistoryUseCaseHarness::new();
    let file = harness.create_versioned_file(Vec::new()).await;

    for content in [b"a".as_slice(), b"b", b"c", b"d"] {
        harness.upload(&file, content).await;
    }

    // Total count is limited by the max version, not by the page
    assert_eq!(
        harness.history(&file, Some(3), None).await,
        (vec![3, 2, 1], 3)
    );
    assert_eq!(
        harness
            .history(
                &file,
                None,
                Some(PaginationOpts {
                    offset: 1,
                    limit: 2,
                })
            )
            .await,
        (vec![3, 2], 4)
    );
    assert_eq!(
        harness
            .history(
                &file,
                Some(3),
                Some(PaginationOpts {
                    offset: 2,
                    limit: 2,
                })
            )
            .await,
        (vec![1], 3)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(VersionedFileBaseUseCaseHarness, versioned_file_base_use_case_harness)]
struct ViewVersionedFileHistoryUseCaseHarness {
    versioned_file_base_use_case_harness: VersionedFileBaseUseCaseHarness,
    view_use_case: Arc<dyn ViewVersionedFileHistoryUseCase>,
}

impl ViewVersionedFileHistoryUseCaseHarness {
    fn new() -> Self {
        let versioned_file_base_use_case_harness = VersionedFileBaseUseCaseHarness::new();

        let catalog =
            dill::CatalogBuilder::new_chained(versioned_file_base_use_case_harness.catalog())
                .add::<ViewVersionedFileHistoryUseCaseImpl>()
                .build();

        Self {
            versioned_file_base_use_case_harness,
            view_use_case: catalog.get_one().unwrap(),
        }
    }

    /// Listed versions and the total count before pagination
    async fn history(
        &self,
        file: &ResolvedDataset,
        max_version: Option<FileVersion>,
        pagination: Option<PaginationOpts>,
    ) -> (Vec<FileVersion>, usize) {
        let page = self
            .view_use_case
            .execute(ReadCheckedDataset::from_ref(file), max_version, pagination)
            .await
            .unwrap();

        (
            page.list.iter().map(|entry| entry.version).collect(),
            page.total_count,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
