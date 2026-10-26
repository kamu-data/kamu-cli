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

use chrono::{TimeZone, Utc};
use kamu_core::MockDidGenerator;
use kamu_datasets::{
    CreateDatasetResult,
    DatasetHeadCommitter,
    DatasetReferenceRepository,
    ResolvedDataset,
};
use pretty_assertions::assert_eq;
use time_source::{SystemTimeSourceProvider, SystemTimeSourceStub};

use crate::tests::use_cases::datasets::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_head_advances_from_expected_head() {
    let harness = DatasetHeadCommitterHarness::new().await;
    let foo = harness.create_foo().await;
    let new_head = harness
        .append_unreferenced_block(&foo, &foo.head, "next")
        .await;

    let res = harness.commit_head(&foo, Some(&foo.head), &new_head).await;
    assert_matches!(res, Ok(()));

    assert_eq!(harness.stored_head(&foo).await, new_head);
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: <foo_id>
                Ref: head
                Prev Head: Some(Multihash<Sha3_256>(<old_head>))
                New Head: Multihash<Sha3_256>(<new_head>)
              }
            Dataset Key Block Messages: 1
              Key Blocks Appended {
                Dataset ID: <foo_id>
                Ref: head
                Key Block Tail: <new_head>
                Key Block Head: <new_head>
              }
            "#
        )
        .replace("<foo_id>", foo.dataset_handle.id.to_string().as_str())
        .replace("<old_head>", foo.head.to_string().as_str())
        .replace("<new_head>", new_head.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_head_from_stale_head_is_a_conflict() {
    let harness = DatasetHeadCommitterHarness::new().await;
    let foo = harness.create_foo().await;
    let concurrent_head = harness
        .append_unreferenced_block(&foo, &foo.head, "concurrent")
        .await;
    let new_head = harness
        .append_unreferenced_block(&foo, &foo.head, "planned")
        .await;

    harness
        .commit_head(&foo, Some(&foo.head), &concurrent_head)
        .await
        .unwrap();
    harness.reset_collected_outbox_messages();

    let res = harness.commit_head(&foo, Some(&foo.head), &new_head).await;
    assert_matches!(
        res,
        Err(odf::dataset::SetChainRefError::CASFailed(e))
        if e.reference == odf::BlockRef::Head &&
           e.expected == Some(foo.head.clone()) &&
           e.actual == Some(concurrent_head.clone())
    );

    assert_eq!(harness.stored_head(&foo).await, concurrent_head);
    assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_head_expecting_no_head_is_a_conflict_when_head_exists() {
    let harness = DatasetHeadCommitterHarness::new().await;
    let foo = harness.create_foo().await;
    let new_head = harness
        .append_unreferenced_block(&foo, &foo.head, "next")
        .await;

    let res = harness.commit_head(&foo, None, &new_head).await;
    assert_matches!(
        res,
        Err(odf::dataset::SetChainRefError::CASFailed(e))
        if e.reference == odf::BlockRef::Head &&
           e.expected.is_none() &&
           e.actual == Some(foo.head.clone())
    );

    assert_eq!(harness.stored_head(&foo).await, foo.head);
    assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_commit_head_to_missing_block_fails() {
    let harness = DatasetHeadCommitterHarness::new().await;
    let foo = harness.create_foo().await;
    let missing_block_hash = odf::Multihash::from_digest_sha3_256(b"missing");

    let res = harness
        .commit_head(&foo, Some(&foo.head), &missing_block_hash)
        .await;
    assert_matches!(res, Err(odf::dataset::SetChainRefError::BlockNotFound(_)));

    assert_eq!(harness.stored_head(&foo).await, foo.head);
    assert_eq!("", harness.collected_outbox_messages());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct DatasetHeadCommitterHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    catalog: dill::Catalog,
    committer: Arc<dyn DatasetHeadCommitter>,
    dataset_ref_repo: Arc<dyn DatasetReferenceRepository>,
}

impl DatasetHeadCommitterHarness {
    async fn new() -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                system_time_source_provider: SystemTimeSourceProvider::Stub(
                    SystemTimeSourceStub::new_set(
                        Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                    ),
                ),
                maybe_mock_did_generator: Some(MockDidGenerator::predefined_dataset_ids(vec![
                    odf::DatasetID::new_seeded_ed25519(b"foo"),
                ])),
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let catalog = dataset_base_use_case_harness.intermediate_catalog().clone();

        Self {
            dataset_base_use_case_harness,
            committer: catalog.get_one().unwrap(),
            dataset_ref_repo: catalog.get_one().unwrap(),
            catalog,
        }
    }

    /// Creates the root dataset `foo`, with no outbox messages left collected.
    async fn create_foo(&self) -> CreateDatasetResult {
        let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
        let foo = self.create_root_dataset(&self.catalog, &alias_foo).await;
        self.reset_collected_outbox_messages();
        foo
    }

    async fn commit_head(
        &self,
        dataset: &CreateDatasetResult,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        self.committer
            .commit_head(
                &ResolvedDataset::from_created(dataset),
                expected_head,
                new_head,
            )
            .await
    }

    async fn stored_head(&self, dataset: &CreateDatasetResult) -> odf::Multihash {
        self.dataset_ref_repo
            .get_dataset_reference(&dataset.dataset_handle.id, &odf::BlockRef::Head)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
