// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use kamu_core::MockDidGenerator;
use kamu_datasets::{CreateDatasetUseCase, DatasetReferenceRepository};
use kamu_datasets_services::CreateDatasetUseCaseImpl;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;
use time_source::{SystemTimeSourceHarnessMode, SystemTimeSourceStub};

use super::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateUseCaseHarness::new(predefined_foo_id.clone()).await;

    let foo_created = harness
        .use_case
        .execute(
            &alias_foo,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(odf::DatasetKind::Root)
                    .id(predefined_foo_id.clone())
                    .build(),
            )
            .system_time(harness.system_time_source().now())
            .build_typed(),
            Default::default(),
        )
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));

    assert_eq!(
        harness
            .get_dataset_reference(&predefined_foo_id, &odf::BlockRef::Head)
            .await,
        foo_created.head,
    );

    // Note: the stability of these identifiers is ensured via
    //  predefined dataset ID and stubbed system time
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Lifecycle Messages: 1
              Created {
                Dataset ID: <foo_id>
                Dataset Name: foo
                Owner: did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f
                Visibility: private
              }
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: <foo_id>
                Ref: head
                Prev Head: None
                New Head: Multihash<Sha3_256>(<new_head>)
              }
            Dataset Key Block Messages: 1
              Key Blocks Appended {
                Dataset ID: <foo_id>
                Ref: head
                Key Block Tail: <key_head>
                Key Block Head: <new_head>
              }
            "#
        )
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<new_head>", foo_created.head.to_string().as_str())
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<key_tail>", foo_created.head.to_string().as_str())
        .replace("<key_head>", foo_created.head.to_string().as_str()),
        harness.collected_outbox_messages()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct CreateUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetUseCase>,
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
}

impl CreateUseCaseHarness {
    async fn new(predefined_dataset_id: odf::DatasetID) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                system_time_source_harness_mode: SystemTimeSourceHarnessMode::Stub(
                    SystemTimeSourceStub::new_set(
                        Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                    ),
                ),
                maybe_mock_did_generator: Some(MockDidGenerator::predefined_dataset_ids(vec![
                    predefined_dataset_id,
                ])),
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let mut b =
            dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.intermediate_catalog());

        b.add::<CreateDatasetUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>();

        let catalog = b.build();

        Self {
            dataset_base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            dataset_reference_repo: catalog.get_one().unwrap(),
        }
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> odf::Multihash {
        self.dataset_reference_repo
            .get_dataset_reference(dataset_id, block_ref)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
