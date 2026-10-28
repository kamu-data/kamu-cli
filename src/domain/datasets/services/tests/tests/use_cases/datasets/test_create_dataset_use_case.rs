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
    CreateDatasetError,
    CreateDatasetResult,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetReferenceRepository,
    ResolvedDataset,
};
use kamu_datasets_services::CreateDatasetUseCaseImpl;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;
use time_source::{SystemTimeSourceProvider, SystemTimeSourceStub};

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
                Owner: did:odf:fed01f9a23c901fda61bf94b6105a09f4d05764e333193ea78dedc392486e6154be62
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

#[tokio::test]
async fn test_create_dataset_head_already_set() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateUseCaseHarness::new(predefined_foo_id.clone()).await;
    let foo = harness.create_root(&alias_foo, &predefined_foo_id).await;

    assert_matches!(
        harness.set_created_head(&foo).await,
        Err(CreateDatasetError::CASFailed(e))
            if e.expected_prev_block_hash.is_none()
                && e.actual_prev_block_hash == Some(foo.head.clone())
    );
    assert_eq!(
        harness
            .get_dataset_reference(&predefined_foo_id, &odf::BlockRef::Head)
            .await,
        foo.head,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_at_initial_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = CreateUseCaseHarness::new(predefined_foo_id.clone()).await;
    let (seed_block, stored_head) = harness
        .store_dataset_with_unreferenced_block(&predefined_foo_id)
        .await;

    let foo_created = harness
        .use_case
        .execute(
            &alias_foo,
            seed_block,
            CreateDatasetUseCaseOptions {
                initial_head: Some(stored_head.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(foo_created.head, stored_head);
    assert_eq!(
        harness
            .get_dataset_reference(&predefined_foo_id, &odf::BlockRef::Head)
            .await,
        stored_head,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct CreateUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn CreateDatasetUseCase>,
    create_helper: Arc<CreateDatasetUseCaseHelper>,
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
}

impl CreateUseCaseHarness {
    async fn new(predefined_dataset_id: odf::DatasetID) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                system_time_source_provider: SystemTimeSourceProvider::Stub(
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
            create_helper: catalog.get_one().unwrap(),
            dataset_reference_repo: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
        }
    }

    async fn create_root(
        &self,
        alias: &odf::DatasetAlias,
        dataset_id: &odf::DatasetID,
    ) -> CreateDatasetResult {
        self.use_case
            .execute(
                alias,
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root)
                        .id(dataset_id.clone())
                        .build(),
                )
                .system_time(self.system_time_source().now())
                .build_typed(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    /// Stores a dataset in storage only, as a pull transfer does: its `Seed`
    /// and one more block, with no `HEAD` and no entry
    async fn store_dataset_with_unreferenced_block(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> (odf::MetadataBlockTyped<odf::metadata::Seed>, odf::Multihash) {
        let seed_block = MetadataFactory::metadata_block(
            MetadataFactory::seed(odf::DatasetKind::Root)
                .id(dataset_id.clone())
                .build(),
        )
        .system_time(self.system_time_source().now())
        .build_typed();

        let stored = self
            .dataset_storage_unit_writer
            .store_dataset(seed_block.clone(), Default::default())
            .await
            .unwrap();

        let head = stored
            .dataset
            .as_metadata_chain()
            .append(
                MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
                    .prev(&stored.seed, 0)
                    .system_time(self.system_time_source().now())
                    .build(),
                odf::dataset::AppendOpts {
                    update_ref: None,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        (seed_block, head)
    }

    /// Sets the first `HEAD` of a dataset again, as a concurrent creation would
    async fn set_created_head(
        &self,
        dataset: &CreateDatasetResult,
    ) -> Result<(), CreateDatasetError> {
        self.create_helper
            .set_created_head(ResolvedDataset::from_created(dataset), &dataset.head)
            .await
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
