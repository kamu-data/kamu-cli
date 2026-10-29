// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use kamu::testing::BaseRepoHarness;
use kamu_core::MockDidGenerator;
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseError,
    AppendDatasetMetadataBatchUseCaseOptions,
    CreateDatasetResult,
    ResolvedDataset,
    SetRefCheckRefMode,
};
use kamu_datasets_services::AppendDatasetMetadataBatchUseCaseImpl;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;
use time_source::{SystemTimeSourceProvider, SystemTimeSourceStub};

use super::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(
        MockDidGenerator::predefined_dataset_ids(vec![predefined_foo_id.clone()]),
    )
    .await;
    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    harness.reset_collected_outbox_messages();

    let foo_old_head = foo.head.clone();

    let set_info_block = odf::MetadataBlock {
        system_time: harness.system_time_source().now(),
        prev_block_hash: Some(foo_old_head.clone()),
        sequence_number: 2,
        event: odf::MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
    };
    let (set_info_block_hash, set_info_block_bytes) =
        BaseRepoHarness::serialize_block(&set_info_block);

    let set_license_block = odf::MetadataBlock {
        system_time: harness.system_time_source().now(),
        prev_block_hash: Some(set_info_block_hash.clone()),
        sequence_number: 3,
        event: odf::MetadataEvent::SetLicense(MetadataFactory::set_license().build()),
    };
    let (set_license_block_hash, set_license_block_bytes) =
        BaseRepoHarness::serialize_block(&set_license_block);

    let new_blocks = VecDeque::from([
        (
            set_info_block_hash.clone(),
            set_info_block,
            set_info_block_bytes,
        ),
        (
            set_license_block_hash.clone(),
            set_license_block,
            set_license_block_bytes,
        ),
    ]);

    let res = harness
        .use_case
        .execute(
            &ResolvedDataset::from_created(&foo),
            Box::new(new_blocks.into_iter()),
            Default::default(),
        )
        .await;
    assert_matches!(res, Ok(_));

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
                Key Block Tail: <foo_key_tail>
                Key Block Head: <foo_key_head>
              }
            "#
        )
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<old_head>", foo_old_head.to_string().as_str())
        .replace("<new_head>", set_license_block_hash.to_string().as_str())
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<foo_key_tail>", set_info_block_hash.to_string().as_str())
        .replace(
            "<foo_key_head>",
            set_license_block_hash.to_string().as_str()
        ),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch_with_same_dependencies() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(
        MockDidGenerator::predefined_dataset_ids(vec![
            odf::DatasetID::new_seeded_ed25519(b"foo"),
            odf::DatasetID::new_seeded_ed25519(b"bar"),
            odf::DatasetID::new_seeded_ed25519(b"baz"),
        ]),
    )
    .await;
    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    let bar = harness
        .create_root_dataset(&harness.catalog, &alias_bar)
        .await;
    let baz = harness
        .create_derived_dataset(
            &harness.catalog,
            &alias_baz,
            vec![
                foo.dataset_handle.as_local_ref(),
                bar.dataset_handle.as_local_ref(),
            ],
        )
        .await;
    harness.reset_collected_outbox_messages();

    let baz_old_head = baz.head.clone();

    let set_transform_block = odf::MetadataBlock {
        system_time: harness.system_time_source().now(),
        prev_block_hash: Some(baz_old_head.clone()),
        sequence_number: 2,
        event: odf::MetadataEvent::SetTransform(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases(vec![
                    (foo.dataset_handle.id.clone(), alias_foo.to_string()),
                    (bar.dataset_handle.id.clone(), alias_bar.to_string()),
                ])
                .build(),
        ),
    };
    let (set_transform_block_hash, set_transform_block_bytes) =
        BaseRepoHarness::serialize_block(&set_transform_block);

    let new_blocks = VecDeque::from([(
        set_transform_block_hash.clone(),
        set_transform_block,
        set_transform_block_bytes,
    )]);

    let res = harness
        .use_case
        .execute(
            &ResolvedDataset::from_created(&baz),
            Box::new(new_blocks.into_iter()),
            Default::default(),
        )
        .await;
    assert_matches!(res, Ok(_));

    // No dependency updates, as they haven't changed
    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: <baz_id>
                Ref: head
                Prev Head: Some(Multihash<Sha3_256>(<old_head>))
                New Head: Multihash<Sha3_256>(<new_head>)
              }
            Dataset Key Block Messages: 1
              Key Blocks Appended {
                Dataset ID: <baz_id>
                Ref: head
                Key Block Tail: <foo_key_tail>
                Key Block Head: <foo_key_head>
              }
            "#
        )
        .replace("<old_head>", baz_old_head.to_string().as_str())
        .replace("<new_head>", set_transform_block_hash.to_string().as_str())
        .replace("<baz_id>", baz.dataset_handle.id.to_string().as_str())
        .replace("<baz_id>", baz.dataset_handle.id.to_string().as_str())
        .replace(
            "<foo_key_tail>",
            set_transform_block_hash.to_string().as_str()
        )
        .replace(
            "<foo_key_head>",
            set_transform_block_hash.to_string().as_str()
        ),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch_with_new_dependencies() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(
        MockDidGenerator::predefined_dataset_ids(vec![
            odf::DatasetID::new_seeded_ed25519(b"foo"),
            odf::DatasetID::new_seeded_ed25519(b"bar"),
            odf::DatasetID::new_seeded_ed25519(b"baz"),
        ]),
    )
    .await;
    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;
    let bar = harness
        .create_derived_dataset(
            &harness.catalog,
            &alias_bar,
            vec![foo.dataset_handle.as_local_ref()],
        )
        .await;
    let baz = harness
        .create_root_dataset(&harness.catalog, &alias_baz)
        .await;
    harness.reset_collected_outbox_messages();

    let bar_old_head = bar.head.clone();

    let set_transform_block = odf::MetadataBlock {
        system_time: harness.system_time_source().now(),
        prev_block_hash: Some(bar_old_head.clone()),
        sequence_number: 2,
        event: odf::MetadataEvent::SetTransform(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases(vec![(
                    baz.dataset_handle.id.clone(),
                    alias_baz.to_string(),
                )])
                .build(),
        ),
    };
    let (set_transform_block_hash, set_transform_block_bytes) =
        BaseRepoHarness::serialize_block(&set_transform_block);

    let new_blocks = VecDeque::from([(
        set_transform_block_hash.clone(),
        set_transform_block,
        set_transform_block_bytes,
    )]);

    let res = harness
        .use_case
        .execute(
            &ResolvedDataset::from_created(&bar),
            Box::new(new_blocks.into_iter()),
            Default::default(),
        )
        .await;
    assert_matches!(res, Ok(_));

    assert_eq!(
        indoc::indoc!(
            r#"
            Dataset Reference Messages: 1
              Ref Updated {
                Dataset ID: <bar_id>
                Ref: head
                Prev Head: Some(Multihash<Sha3_256>(<old_head>))
                New Head: Multihash<Sha3_256>(<new_head>)
              }
            Dataset Dependency Messages: 1
              Deps Updated {
                Dataset ID: <bar_id>
                Added: [<baz_id>]
                Removed: [<foo_id>]
              }
            Dataset Key Block Messages: 1
              Key Blocks Appended {
                Dataset ID: <bar_id>
                Ref: head
                Key Block Tail: <key_tail>
                Key Block Head: <key_head>
              }
            "#
        )
        .replace("<old_head>", bar_old_head.to_string().as_str())
        .replace("<new_head>", set_transform_block_hash.to_string().as_str())
        .replace("<foo_id>", foo.dataset_handle.id.to_string().as_str())
        .replace("<bar_id>", bar.dataset_handle.id.to_string().as_str())
        .replace("<baz_id>", baz.dataset_handle.id.to_string().as_str())
        .replace("<bar_id>", bar.dataset_handle.id.to_string().as_str())
        .replace("<key_tail>", set_transform_block_hash.to_string().as_str())
        .replace("<key_head>", set_transform_block_hash.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch_head_moved_concurrently() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(
        MockDidGenerator::predefined_dataset_ids(vec![predefined_foo_id]),
    )
    .await;
    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;

    let (batch_head, batch) = harness.set_info_batch(&foo, "ours").await;
    let (concurrent_head, concurrent_batch) = harness.set_info_batch(&foo, "concurrent").await;
    harness
        .append_batch(&foo, concurrent_batch, SetRefCheckRefMode::default())
        .await
        .unwrap();

    assert_matches!(
        harness
            .append_batch(&foo, batch, SetRefCheckRefMode::default())
            .await,
        Err(AppendDatasetMetadataBatchUseCaseError::SetRefError(
            odf::dataset::SetChainRefError::CASFailed(odf::dataset::RefCASError {
                expected,
                actual,
                ..
            })
        )) if expected == Some(foo.head.clone()) && actual == Some(concurrent_head.clone())
    );
    assert_ne!(batch_head, concurrent_head);
    assert_eq!(harness.get_head(&foo).await, concurrent_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch_replaces_diverged_history_at_expected_head() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let predefined_foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(
        MockDidGenerator::predefined_dataset_ids(vec![predefined_foo_id]),
    )
    .await;
    let foo = harness
        .create_root_dataset(&harness.catalog, &alias_foo)
        .await;

    let (batch_head, batch) = harness.set_info_batch(&foo, "ours").await;
    let (diverged_head, diverged_batch) = harness.set_info_batch(&foo, "diverged").await;
    harness
        .append_batch(&foo, diverged_batch, SetRefCheckRefMode::default())
        .await
        .unwrap();

    assert_matches!(
        harness
            .append_batch(
                &foo,
                batch,
                SetRefCheckRefMode::Explicit(Some(diverged_head))
            )
            .await,
        Ok(Some(new_head)) if new_head == batch_head
    );
    assert_eq!(harness.get_head(&foo).await, batch_head);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct AppendDatasetMetadataBatchUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn AppendDatasetMetadataBatchUseCase>,
    catalog: dill::Catalog,
}

impl AppendDatasetMetadataBatchUseCaseHarness {
    async fn new(mock_did_generator: MockDidGenerator) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                system_time_source_provider: SystemTimeSourceProvider::Stub(
                    SystemTimeSourceStub::new_set(
                        Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                    ),
                ),
                maybe_mock_did_generator: Some(mock_did_generator),
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let catalog =
            dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.intermediate_catalog())
                .add::<AppendDatasetMetadataBatchUseCaseImpl>()
                .build();

        Self {
            dataset_base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            catalog,
        }
    }

    /// A one-block batch on top of the dataset's initial `HEAD`
    async fn set_info_batch(
        &self,
        created: &CreateDatasetResult,
        description: &str,
    ) -> (
        odf::Multihash,
        VecDeque<odf::dataset::HashedMetadataBlockBytesDecoded>,
    ) {
        let head_block = created
            .dataset
            .as_metadata_chain()
            .get_block(&created.head)
            .await
            .unwrap();

        let block = MetadataFactory::metadata_block(
            MetadataFactory::set_info().description(description).build(),
        )
        .prev(&created.head, head_block.sequence_number)
        .system_time(self.system_time_source().now())
        .build();
        let (hash, bytes) = BaseRepoHarness::serialize_block(&block);

        (hash.clone(), VecDeque::from([(hash, block, bytes)]))
    }

    async fn append_batch(
        &self,
        created: &CreateDatasetResult,
        batch: VecDeque<odf::dataset::HashedMetadataBlockBytesDecoded>,
        set_ref_check_ref_mode: SetRefCheckRefMode,
    ) -> Result<Option<odf::Multihash>, AppendDatasetMetadataBatchUseCaseError> {
        self.use_case
            .execute(
                &ResolvedDataset::from_created(created),
                Box::new(batch.into_iter()),
                AppendDatasetMetadataBatchUseCaseOptions {
                    set_ref_check_ref_mode,
                    ..Default::default()
                },
            )
            .await
    }

    async fn get_head(&self, created: &CreateDatasetResult) -> odf::Multihash {
        created
            .dataset
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
