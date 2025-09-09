// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::VecDeque;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use kamu::testing::BaseRepoHarness;
use kamu_core::MockDidGenerator;
use kamu_datasets::AppendDatasetMetadataBatchUseCase;
use kamu_datasets_services::AppendDatasetMetadataBatchUseCaseImpl;
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::assert_eq;
use time_source::SystemTimeSourceStub;

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
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.reset_collected_outbox_messages();

    let foo_old_head = foo.head.clone();

    let set_info_block = odf::MetadataBlock {
        system_time: harness.system_time_source().now(),
        prev_block_hash: Some(foo_old_head.clone()),
        sequence_number: 2,
        event: odf::MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
    };
    let hash_set_info_block = BaseRepoHarness::hash_from_block(&set_info_block);

    let set_license_block = odf::MetadataBlock {
        system_time: harness.system_time_source().now(),
        prev_block_hash: Some(hash_set_info_block.clone()),
        sequence_number: 3,
        event: odf::MetadataEvent::SetLicense(MetadataFactory::set_license().build()),
    };
    let hash_set_license_block = BaseRepoHarness::hash_from_block(&set_license_block);

    let new_blocks = VecDeque::from([
        (hash_set_info_block.clone(), set_info_block),
        (hash_set_license_block.clone(), set_license_block),
    ]);

    let res = harness
        .use_case
        .execute(
            foo.dataset.as_ref(),
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
              Key Blocks Introduced {
                Dataset ID: <foo_id>
                Ref: head
                Key Block Tail: <foo_key_tail>
                Key Block Head: <foo_key_head>
              }
            "#
        )
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<old_head>", foo_old_head.to_string().as_str())
        .replace("<new_head>", hash_set_license_block.to_string().as_str())
        .replace("<foo_id>", predefined_foo_id.to_string().as_str())
        .replace("<foo_key_tail>", hash_set_info_block.to_string().as_str())
        .replace(
            "<foo_key_head>",
            hash_set_license_block.to_string().as_str()
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
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness
        .create_derived_dataset(
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
    let hash_set_transform_block = BaseRepoHarness::hash_from_block(&set_transform_block);

    let new_blocks = VecDeque::from([(hash_set_transform_block.clone(), set_transform_block)]);

    let res = harness
        .use_case
        .execute(
            baz.dataset.as_ref(),
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
              Key Blocks Introduced {
                Dataset ID: <baz_id>
                Ref: head
                Key Block Tail: <foo_key_tail>
                Key Block Head: <foo_key_head>
              }
            "#
        )
        .replace("<old_head>", baz_old_head.to_string().as_str())
        .replace("<new_head>", hash_set_transform_block.to_string().as_str())
        .replace("<baz_id>", baz.dataset_handle.id.to_string().as_str())
        .replace("<baz_id>", baz.dataset_handle.id.to_string().as_str())
        .replace(
            "<foo_key_tail>",
            hash_set_transform_block.to_string().as_str()
        )
        .replace(
            "<foo_key_head>",
            hash_set_transform_block.to_string().as_str()
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
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;
    let baz = harness.create_root_dataset(&alias_baz).await;
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
    let hash_set_transform_block = BaseRepoHarness::hash_from_block(&set_transform_block);

    let new_blocks = VecDeque::from([(hash_set_transform_block.clone(), set_transform_block)]);

    let res = harness
        .use_case
        .execute(
            bar.dataset.as_ref(),
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
              Key Blocks Introduced {
                Dataset ID: <bar_id>
                Ref: head
                Key Block Tail: <key_tail>
                Key Block Head: <key_head>
              }
            "#
        )
        .replace("<old_head>", bar_old_head.to_string().as_str())
        .replace("<new_head>", hash_set_transform_block.to_string().as_str())
        .replace("<foo_id>", foo.dataset_handle.id.to_string().as_str())
        .replace("<bar_id>", bar.dataset_handle.id.to_string().as_str())
        .replace("<baz_id>", baz.dataset_handle.id.to_string().as_str())
        .replace("<bar_id>", bar.dataset_handle.id.to_string().as_str())
        .replace("<key_tail>", hash_set_transform_block.to_string().as_str())
        .replace("<key_head>", hash_set_transform_block.to_string().as_str()),
        harness.collected_outbox_messages(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct AppendDatasetMetadataBatchUseCaseHarness {
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    use_case: Arc<dyn AppendDatasetMetadataBatchUseCase>,
}

impl AppendDatasetMetadataBatchUseCaseHarness {
    async fn new(mock_did_generator: MockDidGenerator) -> Self {
        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                maybe_system_time_source_stub: Some(SystemTimeSourceStub::new_set(
                    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
                )),
                maybe_mock_did_generator: Some(mock_did_generator),
                ..DatasetBaseUseCaseHarnessOpts::default()
            })
            .await;

        let catalog = dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.catalog())
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .build();

        Self {
            dataset_base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
