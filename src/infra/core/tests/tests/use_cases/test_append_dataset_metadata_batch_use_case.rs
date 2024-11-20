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

use chrono::Utc;
use dill::{Catalog, Component};
use kamu::testing::MetadataFactory;
use kamu::{
    AppendDatasetMetadataBatchUseCaseImpl,
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    AppendDatasetMetadataBatchUseCase,
    CreateDatasetResult,
    DatasetLifecycleMessage,
    DatasetRepository,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::{
    DatasetAlias,
    DatasetKind,
    DatasetName,
    MetadataBlock,
    MetadataEvent,
    Multicodec,
    Multihash,
};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mock_outbox = MockOutbox::new();

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(mock_outbox);
    let create_result_foo = harness.create_dataset(&alias_foo, DatasetKind::Root).await;

    let foo_dataset = harness
        .dataset_repo
        .get_dataset_by_handle(&create_result_foo.dataset_handle);

    let set_info_block = MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(create_result_foo.head.clone()),
        sequence_number: 1,
        event: MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
    };
    let hash_set_info_block =
        AppendDatasetMetadataBatchUseCaseHarness::hash_from_block(&set_info_block);

    let set_license_block = MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(hash_set_info_block.clone()),
        sequence_number: 2,
        event: MetadataEvent::SetLicense(MetadataFactory::set_license().build()),
    };
    let hash_set_license_block =
        AppendDatasetMetadataBatchUseCaseHarness::hash_from_block(&set_license_block);

    let new_blocks = VecDeque::from([
        (hash_set_info_block, set_info_block),
        (hash_set_license_block, set_license_block),
    ]);

    let res = harness
        .use_case
        .execute(foo_dataset.as_ref(), new_blocks, false)
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch_with_new_dependencies() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let mut mock_outbox = MockOutbox::new();
    AppendDatasetMetadataBatchUseCaseHarness::add_outbox_dataset_dependencies_updated_expectation(
        &mut mock_outbox,
        1,
    );

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(mock_outbox);
    let create_result_foo = harness.create_dataset(&alias_foo, DatasetKind::Root).await;
    let create_result_bar = harness
        .create_dataset(&alias_bar, DatasetKind::Derivative)
        .await;

    let bar_dataset = harness
        .dataset_repo
        .get_dataset_by_handle(&create_result_bar.dataset_handle);

    let set_transform_block = MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(create_result_bar.head.clone()),
        sequence_number: 1,
        event: MetadataEvent::SetTransform(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases(vec![(
                    create_result_foo.dataset_handle.id,
                    alias_foo.to_string(),
                )])
                .build(),
        ),
    };
    let hash_set_transform_block =
        AppendDatasetMetadataBatchUseCaseHarness::hash_from_block(&set_transform_block);

    let new_blocks = VecDeque::from([(hash_set_transform_block, set_transform_block)]);

    let res = harness
        .use_case
        .execute(bar_dataset.as_ref(), new_blocks, false)
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AppendDatasetMetadataBatchUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    use_case: Arc<dyn AppendDatasetMetadataBatchUseCase>,
}

impl AppendDatasetMetadataBatchUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<SystemTimeSourceDefault>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog
            .get_one::<dyn AppendDatasetMetadataBatchUseCase>()
            .unwrap();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        Self {
            _temp_dir: tempdir,
            catalog,
            use_case,
            dataset_repo,
        }
    }

    async fn create_dataset(&self, alias: &DatasetAlias, kind: DatasetKind) -> CreateDatasetResult {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(kind)
            .build();

        let dataset_repo_writer = self
            .catalog
            .get_one::<dyn DatasetRepositoryWriter>()
            .unwrap();

        let result = dataset_repo_writer
            .create_dataset_from_snapshot(snapshot)
            .await
            .unwrap();

        result.create_dataset_result
    }

    fn hash_from_block(block: &MetadataBlock) -> Multihash {
        let block_data = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .unwrap();

        Multihash::from_digest::<sha3::Sha3_256>(Multicodec::Sha3_256, &block_data)
    }

    fn add_outbox_dataset_dependencies_updated_expectation(
        mock_outbox: &mut MockOutbox,
        times: usize,
    ) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                        Ok(DatasetLifecycleMessage::DependenciesUpdated(_))
                    )
                }),
                eq(1),
            )
            .times(times)
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
