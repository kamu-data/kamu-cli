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
use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu_datasets::AppendDatasetMetadataBatchUseCase;
use kamu_datasets_services::{
    AppendDatasetMetadataBatchUseCaseImpl,
    DependencyGraphWriter,
    MockDependencyGraphWriter,
};
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(MockDependencyGraphWriter::new());
    let foo = harness.create_root_dataset(&alias_foo).await;

    let set_info_block = odf::MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(foo.head.clone()),
        sequence_number: 2,
        event: odf::MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
    };
    let hash_set_info_block = BaseRepoHarness::hash_from_block(&set_info_block);

    let set_license_block = odf::MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(hash_set_info_block.clone()),
        sequence_number: 3,
        event: odf::MetadataEvent::SetLicense(MetadataFactory::set_license().build()),
    };
    let hash_set_license_block = BaseRepoHarness::hash_from_block(&set_license_block);

    let new_blocks = VecDeque::from([
        (hash_set_info_block, set_info_block),
        (hash_set_license_block, set_license_block),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch_with_new_dependencies() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let mut mock_dependency_writer = MockDependencyGraphWriter::new();
    mock_dependency_writer
        .expect_update_dataset_node_dependencies()
        .once()
        .returning(|_, _, _| Ok(()));

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(mock_dependency_writer);
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let set_transform_block = odf::MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(bar.head.clone()),
        sequence_number: 2,
        event: odf::MetadataEvent::SetTransform(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases(vec![(foo.dataset_handle.id, alias_foo.to_string())])
                .build(),
        ),
    };
    let hash_set_transform_block = BaseRepoHarness::hash_from_block(&set_transform_block);

    let new_blocks = VecDeque::from([(hash_set_transform_block, set_transform_block)]);

    let res = harness
        .use_case
        .execute(
            bar.dataset.as_ref(),
            Box::new(new_blocks.into_iter()),
            Default::default(),
        )
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct AppendDatasetMetadataBatchUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn AppendDatasetMetadataBatchUseCase>,
}

impl AppendDatasetMetadataBatchUseCaseHarness {
    fn new(mock_dependency_graph_writer: MockDependencyGraphWriter) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let catalog = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog())
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .add_value(mock_dependency_graph_writer)
            .bind::<dyn DependencyGraphWriter, MockDependencyGraphWriter>()
            .build();

        let use_case = catalog
            .get_one::<dyn AppendDatasetMetadataBatchUseCase>()
            .unwrap();

        Self {
            base_use_case_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
