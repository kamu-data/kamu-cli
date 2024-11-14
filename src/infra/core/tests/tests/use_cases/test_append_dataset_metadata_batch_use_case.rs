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
use kamu::testing::MetadataFactory;
use kamu::AppendDatasetMetadataBatchUseCaseImpl;
use kamu_core::AppendDatasetMetadataBatchUseCase;
use messaging_outbox::MockOutbox;
use opendatafabric::*;

use crate::tests::use_cases::*;
use crate::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mock_outbox = MockOutbox::new();

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(mock_outbox);
    let created_foo = harness.create_root_dataset(&alias_foo).await;

    let set_info_block = MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(created_foo.head.clone()),
        sequence_number: 2,
        event: MetadataEvent::SetInfo(MetadataFactory::set_info().description("test").build()),
    };
    let hash_set_info_block = BaseRepoHarness::hash_from_block(&set_info_block);

    let set_license_block = MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(hash_set_info_block.clone()),
        sequence_number: 3,
        event: MetadataEvent::SetLicense(MetadataFactory::set_license().build()),
    };
    let hash_set_license_block = BaseRepoHarness::hash_from_block(&set_license_block);

    let new_blocks = VecDeque::from([
        (hash_set_info_block, set_info_block),
        (hash_set_license_block, set_license_block),
    ]);

    let res = harness
        .use_case
        .execute(created_foo.dataset.as_ref(), new_blocks, false)
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_append_dataset_metadata_batch_with_new_dependencies() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let mut mock_outbox = MockOutbox::new();
    expect_outbox_dataset_dependencies_updated(&mut mock_outbox, 1);

    let harness = AppendDatasetMetadataBatchUseCaseHarness::new(mock_outbox);
    let foo_created = harness.create_root_dataset(&alias_foo).await;
    let bar_created = harness
        .create_derived_dataset(&alias_bar, vec![foo_created.dataset_handle.as_local_ref()])
        .await;

    let set_transform_block = MetadataBlock {
        system_time: Utc::now(),
        prev_block_hash: Some(bar_created.head.clone()),
        sequence_number: 2,
        event: MetadataEvent::SetTransform(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases(vec![(
                    foo_created.dataset_handle.id,
                    alias_foo.to_string(),
                )])
                .build(),
        ),
    };
    let hash_set_transform_block = BaseRepoHarness::hash_from_block(&set_transform_block);

    let new_blocks = VecDeque::from([(hash_set_transform_block, set_transform_block)]);

    let res = harness
        .use_case
        .execute(bar_created.dataset.as_ref(), new_blocks, false)
        .await;
    assert_matches!(res, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_harness)]
struct AppendDatasetMetadataBatchUseCaseHarness {
    base_harness: BaseUseCaseHarness,
    use_case: Arc<dyn AppendDatasetMetadataBatchUseCase>,
}

impl AppendDatasetMetadataBatchUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_harness =
            BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new().with_outbox(mock_outbox));

        let catalog = dill::CatalogBuilder::new_chained(base_harness.catalog())
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .build();

        let use_case = catalog
            .get_one::<dyn AppendDatasetMetadataBatchUseCase>()
            .unwrap();

        Self {
            base_harness,
            use_case,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
