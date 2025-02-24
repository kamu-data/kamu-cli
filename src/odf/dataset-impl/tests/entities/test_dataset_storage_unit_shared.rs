// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use odf::DatasetID;
use odf_metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store_dataset<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter,
>(
    storage_unit: &TDatasetStorageUnit,
) {
    let seed_block = MetadataFactory::metadata_block(
        MetadataFactory::seed(odf::DatasetKind::Root)
            .id_random()
            .build(),
    )
    .build_typed();
    assert_matches!(
        storage_unit
            .get_stored_dataset_by_id(&seed_block.event.dataset_id)
            .await
            .err()
            .unwrap(),
        odf::dataset::GetStoredDatasetError::UnresolvedId(_)
    );

    let store_result = storage_unit
        .store_dataset(seed_block.clone())
        .await
        .unwrap();

    assert_eq!(store_result.dataset_id, seed_block.event.dataset_id);

    // We should see the dataset
    assert!(storage_unit
        .get_stored_dataset_by_id(&seed_block.event.dataset_id)
        .await
        .is_ok());

    // Now test ID collision
    let store_result = storage_unit.store_dataset(seed_block).await;

    assert_matches!(
        store_result.err(),
        Some(odf::dataset::StoreDatasetError::RefCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: &TDatasetStorageUnit,
) {
    // Create dataset
    let seed_block = MetadataFactory::metadata_block(
        MetadataFactory::seed(odf::DatasetKind::Root)
            .id_random()
            .build(),
    )
    .build_typed();
    let store_result = storage_unit
        .store_dataset(seed_block.clone())
        .await
        .unwrap();

    // We should see the dataset
    assert!(storage_unit
        .get_stored_dataset_by_id(&seed_block.event.dataset_id)
        .await
        .is_ok());

    // Delete the dataset
    storage_unit
        .delete_dataset(&store_result.dataset_id)
        .await
        .unwrap();

    // No longer visible
    assert_matches!(
        storage_unit
            .get_stored_dataset_by_id(&seed_block.event.dataset_id)
            .await
            .err()
            .unwrap(),
        odf::dataset::GetStoredDatasetError::UnresolvedId(_),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_iterate_datasets<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: &TDatasetStorageUnit,
) {
    // Create 2 datasets
    let seed_block_1 = MetadataFactory::metadata_block(
        MetadataFactory::seed(odf::DatasetKind::Root)
            .id_random()
            .build(),
    )
    .build_typed();
    let seed_block_2 = MetadataFactory::metadata_block(
        MetadataFactory::seed(odf::DatasetKind::Root)
            .id_random()
            .build(),
    )
    .build_typed();

    let store_result_1 = storage_unit
        .store_dataset(seed_block_1.clone())
        .await
        .unwrap();
    let store_result_2 = storage_unit
        .store_dataset(seed_block_2.clone())
        .await
        .unwrap();

    use futures::TryStreamExt;
    let mut actual_datasets: Vec<_> = storage_unit
        .stored_dataset_ids()
        .try_collect()
        .await
        .unwrap();
    actual_datasets.sort_by_key(DatasetID::to_string);

    let mut expected_datasets = vec![
        store_result_1.dataset_id.clone(),
        store_result_2.dataset_id.clone(),
    ];
    expected_datasets.sort_by_key(DatasetID::to_string);

    assert_eq!(expected_datasets, actual_datasets);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
