// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use dill::Catalog;
use kamu_configuration::{DatasetSecretSetBindingRepository, ReplaceDatasetBindingsError};

use crate::dataset_variable_set_binding_repository_test_suite::prepare_dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_and_list_bindings(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetSecretSetBindingRepository>()
        .unwrap();

    repo.replace_bindings(&dataset_id, &resource_uids)
        .await
        .unwrap();

    let bindings = repo.list_bindings(&dataset_id).await.unwrap();
    assert_eq!(bindings.len(), 3);
    assert_eq!(bindings[0].resource_uid, resource_uids[0]);
    assert_eq!(bindings[1].resource_uid, resource_uids[1]);
    assert_eq!(bindings[2].resource_uid, resource_uids[2]);
    assert_eq!(bindings[0].binding_order, 0);
    assert_eq!(bindings[1].binding_order, 1);
    assert_eq!(bindings[2].binding_order, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_overwrites_previous_bindings(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetSecretSetBindingRepository>()
        .unwrap();

    repo.replace_bindings(&dataset_id, &resource_uids[..2])
        .await
        .unwrap();
    repo.replace_bindings(&dataset_id, &resource_uids[1..])
        .await
        .unwrap();

    let bindings = repo.list_bindings(&dataset_id).await.unwrap();
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].resource_uid, resource_uids[1]);
    assert_eq!(bindings[1].resource_uid, resource_uids[2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_rejects_duplicates(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetSecretSetBindingRepository>()
        .unwrap();

    let result = repo
        .replace_bindings(
            &dataset_id,
            &[resource_uids[0], resource_uids[1], resource_uids[0]],
        )
        .await;

    assert_matches!(result, Err(ReplaceDatasetBindingsError::Duplicate(_)));
    assert!(repo.list_bindings(&dataset_id).await.unwrap().is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_bindings_for_dataset(catalog: &Catalog) {
    let (dataset_id, resource_uids) = prepare_dataset(catalog).await;
    let repo = catalog
        .get_one::<dyn DatasetSecretSetBindingRepository>()
        .unwrap();

    repo.replace_bindings(&dataset_id, &resource_uids)
        .await
        .unwrap();

    repo.delete_bindings_for_dataset(&dataset_id).await.unwrap();

    assert!(repo.list_bindings(&dataset_id).await.unwrap().is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
