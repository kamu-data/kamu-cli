// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use dill::Catalog;
use kamu_resources::{ResourceID, ResourceLabelProjectionRepository, ResourceRepository, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static TEST_KIND: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("TestKind"));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// resource_labels_projection has a FK to resources(resource_id), so every
// entry here is projected for a resource actually created through
// ResourceRepository first — same as real usage via the persistence
// controller, which always writes the resources row before the projection.
async fn create_test_resource(catalog: &Catalog, name: &str) -> ResourceID {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account = odf::AccountHandle::new_test("test-account");
    let snapshot =
        crate::resource_repository_test_suite::make_test_snapshot(&account, &TEST_KIND, name);
    let id = snapshot.id;

    repo.create_resource(&snapshot).await.unwrap();

    id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_no_entries_initially(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let id = create_test_resource(catalog, "no-entries").await;

    assert_eq!(repo.find_entries(&id).await.unwrap(), vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_entries_then_find(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let id = create_test_resource(catalog, "replace-then-find").await;

    repo.replace_entries(
        &id,
        &[
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "platform".to_string()),
        ],
    )
    .await
    .unwrap();

    assert_eq!(
        repo.find_entries(&id).await.unwrap(),
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "platform".to_string()),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_entries_overwrites_previous_set(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let id = create_test_resource(catalog, "replace-overwrites").await;

    repo.replace_entries(
        &id,
        &[
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "platform".to_string()),
        ],
    )
    .await
    .unwrap();

    // change a value, drop a key, add a key
    repo.replace_entries(
        &id,
        &[
            ("env".to_string(), "staging".to_string()),
            ("owner".to_string(), "alice".to_string()),
        ],
    )
    .await
    .unwrap();

    assert_eq!(
        repo.find_entries(&id).await.unwrap(),
        vec![
            ("env".to_string(), "staging".to_string()),
            ("owner".to_string(), "alice".to_string()),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_entries_with_empty_slice_clears(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let id = create_test_resource(catalog, "replace-empty-clears").await;

    repo.replace_entries(&id, &[("env".to_string(), "prod".to_string())])
        .await
        .unwrap();
    assert_eq!(repo.find_entries(&id).await.unwrap().len(), 1);

    repo.replace_entries(&id, &[]).await.unwrap();

    assert_eq!(repo.find_entries(&id).await.unwrap(), vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_entries_isolated_by_resource_id(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn ResourceLabelProjectionRepository>()
        .unwrap();

    let id_a = create_test_resource(catalog, "isolated-a").await;
    let id_b = create_test_resource(catalog, "isolated-b").await;

    repo.replace_entries(&id_a, &[("env".to_string(), "prod".to_string())])
        .await
        .unwrap();
    repo.replace_entries(&id_b, &[("env".to_string(), "staging".to_string())])
        .await
        .unwrap();

    assert_eq!(
        repo.find_entries(&id_a).await.unwrap(),
        vec![("env".to_string(), "prod".to_string())]
    );
    assert_eq!(
        repo.find_entries(&id_b).await.unwrap(),
        vec![("env".to_string(), "staging".to_string())]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
