// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use database_common::testing::{in_committed_transaction, in_rolled_back_transaction};
use dill::Catalog;
use kamu_resources::{ResourceID, ResourceOwnerProjectionRepository, ResourceRepository, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static TEST_KIND: LazyLock<TypeUri> = LazyLock::new(|| TypeUri::new_unchecked("TestKind"));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Only the owned resource must exist: the index outlives the owners
async fn create_test_resource(catalog: &Catalog, name: &str) -> ResourceID {
    let repo = catalog.get_one::<dyn ResourceRepository>().unwrap();

    let account = odf::ResourceOwnerHandle::new_test("test-account");
    let snapshot =
        crate::resource_repository_test_suite::make_test_snapshot(&account, &TEST_KIND, name);
    let id = snapshot.id;

    repo.create_resource(&snapshot).await.unwrap();

    id
}

fn make_owner_ids() -> [ResourceID; 2] {
    let mut ids = [(); 2].map(|()| ResourceID::new(uuid::Uuid::new_v4()));
    ids.sort();
    ids
}

async fn replace_owners(catalog: &Catalog, id: ResourceID, owner_ids: &[ResourceID]) {
    catalog
        .get_one::<dyn ResourceOwnerProjectionRepository>()
        .unwrap()
        .replace_owners(&id, owner_ids)
        .await
        .unwrap();
}

async fn find_owners(catalog: &Catalog, id: ResourceID) -> Vec<ResourceID> {
    catalog
        .get_one::<dyn ResourceOwnerProjectionRepository>()
        .unwrap()
        .find_owners(&id)
        .await
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_replace_owners(catalog: &Catalog) {
    let owned = create_test_resource(catalog, "owned").await;
    let other = create_test_resource(catalog, "other").await;
    let [first, second] = make_owner_ids();

    assert_eq!(find_owners(catalog, owned).await, []);

    replace_owners(catalog, owned, &[second, first]).await;
    replace_owners(catalog, other, &[first]).await;
    assert_eq!(find_owners(catalog, owned).await, [first, second]);

    replace_owners(catalog, owned, &[second]).await;
    assert_eq!(find_owners(catalog, owned).await, [second]);

    replace_owners(catalog, owned, &[]).await;
    assert_eq!(find_owners(catalog, owned).await, []);

    assert_eq!(find_owners(catalog, other).await, [first]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rolled_back_replace_keeps_previous_owners(catalog: &Catalog) {
    let [owner, _] = make_owner_ids();

    let id = in_committed_transaction(catalog, |catalog| async move {
        let id = create_test_resource(&catalog, "rolled-back-owners").await;
        replace_owners(&catalog, id, &[owner]).await;
        id
    })
    .await;

    in_rolled_back_transaction(catalog, |catalog| async move {
        replace_owners(&catalog, id, &[]).await;
    })
    .await;

    let owners =
        in_committed_transaction(
            catalog,
            |catalog| async move { find_owners(&catalog, id).await },
        )
        .await;
    assert_eq!(owners, [owner]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
