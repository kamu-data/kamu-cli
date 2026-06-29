// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::ApplyResourceOutcome;
use kamu_resources_facade::{
    ApplyManifestRequest,
    DeleteResourceError,
    GetResourceError,
    ListResourcesRequest,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_KIND,
    VARIABLE_SET_KIND,
    assert_applied_outcome,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_resource(h: &impl FacadeContractHarness, name: &str) -> kamu_resources::ResourceID {
    let facade = h.facade_for(TestAccount::Alice);
    let manifest = variable_set_manifest_json(name, None, &[("K", "v")]);
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    let result = assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
    result.headers.id
}

fn by_name(name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

fn by_id(id: &kamu_resources::ResourceID) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(*id),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-130
contract_test!(delete_by_name, super::test_delete_by_name);

pub async fn test_delete_by_name(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "del-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deleted_uid = facade.delete(by_name("del-name-test")).await.unwrap();

    assert_eq!(deleted_uid, id, "deleted id must match created id");

    // Resource must not be found by name
    let get_by_name = facade
        .get(by_name("del-name-test"), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(get_by_name, Err(GetResourceError::LookupProblem(_))),
        "deleted resource must not be found by name"
    );

    // Resource must not be found by id
    let get_by_uid = facade.get(by_id(&id), SpecViewMode::Encrypted).await;
    assert!(
        matches!(get_by_uid, Err(GetResourceError::LookupProblem(_))),
        "deleted resource must not be found by id"
    );

    // Resource must not appear in list
    let list = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts {
                limit: 1000,
                offset: 0,
            },
        })
        .await
        .unwrap();
    assert!(
        !list.iter().any(|s| s.id == id),
        "deleted resource must not appear in list"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-131
contract_test!(delete_by_uid, super::test_delete_by_uid);

pub async fn test_delete_by_uid(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "del-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deleted_uid = facade.delete(by_id(&id)).await.unwrap();

    assert_eq!(deleted_uid, id, "deleted id must match created id");

    let get_by_name = facade
        .get(by_name("del-id-test"), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(get_by_name, Err(GetResourceError::LookupProblem(_))),
        "resource must not be found by name after delete by id"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-132
contract_test!(
    delete_missing_name_returns_not_found,
    super::test_delete_missing_name_returns_not_found
);

pub async fn test_delete_missing_name_returns_not_found(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let result = facade.delete(by_name("no-such-delete")).await;
    assert!(
        matches!(
            result,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "expected NameNotFound, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-133
contract_test!(
    delete_missing_uid_returns_not_found,
    super::test_delete_missing_uid_returns_not_found
);

pub async fn test_delete_missing_uid_returns_not_found(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());

    let result = facade.delete(by_id(&absent_uid)).await;
    assert!(
        matches!(
            result,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::IDNotFound(_)
            ))
        ),
        "expected UIDNotFound, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-134
contract_test!(
    delete_wrong_schema_returns_mismatch,
    super::test_delete_wrong_schema_returns_mismatch
);

pub async fn test_delete_wrong_schema_returns_mismatch(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "del-api-ver").await;
    let facade = h.facade_for(TestAccount::Alice);

    let wrong_schema_selector = ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(id),
    };
    let result = facade.delete(wrong_schema_selector).await;
    assert!(
        matches!(
            result,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch, got: {result:?}"
    );

    let wrong_kind = ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(id),
    };
    let result = facade.delete(wrong_kind).await;
    assert!(
        matches!(
            result,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-135
// Deleting a resource in one account must not affect a resource with the
// same name in another account.
contract_test!(
    delete_is_account_scoped,
    super::test_delete_is_account_scoped
);

pub async fn test_delete_is_account_scoped(h: &impl FacadeContractHarness) {
    let alice_uid = create_resource(h, "scoped-del").await;

    // Create same-named resource for Bob.
    let bob_uid = {
        let facade = h.facade_for(TestAccount::Bob);
        let manifest = variable_set_manifest_json("scoped-del", None, &[("K", "v")]);
        let decision = facade
            .apply_manifest(ApplyManifestRequest {
                format: ResourceManifestFormat::Json,
                manifest,
            })
            .await
            .unwrap();
        assert_applied_outcome(&decision, ApplyResourceOutcome::Created)
            .headers
            .id
    };

    // Delete Alice's resource.
    let alice_facade = h.facade_for(TestAccount::Alice);
    let deleted_uid = alice_facade.delete(by_name("scoped-del")).await.unwrap();
    assert_eq!(deleted_uid, alice_uid);

    // Alice's resource must be gone.
    let alice_get = alice_facade
        .get(by_name("scoped-del"), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(alice_get, Err(GetResourceError::LookupProblem(_))),
        "Alice's resource must be gone after delete"
    );

    // Bob's resource must still exist.
    let bob_facade = h.facade_for(TestAccount::Bob);
    let bob_view = bob_facade
        .get(by_name("scoped-del"), SpecViewMode::Encrypted)
        .await
        .expect("Bob's resource must survive Alice's delete");
    assert_eq!(bob_view.headers.id, bob_uid);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-136
// Deleting the same resource twice: second call must return a not-found error.
contract_test!(
    repeated_delete_is_deterministic,
    super::test_repeated_delete_is_deterministic
);

pub async fn test_repeated_delete_is_deterministic(h: &impl FacadeContractHarness) {
    let id = create_resource(h, "repeat-del").await;
    let facade = h.facade_for(TestAccount::Alice);

    // First delete succeeds.
    let deleted = facade.delete(by_id(&id)).await.unwrap();
    assert_eq!(deleted, id);

    // Second delete must return not-found.
    let result = facade.delete(by_id(&id)).await;
    assert!(
        matches!(
            result,
            Err(DeleteResourceError::LookupProblem(
                ResourceLookupProblem::IDNotFound(_)
            ))
        ),
        "second delete must return UIDNotFound, got: {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
