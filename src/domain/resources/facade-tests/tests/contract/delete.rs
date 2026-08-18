// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources::{ApplyResourceOutcome, ResourceRef, ResourceSelector, TypeName};
use kamu_resources_facade::{
    ApplyManifestRequest,
    ResourceLookupProblem,
    ResourceManifestFormat,
    SearchResourcesRequest,
    SpecViewOpts,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    assert_applied_outcome,
    assert_single_batch_problem,
    assert_single_batch_success,
    create_variable_set,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name(name: &str) -> ResourceRef {
    ResourceRef {
        account: None,
        r#type: Some(
            VARIABLE_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: None,
        did: None,
        name: Some(name.parse().unwrap()),
    }
}

fn by_id(id: &kamu_resources::ResourceID) -> ResourceRef {
    ResourceRef {
        account: None,
        r#type: Some(
            VARIABLE_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: Some(*id),
        did: None,
        name: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-130
contract_test!(delete_by_name, super::test_delete_by_name);

pub async fn test_delete_by_name(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deleted_uid =
        assert_single_batch_success(facade.delete(vec![by_name("del-name-test")]).await.unwrap());

    assert_eq!(deleted_uid, id, "deleted id must match created id");

    // Resource must not be found by name
    let get_by_name = facade
        .get(vec![by_name("del-name-test")], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    let get_err = assert_single_batch_problem(get_by_name);
    assert_matches!(
        get_err,
        ResourceLookupProblem::NameNotFound(_),
        "deleted resource must not be found by name"
    );

    // Resource must not be found by id
    let get_by_uid = facade
        .get(vec![by_id(&id)], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    let get_err = assert_single_batch_problem(get_by_uid);
    assert_matches!(
        get_err,
        ResourceLookupProblem::IDNotFound(_),
        "deleted resource must not be found by id"
    );

    // Resource must not appear in list
    let list = facade
        .search(SearchResourcesRequest {
            selectors: vec![ResourceSelector::of_type(
                VARIABLE_SET_CANONICAL_SELECTOR.parse().unwrap(),
            )],
            account: None,
            pagination: PaginationOpts {
                limit: 1000,
                offset: 0,
            },
        })
        .await
        .unwrap();
    assert!(
        !list.items.iter().any(|s| s.id == id),
        "deleted resource must not appear in list"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-131
contract_test!(delete_by_uid, super::test_delete_by_uid);

pub async fn test_delete_by_uid(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deleted_uid = assert_single_batch_success(facade.delete(vec![by_id(&id)]).await.unwrap());

    assert_eq!(deleted_uid, id, "deleted id must match created id");

    let get_by_name = facade
        .get(vec![by_name("del-id-test")], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    let get_err = assert_single_batch_problem(get_by_name);
    assert_matches!(
        get_err,
        ResourceLookupProblem::NameNotFound(_),
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

    let result = facade
        .delete(vec![by_name("no-such-delete")])
        .await
        .unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::NameNotFound(_),
        "expected NameNotFound"
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

    let result = facade.delete(vec![by_id(&absent_uid)]).await.unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::IDNotFound(_),
        "expected IDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-134
contract_test!(
    delete_wrong_schema_returns_mismatch,
    super::test_delete_wrong_schema_returns_mismatch
);

pub async fn test_delete_wrong_schema_returns_mismatch(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "del-api-ver").await;
    let facade = h.facade_for(TestAccount::Alice);

    let wrong_schema_selector = ResourceRef {
        account: None,
        r#type: Some(
            SECRET_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: Some(id),
        did: None,
        name: None,
    };
    let result = facade
        .delete(vec![wrong_schema_selector.clone()])
        .await
        .unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch"
    );

    let result = facade.delete(vec![wrong_schema_selector]).await.unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch"
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
    let alice_uid = create_variable_set(h, TestAccount::Alice, "scoped-del").await;

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
    let deleted_uid = assert_single_batch_success(
        alice_facade
            .delete(vec![by_name("scoped-del")])
            .await
            .unwrap(),
    );
    assert_eq!(deleted_uid, alice_uid);

    // Alice's resource must be gone.
    let alice_get = alice_facade
        .get(vec![by_name("scoped-del")], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    let alice_err = assert_single_batch_problem(alice_get);
    assert_matches!(
        alice_err,
        ResourceLookupProblem::NameNotFound(_),
        "Alice's resource must be gone after delete"
    );

    // Bob's resource must still exist.
    let bob_facade = h.facade_for(TestAccount::Bob);
    let bob_view = assert_single_batch_success(
        bob_facade
            .get(vec![by_name("scoped-del")], SpecViewOpts::ENCRYPTED)
            .await
            .unwrap(),
    );
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
    let id = create_variable_set(h, TestAccount::Alice, "repeat-del").await;
    let facade = h.facade_for(TestAccount::Alice);

    // First delete succeeds.
    let deleted = assert_single_batch_success(facade.delete(vec![by_id(&id)]).await.unwrap());
    assert_eq!(deleted, id);

    // Second delete must return not-found.
    let result = facade.delete(vec![by_id(&id)]).await.unwrap();
    let err = assert_single_batch_problem(result);
    assert_matches!(
        err,
        ResourceLookupProblem::IDNotFound(_),
        "second delete must return IDNotFound"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
