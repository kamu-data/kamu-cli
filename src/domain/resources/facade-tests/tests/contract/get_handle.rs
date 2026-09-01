// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::VariableSetResource;
use kamu_resources::{ResourceRef, ResourceSchemaProvider, TypeName};
use kamu_resources_facade::{ResourceLookupProblem, ResourceSchemaMismatchError, SpecViewOpts};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    assert_handle_fields,
    assert_resource_view_fields,
    assert_single_batch_problem,
    assert_single_batch_success,
    create_variable_set,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name_selector(name: &str) -> ResourceRef {
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

fn by_id_selector(id: &kamu_resources::ResourceID) -> ResourceRef {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-030
contract_test!(get_by_name, super::test_get_by_name);

pub async fn test_get_by_name(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "get-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let view = assert_single_batch_success(
        facade
            .get(
                vec![by_name_selector("get-name-test")],
                SpecViewOpts::ENCRYPTED,
            )
            .await
            .unwrap(),
    );

    assert_resource_view_fields(&view, VariableSetResource::schema(), "get-name-test");
    assert_eq!(view.headers.id, id, "id must match");
    assert!(view.headers.deleted_at.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-031
contract_test!(get_by_uid, super::test_get_by_uid);

pub async fn test_get_by_uid(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "get-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let view_by_uid = assert_single_batch_success(
        facade
            .get(vec![by_id_selector(&id)], SpecViewOpts::ENCRYPTED)
            .await
            .unwrap(),
    );

    assert_resource_view_fields(&view_by_uid, VariableSetResource::schema(), "get-id-test");
    assert_eq!(view_by_uid.headers.id, id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-032
contract_test!(get_handle_by_name, super::test_get_handle_by_name);

pub async fn test_get_handle_by_name(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "ident-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let handle = assert_single_batch_success(
        facade
            .get_handles(vec![by_name_selector("ident-name-test")])
            .await
            .unwrap(),
    );

    assert_handle_fields(
        &handle,
        VariableSetResource::schema(),
        "ident-name-test",
        &id,
    );
    assert!(
        !kamu_resources::resource_type_name(&handle.r#type)
            .unwrap()
            .as_str()
            .is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-033
contract_test!(get_handle_by_uid, super::test_get_handle_by_uid);

pub async fn test_get_handle_by_uid(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "ident-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let handle_by_name = assert_single_batch_success(
        facade
            .get_handles(vec![by_name_selector("ident-id-test")])
            .await
            .unwrap(),
    );
    let handle_by_uid =
        assert_single_batch_success(facade.get_handles(vec![by_id_selector(&id)]).await.unwrap());

    assert_eq!(
        handle_by_name.id, handle_by_uid.id,
        "id must match when fetched by name vs id"
    );
    assert_eq!(handle_by_name.name, handle_by_uid.name);
    assert_eq!(handle_by_name.r#type, handle_by_uid.r#type);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-034
contract_test!(
    get_missing_name_returns_not_found,
    super::test_get_missing_name_returns_not_found
);

pub async fn test_get_missing_name_returns_not_found(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    let get_result = facade
        .get(
            vec![by_name_selector("no-such-resource")],
            SpecViewOpts::ENCRYPTED,
        )
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get_result),
        ResourceLookupProblem::NameNotFound(_),
        "expected NameNotFound"
    );

    let handle_result = facade
        .get_handles(vec![by_name_selector("no-such-resource")])
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(handle_result),
        ResourceLookupProblem::NameNotFound(_),
        "expected NameNotFound from get_handles"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-035
contract_test!(
    get_missing_uid_returns_not_found,
    super::test_get_missing_uid_returns_not_found
);

pub async fn test_get_missing_uid_returns_not_found(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let absent_uid = kamu_resources::ResourceID::new(uuid::Uuid::new_v4());

    let get_result = facade
        .get(vec![by_id_selector(&absent_uid)], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(get_result),
        ResourceLookupProblem::IDNotFound(_),
        "expected IDNotFound"
    );

    let handle_result = facade
        .get_handles(vec![by_id_selector(&absent_uid)])
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(handle_result),
        ResourceLookupProblem::IDNotFound(_),
        "expected IDNotFound from get_handles"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-036
contract_test!(
    get_wrong_schema_returns_mismatch,
    super::test_get_wrong_schema_returns_mismatch
);

pub async fn test_get_wrong_schema_returns_mismatch(h: &impl FacadeContractHarness) {
    let id = create_variable_set(h, TestAccount::Alice, "api-ver-mismatch-test").await;
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
        .get(vec![wrong_schema_selector.clone()], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(result),
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch"
    );

    let handle_result = facade
        .get_handles(vec![wrong_schema_selector])
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(handle_result),
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch from get_handles"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-037
contract_test!(
    get_wrong_schema_returns_schema_mismatch,
    super::test_get_wrong_schema_returns_schema_mismatch
);

pub async fn test_get_wrong_schema_returns_schema_mismatch(h: &impl FacadeContractHarness) {
    use crate::helpers::SECRET_SET_SCHEMA_STR;

    let id = create_variable_set(h, TestAccount::Alice, "schema-mismatch-test").await;
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
        .get(vec![wrong_schema_selector.clone()], SpecViewOpts::ENCRYPTED)
        .await
        .unwrap();
    match assert_single_batch_problem(result) {
        ResourceLookupProblem::SchemaMismatch(ResourceSchemaMismatchError {
            expected_schema,
            actual_schema,
            ..
        }) => {
            assert_eq!(
                expected_schema.as_str(),
                SECRET_SET_SCHEMA_STR,
                "expected_schema must be the requested schema"
            );
            assert_eq!(
                actual_schema.as_str(),
                VARIABLE_SET_SCHEMA_STR,
                "actual_schema must be the stored schema"
            );
        }
        other => panic!("expected SchemaMismatch, got: {other:?}"),
    }

    let handle_result = facade
        .get_handles(vec![wrong_schema_selector])
        .await
        .unwrap();
    assert_matches!(
        assert_single_batch_problem(handle_result),
        ResourceLookupProblem::SchemaMismatch(_),
        "expected SchemaMismatch from get_handles"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
