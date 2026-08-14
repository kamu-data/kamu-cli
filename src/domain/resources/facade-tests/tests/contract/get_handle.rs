// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::VariableSetResource;
use kamu_resources::{ApplyResourceOutcome, ResourceRef, ResourceSchemaProvider, TypeName};
use kamu_resources_facade::{
    ApplyManifestRequest,
    GetResourceError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceSchemaMismatchError,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    assert_applied_outcome,
    assert_handle_fields,
    assert_resource_view_fields,
    variable_set_manifest_json,
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

async fn create_test_resource(
    h: &impl FacadeContractHarness,
    name: &str,
) -> kamu_resources::ResourceID {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-030
contract_test!(get_by_name, super::test_get_by_name);

pub async fn test_get_by_name(h: &impl FacadeContractHarness) {
    let id = create_test_resource(h, "get-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let view = facade
        .get(by_name_selector("get-name-test"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(&view, VariableSetResource::schema(), "get-name-test");
    assert_eq!(view.headers.id, id, "id must match");
    assert!(view.headers.deleted_at.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-031
contract_test!(get_by_uid, super::test_get_by_uid);

pub async fn test_get_by_uid(h: &impl FacadeContractHarness) {
    let id = create_test_resource(h, "get-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let view_by_uid = facade
        .get(by_id_selector(&id), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(&view_by_uid, VariableSetResource::schema(), "get-id-test");
    assert_eq!(view_by_uid.headers.id, id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-032
contract_test!(get_handle_by_name, super::test_get_handle_by_name);

pub async fn test_get_handle_by_name(h: &impl FacadeContractHarness) {
    let id = create_test_resource(h, "ident-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let handle = facade
        .get_handle(by_name_selector("ident-name-test"))
        .await
        .unwrap();

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
    let id = create_test_resource(h, "ident-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let handle_by_name = facade
        .get_handle(by_name_selector("ident-id-test"))
        .await
        .unwrap();
    let handle_by_uid = facade.get_handle(by_id_selector(&id)).await.unwrap();

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
            by_name_selector("no-such-resource"),
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            get_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "expected NameNotFound, got: {get_result:?}"
    );

    let handle_result = facade
        .get_handle(by_name_selector("no-such-resource"))
        .await;
    assert!(
        matches!(
            handle_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "expected NameNotFound from get_handle, got: {handle_result:?}"
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
        .get(by_id_selector(&absent_uid), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(
            get_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::IDNotFound(_)
            ))
        ),
        "expected IDNotFound, got: {get_result:?}"
    );

    let handle_result = facade.get_handle(by_id_selector(&absent_uid)).await;
    assert!(
        matches!(
            handle_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::IDNotFound(_)
            ))
        ),
        "expected IDNotFound from get_handle, got: {handle_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-036
contract_test!(
    get_wrong_schema_returns_mismatch,
    super::test_get_wrong_schema_returns_mismatch
);

pub async fn test_get_wrong_schema_returns_mismatch(h: &impl FacadeContractHarness) {
    let id = create_test_resource(h, "api-ver-mismatch-test").await;
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
        .get(wrong_schema_selector.clone(), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(
            result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch, got: {result:?}"
    );

    let handle_result = facade.get_handle(wrong_schema_selector).await;
    assert!(
        matches!(
            handle_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch from get_handle, got: {handle_result:?}"
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

    let id = create_test_resource(h, "schema-mismatch-test").await;
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
        .get(wrong_schema_selector.clone(), SpecViewMode::Encrypted)
        .await;
    match &result {
        Err(GetResourceError::LookupProblem(ResourceLookupProblem::SchemaMismatch(
            ResourceSchemaMismatchError {
                expected_schema,
                actual_schema,
                ..
            },
        ))) => {
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

    let handle_result = facade.get_handle(wrong_schema_selector).await;
    assert!(
        matches!(
            handle_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch from get_handle, got: {handle_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
