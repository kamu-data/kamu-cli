// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ApplyResourceOutcome;
use kamu_resources_facade::{
    ApplyManifestRequest,
    GetResourceError,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSchemaMismatchError,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_KIND,
    VARIABLE_SET_KIND,
    VARIABLE_SET_SCHEMA,
    assert_applied_outcome,
    assert_identity_fields,
    assert_resource_view_fields,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn by_name_selector(name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

fn by_id_selector(id: &kamu_resources::ResourceID) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(*id),
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

    assert_resource_view_fields(
        &view,
        VARIABLE_SET_SCHEMA,
        VARIABLE_SET_SCHEMA,
        "get-name-test",
    );
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

    assert_resource_view_fields(
        &view_by_uid,
        VARIABLE_SET_SCHEMA,
        VARIABLE_SET_SCHEMA,
        "get-id-test",
    );
    assert_eq!(view_by_uid.headers.id, id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-032
contract_test!(get_identity_by_name, super::test_get_identity_by_name);

pub async fn test_get_identity_by_name(h: &impl FacadeContractHarness) {
    let id = create_test_resource(h, "ident-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let identity = facade
        .get_identity(by_name_selector("ident-name-test"))
        .await
        .unwrap();

    assert_identity_fields(
        &identity,
        VARIABLE_SET_SCHEMA,
        VARIABLE_SET_SCHEMA,
        "ident-name-test",
        &id,
    );
    assert!(!identity.canonical_kind_name.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-033
contract_test!(get_identity_by_uid, super::test_get_identity_by_uid);

pub async fn test_get_identity_by_uid(h: &impl FacadeContractHarness) {
    let id = create_test_resource(h, "ident-id-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let identity_by_name = facade
        .get_identity(by_name_selector("ident-id-test"))
        .await
        .unwrap();
    let identity_by_uid = facade.get_identity(by_id_selector(&id)).await.unwrap();

    assert_eq!(
        identity_by_name.id, identity_by_uid.id,
        "id must match when fetched by name vs id"
    );
    assert_eq!(identity_by_name.name, identity_by_uid.name);
    assert_eq!(identity_by_name.schema, identity_by_uid.schema);
    assert_eq!(identity_by_name.schema, identity_by_uid.schema);
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

    let identity_result = facade
        .get_identity(by_name_selector("no-such-resource"))
        .await;
    assert!(
        matches!(
            identity_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "expected NameNotFound from get_identity, got: {identity_result:?}"
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
        "expected UIDNotFound, got: {get_result:?}"
    );

    let identity_result = facade.get_identity(by_id_selector(&absent_uid)).await;
    assert!(
        matches!(
            identity_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::IDNotFound(_)
            ))
        ),
        "expected UIDNotFound from get_identity, got: {identity_result:?}"
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

    let wrong_schema_selector = ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(id),
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

    let identity_result = facade.get_identity(wrong_schema_selector).await;
    assert!(
        matches!(
            identity_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch from get_identity, got: {identity_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-037
contract_test!(
    get_wrong_kind_returns_kind_mismatch,
    super::test_get_wrong_kind_returns_kind_mismatch
);

pub async fn test_get_wrong_kind_returns_kind_mismatch(h: &impl FacadeContractHarness) {
    use crate::helpers::SECRET_SET_SCHEMA;

    let id = create_test_resource(h, "kind-mismatch-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let wrong_kind_selector = ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        resource_ref: ResourceRef::ById(id),
    };

    let result = facade
        .get(wrong_kind_selector.clone(), SpecViewMode::Encrypted)
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
                expected_schema, SECRET_SET_SCHEMA,
                "expected_schema must be the requested kind"
            );
            assert_eq!(
                actual_schema, VARIABLE_SET_SCHEMA,
                "actual_schema must be the stored kind"
            );
        }
        other => panic!("expected SchemaMismatch, got: {other:?}"),
    }

    let identity_result = facade.get_identity(wrong_kind_selector).await;
    assert!(
        matches!(
            identity_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::SchemaMismatch(_)
            ))
        ),
        "expected SchemaMismatch from get_identity, got: {identity_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
