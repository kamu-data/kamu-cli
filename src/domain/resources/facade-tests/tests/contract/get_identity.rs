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
    ResourceKindMismatchError,
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
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
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
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

fn by_id_selector(uid: &kamu_resources::ResourceUID) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ById(*uid),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_resource(
    h: &impl FacadeContractHarness,
    name: &str,
) -> kamu_resources::ResourceUID {
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
    result.metadata.uid
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-030
contract_test!(get_by_name, super::test_get_by_name);

pub async fn test_get_by_name(h: &impl FacadeContractHarness) {
    let uid = create_test_resource(h, "get-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let view = facade
        .get(by_name_selector("get-name-test"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(
        &view,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "get-name-test",
    );
    assert_eq!(view.metadata.uid, uid, "uid must match");
    assert!(view.metadata.deleted_at.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-031
contract_test!(get_by_uid, super::test_get_by_uid);

pub async fn test_get_by_uid(h: &impl FacadeContractHarness) {
    let uid = create_test_resource(h, "get-uid-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let view_by_uid = facade
        .get(by_id_selector(&uid), SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_resource_view_fields(
        &view_by_uid,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "get-uid-test",
    );
    assert_eq!(view_by_uid.metadata.uid, uid);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-032
contract_test!(get_identity_by_name, super::test_get_identity_by_name);

pub async fn test_get_identity_by_name(h: &impl FacadeContractHarness) {
    let uid = create_test_resource(h, "ident-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let identity = facade
        .get_identity(by_name_selector("ident-name-test"))
        .await
        .unwrap();

    assert_identity_fields(
        &identity,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "ident-name-test",
        &uid,
    );
    assert!(!identity.canonical_kind_name.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-033
contract_test!(get_identity_by_uid, super::test_get_identity_by_uid);

pub async fn test_get_identity_by_uid(h: &impl FacadeContractHarness) {
    let uid = create_test_resource(h, "ident-uid-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let identity_by_name = facade
        .get_identity(by_name_selector("ident-uid-test"))
        .await
        .unwrap();
    let identity_by_uid = facade.get_identity(by_id_selector(&uid)).await.unwrap();

    assert_eq!(
        identity_by_name.uid, identity_by_uid.uid,
        "uid must match when fetched by name vs uid"
    );
    assert_eq!(identity_by_name.name, identity_by_uid.name);
    assert_eq!(identity_by_name.kind, identity_by_uid.kind);
    assert_eq!(identity_by_name.api_version, identity_by_uid.api_version);
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
    let absent_uid = kamu_resources::ResourceUID::new(uuid::Uuid::new_v4());

    let get_result = facade
        .get(by_id_selector(&absent_uid), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(
            get_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::UIDNotFound(_)
            ))
        ),
        "expected UIDNotFound, got: {get_result:?}"
    );

    let identity_result = facade.get_identity(by_id_selector(&absent_uid)).await;
    assert!(
        matches!(
            identity_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::UIDNotFound(_)
            ))
        ),
        "expected UIDNotFound from get_identity, got: {identity_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-036
contract_test!(
    get_wrong_api_version_returns_mismatch,
    super::test_get_wrong_api_version_returns_mismatch
);

pub async fn test_get_wrong_api_version_returns_mismatch(h: &impl FacadeContractHarness) {
    let uid = create_test_resource(h, "api-ver-mismatch-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let wrong_version_selector = ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some("v0.never.existed".to_string()),
        resource_ref: ResourceRef::ById(uid),
    };

    let result = facade
        .get(wrong_version_selector.clone(), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(
            result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::ApiVersionMismatch(_)
            ))
        ),
        "expected ApiVersionMismatch, got: {result:?}"
    );

    let identity_result = facade.get_identity(wrong_version_selector).await;
    assert!(
        matches!(
            identity_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::ApiVersionMismatch(_)
            ))
        ),
        "expected ApiVersionMismatch from get_identity, got: {identity_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-037
contract_test!(
    get_wrong_kind_returns_kind_mismatch,
    super::test_get_wrong_kind_returns_kind_mismatch
);

pub async fn test_get_wrong_kind_returns_kind_mismatch(h: &impl FacadeContractHarness) {
    use crate::helpers::{SECRET_SET_API_VERSION, SECRET_SET_KIND};

    let uid = create_test_resource(h, "kind-mismatch-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let wrong_kind_selector = ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        api_version: Some(SECRET_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ById(uid),
    };

    let result = facade
        .get(wrong_kind_selector.clone(), SpecViewMode::Encrypted)
        .await;
    match &result {
        Err(GetResourceError::LookupProblem(ResourceLookupProblem::KindMismatch(
            ResourceKindMismatchError {
                expected_kind,
                actual_kind,
                ..
            },
        ))) => {
            assert_eq!(
                expected_kind, SECRET_SET_KIND,
                "expected_kind must be the requested kind"
            );
            assert_eq!(
                actual_kind, VARIABLE_SET_KIND,
                "actual_kind must be the stored kind"
            );
        }
        other => panic!("expected KindMismatch, got: {other:?}"),
    }

    let identity_result = facade.get_identity(wrong_kind_selector).await;
    assert!(
        matches!(
            identity_result,
            Err(GetResourceError::LookupProblem(
                ResourceLookupProblem::KindMismatch(_)
            ))
        ),
        "expected KindMismatch from get_identity, got: {identity_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
