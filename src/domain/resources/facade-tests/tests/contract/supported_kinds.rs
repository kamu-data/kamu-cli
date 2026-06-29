// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_resources_facade::{
    GetResourceError,
    ListResourceIdentitiesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ResourceSelector,
    SpecViewMode,
};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_KIND,
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    apply_manifest_and_get_uid,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-001
contract_test!(lists_supported_kinds, super::test_lists_supported_kinds);

pub async fn test_lists_supported_kinds(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);
    let descriptors = facade.list_supported_kinds().await.unwrap();

    assert!(!descriptors.is_empty(), "supported kinds must not be empty");

    for d in &descriptors {
        assert!(!d.name.is_empty(), "descriptor name must not be empty");
        assert!(!d.kind.is_empty(), "descriptor kind must not be empty");
        assert!(
            !d.api_version.is_empty(),
            "descriptor api_version must not be empty"
        );
    }

    // Descriptor names are unique
    let names: Vec<&str> = descriptors.iter().map(|d| d.name.as_str()).collect();
    let name_count = names.len();
    let unique_names: std::collections::HashSet<_> = names.into_iter().collect();
    assert_eq!(
        unique_names.len(),
        name_count,
        "descriptor names must be unique"
    );

    // (kind, api_version) pairs are unique
    let pairs: Vec<_> = descriptors
        .iter()
        .map(|d| (d.kind.as_str(), d.api_version.as_str()))
        .collect();
    let pair_count = pairs.len();
    let unique_pairs: std::collections::HashSet<_> = pairs.into_iter().collect();
    assert_eq!(
        unique_pairs.len(),
        pair_count,
        "(kind, api_version) pairs must be unique"
    );

    // VariableSet and SecretSet must be present
    let has_variable_set = descriptors.iter().any(|d| d.kind == VARIABLE_SET_KIND);
    let has_secret_set = descriptors.iter().any(|d| d.kind == SECRET_SET_KIND);
    assert!(has_variable_set, "VariableSet kind must be present");
    assert!(has_secret_set, "SecretSet kind must be present");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-002
// Current behavior: kind short names ("vs", "ss") are informational only and
// are NOT accepted as kind selectors in facade APIs.  The dispatcher registry
// matches on the exact canonical resource_type string, so passing "vs" to
// list/get/etc. returns UnsupportedDescriptor just like any other unknown kind
// string. This test documents the current contract.  If short-name resolution
// is added to the dispatcher lookup in the future, update this test
// accordingly.
contract_test!(
    kind_aliases_resolve_consistently,
    super::test_kind_aliases_resolve_consistently
);

pub async fn test_kind_aliases_resolve_consistently(h: &impl FacadeContractHarness) {
    apply_manifest_and_get_uid(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("alias-check", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);

    // Canonical kind name works for list, list_identities, and get
    let summaries = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .expect("list with canonical kind must succeed");
    for s in &summaries {
        assert_eq!(s.kind, VARIABLE_SET_KIND, "list kind must be canonical");
    }

    let identities = facade
        .list_identities(ListResourceIdentitiesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await
        .expect("list_identities with canonical kind must succeed");
    for i in &identities {
        assert_eq!(
            i.kind, VARIABLE_SET_KIND,
            "list_identities kind must be canonical"
        );
    }

    facade
        .get(
            ResourceSelector {
                account: None,
                kind: VARIABLE_SET_KIND.to_string(),
                api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
                resource_ref: kamu_resources_facade::ResourceRef::ByName("alias-check".to_string()),
            },
            SpecViewMode::Encrypted,
        )
        .await
        .expect("get with canonical kind must succeed");

    // Short name "vs" is NOT a supported selector — it returns
    // UnsupportedDescriptor
    let short_name_list = facade
        .list(ListResourcesRequest {
            kind: "vs".to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    assert!(
        matches!(
            short_name_list,
            Err(ListResourcesError::UnsupportedDescriptor(_))
        ),
        "short name 'vs' is not a supported kind selector for list; got: {short_name_list:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-003
// Unsupported kind rejection behavior by API:
//
// - list / list_identities: UnsupportedDescriptor (kind validated before DB
//   query)
// - apply_manifest: UnsupportedDescriptor (kind validated from manifest)
// - delete (by UID): UnsupportedDescriptor (kind validated before UID lookup)
//
// Known gap — get / get_identity by ByName with an unknown kind:
//   The facade resolves the UID via a DB name lookup first, passing the raw
// kind   string as a filter column.  For an unknown kind, nothing matches →
//   LookupProblem(NameNotFound) is returned instead of UnsupportedDescriptor.
//   This is an implementation detail of the current ByName resolution path.
//   get / get_identity by ById does return UnsupportedDescriptor because the
// kind   is validated when the CRUD dispatcher is resolved after the UID is
// known.
contract_test!(
    unsupported_kind_rejected_consistently,
    super::test_unsupported_kind_rejected_consistently
);

pub async fn test_unsupported_kind_rejected_consistently(h: &impl FacadeContractHarness) {
    let uid = apply_manifest_and_get_uid(
        h,
        TestAccount::Alice,
        variable_set_manifest_json("unsupported-kind-base", None, &[("K", "v")]),
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    let bad_kind = "NoSuchResourceKindXYZ";

    // get by ByName — NameNotFound (kind not validated before DB name lookup)
    let get_by_name = facade
        .get(
            ResourceSelector {
                account: None,
                kind: bad_kind.to_string(),
                api_version: None,
                resource_ref: kamu_resources_facade::ResourceRef::ByName(
                    "unsupported-kind-base".to_string(),
                ),
            },
            SpecViewMode::Encrypted,
        )
        .await;
    assert!(
        matches!(
            get_by_name,
            Err(GetResourceError::LookupProblem(
                kamu_resources_facade::ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "get by ByName with unknown kind returns NameNotFound, got: {get_by_name:?}"
    );

    // get_identity by ByName — same NameNotFound behavior
    let gi_by_name = facade
        .get_identity(ResourceSelector {
            account: None,
            kind: bad_kind.to_string(),
            api_version: None,
            resource_ref: kamu_resources_facade::ResourceRef::ByName(
                "unsupported-kind-base".to_string(),
            ),
        })
        .await;
    assert!(
        matches!(
            gi_by_name,
            Err(GetResourceError::LookupProblem(
                kamu_resources_facade::ResourceLookupProblem::NameNotFound(_)
            ))
        ),
        "get_identity by ByName with unknown kind returns NameNotFound, got: {gi_by_name:?}"
    );

    // list — UnsupportedDescriptor (kind validated before DB query)
    let list_result = facade
        .list(ListResourcesRequest {
            kind: bad_kind.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    assert!(
        matches!(
            list_result,
            Err(ListResourcesError::UnsupportedDescriptor(_))
        ),
        "list: unsupported kind must return UnsupportedDescriptor, got: {list_result:?}"
    );

    // list_identities — UnsupportedDescriptor
    let li_result = facade
        .list_identities(ListResourceIdentitiesRequest {
            kind: bad_kind.to_string(),
            account: None,
            pagination: PaginationOpts::from_max_results(1000),
        })
        .await;
    assert!(
        matches!(li_result, Err(ListResourcesError::UnsupportedDescriptor(_))),
        "list_identities: unsupported kind must return UnsupportedDescriptor, got: {li_result:?}"
    );

    // apply_manifest — UnsupportedDescriptor
    let bad_manifest = format!(
        r#"{{"apiVersion":"v1","kind":"{bad_kind}","headers":{{"name":"x"}},"spec":{{}}}}"#
    );
    let apply_result = facade
        .apply_manifest(kamu_resources_facade::ApplyManifestRequest {
            format: kamu_resources_facade::ResourceManifestFormat::Json,
            manifest: bad_manifest,
        })
        .await;
    assert!(
        matches!(
            apply_result,
            Err(kamu_resources_facade::ApplyManifestError::UnsupportedDescriptor(_))
        ),
        "apply_manifest: unsupported kind must return UnsupportedDescriptor, got: {apply_result:?}"
    );

    // delete by ById — UnsupportedDescriptor (kind validated after UID is known)
    let delete_result = facade
        .delete(ResourceSelector {
            account: None,
            kind: bad_kind.to_string(),
            api_version: None,
            resource_ref: kamu_resources_facade::ResourceRef::ById(uid),
        })
        .await;
    assert!(
        matches!(
            delete_result,
            Err(kamu_resources_facade::DeleteResourceError::UnsupportedDescriptor(_))
        ),
        "delete by ById: unsupported kind must return UnsupportedDescriptor, got: \
         {delete_result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
