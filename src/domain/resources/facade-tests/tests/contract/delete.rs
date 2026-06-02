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
    GetResourceError,
    ListResourcesRequest,
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
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_resource(
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

fn by_name(name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

fn by_id(uid: &kamu_resources::ResourceUID) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ById(*uid),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-130
contract_test!(delete_by_name, super::test_delete_by_name);

pub async fn test_delete_by_name(h: &impl FacadeContractHarness) {
    let uid = create_resource(h, "del-name-test").await;
    let facade = h.facade_for(TestAccount::Alice);

    let deleted_uid = facade.delete(by_name("del-name-test")).await.unwrap();

    assert_eq!(deleted_uid, uid, "deleted uid must match created uid");

    // Resource must not be found by name
    let get_by_name = facade
        .get(by_name("del-name-test"), SpecViewMode::Encrypted)
        .await;
    assert!(
        matches!(get_by_name, Err(GetResourceError::LookupProblem(_))),
        "deleted resource must not be found by name"
    );

    // Resource must not be found by uid
    let get_by_uid = facade.get(by_id(&uid), SpecViewMode::Encrypted).await;
    assert!(
        matches!(get_by_uid, Err(GetResourceError::LookupProblem(_))),
        "deleted resource must not be found by uid"
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
        !list.iter().any(|s| s.uid == uid),
        "deleted resource must not appear in list"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
