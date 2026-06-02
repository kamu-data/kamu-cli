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
    ListResourceIdentitiesRequest,
    ListResourcesRequest,
    ResourceManifestFormat,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    assert_applied_outcome,
    normalize_identity_views,
    normalize_summary_views,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_resource(h: &impl FacadeContractHarness, account: TestAccount, name: &str) {
    let facade = h.facade_for(account);
    let manifest = variable_set_manifest_json(name, None, &[("K", "v")]);
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    assert_applied_outcome(&decision, ApplyResourceOutcome::Created);
}

fn all_pagination() -> PaginationOpts {
    PaginationOpts {
        limit: 1000,
        offset: 0,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-080
contract_test!(
    list_summaries_for_account,
    super::test_list_summaries_for_account
);

pub async fn test_list_summaries_for_account(h: &impl FacadeContractHarness) {
    // Create resources in each account
    create_resource(h, TestAccount::Alice, "list-alice-1").await;
    create_resource(h, TestAccount::Alice, "list-alice-2").await;
    create_resource(h, TestAccount::Bob, "list-bob-1").await;

    let facade = h.facade_for(TestAccount::Alice);

    let mut summaries = facade
        .list(ListResourcesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None, // default = alice
            pagination: all_pagination(),
        })
        .await
        .unwrap();

    normalize_summary_views(&mut summaries);

    // Only alice's resources
    let names: Vec<&str> = summaries.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"list-alice-1"), "alice-1 must be listed");
    assert!(names.contains(&"list-alice-2"), "alice-2 must be listed");
    assert!(
        !names.contains(&"list-bob-1"),
        "bob-1 must not appear in alice's list"
    );

    for s in &summaries {
        assert_eq!(s.kind, VARIABLE_SET_KIND, "kind must match");
        assert_eq!(
            s.api_version, VARIABLE_SET_API_VERSION,
            "api_version must match"
        );
        assert!(!s.uid.to_string().is_empty(), "uid must be set");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-081
contract_test!(
    list_identities_for_account,
    super::test_list_identities_for_account
);

pub async fn test_list_identities_for_account(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "idlist-alice-1").await;
    create_resource(h, TestAccount::Alice, "idlist-alice-2").await;
    create_resource(h, TestAccount::Bob, "idlist-bob-1").await;

    let facade = h.facade_for(TestAccount::Alice);

    let mut identities = facade
        .list_identities(ListResourceIdentitiesRequest {
            kind: VARIABLE_SET_KIND.to_string(),
            account: None,
            pagination: all_pagination(),
        })
        .await
        .unwrap();

    normalize_identity_views(&mut identities);

    let names: Vec<&str> = identities.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"idlist-alice-1"));
    assert!(names.contains(&"idlist-alice-2"));
    assert!(!names.contains(&"idlist-bob-1"));

    for i in &identities {
        assert_eq!(i.kind, VARIABLE_SET_KIND);
        assert_eq!(i.api_version, VARIABLE_SET_API_VERSION);
        assert!(!i.canonical_kind_name.is_empty());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
