// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ApplyManifestApplicationDecision,
    CreationProvenance,
    ResourceRef,
    ResourceVisibility,
    TypeName,
};
use kamu_resources_facade::{
    ApplyManifestRequest,
    ResourceFacade,
    ResourceManifestFormat,
    SpecViewOpts,
};
use pretty_assertions::{assert_eq, assert_matches};

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_CANONICAL_SELECTOR,
    VARIABLE_SET_SCHEMA_STR,
    assert_single_batch_success,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-198
contract_test!(
    created_resource_records_its_creator,
    super::test_created_resource_records_its_creator
);

pub async fn test_created_resource_records_its_creator(h: &impl FacadeContractHarness) {
    let alice = h.facade_for(TestAccount::Alice);

    apply(alice.as_ref(), "one").await;
    let created = provenance(alice.as_ref()).await.unwrap();

    assert_eq!(
        created.principal.r#type,
        *odf::metadata::auth::User::type_uri()
    );
    assert_eq!(created.principal.name.as_str(), TestAccount::Alice.name());
    assert_eq!(
        created.principal.did,
        Some(h.account_id(TestAccount::Alice).into())
    );
    assert_eq!(created.origin, h.creation_origin());

    // An update keeps the record of the creation, also when it comes through
    // another boundary
    apply(h.local_facade_for(TestAccount::Alice).as_ref(), "two").await;
    assert_eq!(provenance(alice.as_ref()).await, Some(created));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_NAME: &str = "provenance-vars";

async fn apply(facade: &dyn ResourceFacade, value: &str) {
    let manifest = format!(
        r#"{{
            "$schema": "{VARIABLE_SET_SCHEMA_STR}",
            "headers": {{"name": "{RESOURCE_NAME}"}},
            "spec": {{"variables": {{"A": {{"value": "{value}"}}}}}}
        }}"#
    );

    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    assert_matches!(decision, ApplyManifestApplicationDecision::Applied(_));
}

async fn provenance(facade: &dyn ResourceFacade) -> Option<CreationProvenance> {
    let resource = assert_single_batch_success(
        facade
            .get(
                vec![ResourceRef {
                    account: None,
                    r#type: Some(
                        VARIABLE_SET_CANONICAL_SELECTOR
                            .parse::<TypeName>()
                            .unwrap()
                            .into(),
                    ),
                    id: None,
                    did: None,
                    name: Some(RESOURCE_NAME.parse().unwrap()),
                }],
                SpecViewOpts::ENCRYPTED,
                ResourceVisibility::Active,
            )
            .await
            .unwrap(),
    );

    CreationProvenance::from_conditions(&resource.status.conditions).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
