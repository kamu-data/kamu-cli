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
    ResourceBatchSelector,
    ResourceManifestFormat,
    ResourceRef,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    VARIABLE_SET_API_VERSION,
    VARIABLE_SET_KIND,
    assert_applied_outcome,
    assert_batch_indexes,
    assert_resource_view_fields,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-050
contract_test!(get_many_all_successes, super::test_get_many_all_successes);

pub async fn test_get_many_all_successes(h: &impl FacadeContractHarness) {
    let uid_a = create_resource(h, "batch-a").await;
    let uid_b = create_resource(h, "batch-b").await;
    let facade = h.facade_for(TestAccount::Alice);

    let selector = ResourceBatchSelector {
        account: None,
        kind: VARIABLE_SET_KIND.to_string(),
        api_version: Some(VARIABLE_SET_API_VERSION.to_string()),
        resource_refs: vec![
            ResourceRef::ByName("batch-a".to_string()),
            ResourceRef::ById(uid_b),
        ],
    };

    let response = facade
        .get_many(selector, SpecViewMode::Encrypted)
        .await
        .unwrap();

    assert_batch_indexes(&response, &[0, 1], &[]);
    assert_eq!(response.successes.len(), 2);

    let by_index: std::collections::HashMap<usize, &kamu_resources::ResourceView> = response
        .successes
        .iter()
        .map(|s| (s.request_index, &s.item))
        .collect();

    let view_a = by_index[&0];
    let view_b = by_index[&1];

    assert_resource_view_fields(
        view_a,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "batch-a",
    );
    assert_eq!(view_a.metadata.uid, uid_a);

    assert_resource_view_fields(
        view_b,
        VARIABLE_SET_KIND,
        VARIABLE_SET_API_VERSION,
        "batch-b",
    );
    assert_eq!(view_b.metadata.uid, uid_b);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
