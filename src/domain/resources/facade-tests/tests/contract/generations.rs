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
    ResourcePhase,
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

// RF-181
contract_test!(
    generations_count_desired_state_and_status_changes,
    super::test_generations_count_desired_state_and_status_changes,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_generations_count_desired_state_and_status_changes(
    h: &impl FacadeContractHarness,
) {
    let facade = h.facade_for(TestAccount::Alice);

    // Created, then reconciled: one status update when reconciliation starts,
    // one with its outcome
    apply(facade.as_ref(), "one", None).await;
    h.flush_outbox().await;
    assert_eq!(observe(facade.as_ref()).await, (1, 3, ResourcePhase::Ready));

    apply(facade.as_ref(), "two", None).await;
    assert_eq!(
        observe(facade.as_ref()).await,
        (2, 3, ResourcePhase::Pending)
    );
    h.flush_outbox().await;
    assert_eq!(observe(facade.as_ref()).await, (2, 5, ResourcePhase::Ready));

    // A labels-only change is a change of the desired state too
    apply(facade.as_ref(), "two", Some("data")).await;
    assert_eq!(
        observe(facade.as_ref()).await,
        (3, 5, ResourcePhase::Pending)
    );
    h.flush_outbox().await;
    assert_eq!(observe(facade.as_ref()).await, (3, 7, ResourcePhase::Ready));

    apply(facade.as_ref(), "two", Some("data")).await;
    h.flush_outbox().await;
    assert_eq!(observe(facade.as_ref()).await, (3, 7, ResourcePhase::Ready));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_NAME: &str = "gen-vars";

async fn apply(facade: &dyn ResourceFacade, value: &str, team_label: Option<&str>) {
    let labels = team_label
        .map(|team| format!(r#", "labels": {{"team": "{team}"}}"#))
        .unwrap_or_default();
    let manifest = format!(
        r#"{{
            "$schema": "{VARIABLE_SET_SCHEMA_STR}",
            "headers": {{"name": "{RESOURCE_NAME}"{labels}}},
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

/// `(generation, status_generation, phase)` of the resource
async fn observe(facade: &dyn ResourceFacade) -> (u64, u64, ResourcePhase) {
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

    (
        resource.headers.generation,
        resource.headers.status_generation,
        resource.status.phase,
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
