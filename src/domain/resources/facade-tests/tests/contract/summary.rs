// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ApplyResourceOutcome, ResourcePhaseCounts, ResourcesSummary};
use kamu_resources_facade::{
    ApplyManifestRequest,
    ResourceManifestFormat,
    ResourcesSummaryRequest,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{VARIABLE_SET_SCHEMA, assert_applied_outcome, variable_set_manifest_json};

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-110
contract_test!(summary_empty_account, super::test_summary_empty_account);

pub async fn test_summary_empty_account(h: &impl FacadeContractHarness) {
    // Bob has no resources in this test; use a fresh Bob perspective
    let facade = h.facade_for(TestAccount::Bob);
    let summary = facade
        .summary(ResourcesSummaryRequest { account: None })
        .await
        .unwrap();

    let bob_vs_count = summary
        .resource_counts
        .iter()
        .find(|r| r.schema == VARIABLE_SET_SCHEMA)
        .map(|r| r.total_count)
        .unwrap_or(0);

    assert_eq!(
        bob_vs_count, 0,
        "empty account must report 0 for VariableSet"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-111
contract_test!(
    summary_counts_resources,
    super::test_summary_counts_resources
);

pub async fn test_summary_counts_resources(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "sum-alice-1").await;
    create_resource(h, TestAccount::Alice, "sum-alice-2").await;
    create_resource(h, TestAccount::Bob, "sum-bob-1").await;

    let alice_facade = h.facade_for(TestAccount::Alice);
    let alice_summary = alice_facade
        .summary(ResourcesSummaryRequest { account: None })
        .await
        .unwrap();

    let alice_vs_count = alice_summary
        .resource_counts
        .iter()
        .find(|r| r.schema == VARIABLE_SET_SCHEMA)
        .map(|r| r.total_count)
        .expect("VariableSet kind must appear in summary");

    assert_eq!(alice_vs_count, 2, "Alice must have exactly 2 VariableSets");

    // Bob's resources must not appear in Alice's summary
    let bob_facade = h.facade_for(TestAccount::Bob);
    let bob_summary = bob_facade
        .summary(ResourcesSummaryRequest { account: None })
        .await
        .unwrap();

    let bob_vs_count = bob_summary
        .resource_counts
        .iter()
        .find(|r| r.schema == VARIABLE_SET_SCHEMA)
        .map(|r| r.total_count)
        .unwrap_or(0);

    assert_eq!(bob_vs_count, 1, "Bob must have exactly 1 VariableSet");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-112
contract_test!(
    summary_phase_counts,
    super::test_summary_phase_counts,
    messaging_outbox::OutboxProvider::Dispatching
);

pub async fn test_summary_phase_counts(h: &impl FacadeContractHarness) {
    let facade = h.facade_for(TestAccount::Alice);

    // Apply a resource without flushing the outbox — it stays in Pending.
    let manifest = variable_set_manifest_json("phase-a", None, &[("K", "v")]);
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest,
        })
        .await
        .unwrap();
    assert_applied_outcome(&decision, ApplyResourceOutcome::Created);

    // Before flush: one resource in Pending, none in Ready.
    let before = facade
        .summary(ResourcesSummaryRequest { account: None })
        .await
        .unwrap();
    let counts_before = vs_phase_counts(&before);
    assert_eq!(
        counts_before.pending, 1,
        "resource must be Pending before reconciliation"
    );
    assert_eq!(
        counts_before.ready, 0,
        "no resources must be Ready before reconciliation"
    );

    // Flush outbox: Applied message is consumed, reconciler runs, resource
    // transitions Pending → Reconciling → Ready within the same iteration.
    // The Reconciling phase is a very short-lived internal transient that is
    // not observable between two flush calls at facade granularity.
    h.flush_outbox().await;

    // After flush: resource is Ready, no longer Pending.
    let after = facade
        .summary(ResourcesSummaryRequest { account: None })
        .await
        .unwrap();
    let counts_after = vs_phase_counts(&after);
    assert_eq!(
        counts_after.pending, 0,
        "resource must no longer be Pending after reconciliation"
    );
    assert_eq!(
        counts_after.ready, 1,
        "resource must be Ready after reconciliation"
    );
}

fn vs_phase_counts(summary: &ResourcesSummary) -> ResourcePhaseCounts {
    summary
        .resource_counts
        .iter()
        .find(|r| r.schema == VARIABLE_SET_SCHEMA)
        .map(|r| r.phase_counts.clone())
        .unwrap_or_default()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-113
contract_test!(summary_account_scoping, super::test_summary_account_scoping);

pub async fn test_summary_account_scoping(h: &impl FacadeContractHarness) {
    create_resource(h, TestAccount::Alice, "sum-scope-alice").await;
    create_resource(h, TestAccount::Bob, "sum-scope-bob").await;

    let alice_facade = h.facade_for(TestAccount::Alice);
    let alice_summary = alice_facade
        .summary(ResourcesSummaryRequest { account: None })
        .await
        .unwrap();

    let bob_facade = h.facade_for(TestAccount::Bob);
    let bob_summary = bob_facade
        .summary(ResourcesSummaryRequest { account: None })
        .await
        .unwrap();

    let alice_vs = alice_summary
        .resource_counts
        .iter()
        .find(|r| r.schema == VARIABLE_SET_SCHEMA)
        .map(|r| r.total_count)
        .unwrap_or(0);

    let bob_vs = bob_summary
        .resource_counts
        .iter()
        .find(|r| r.schema == VARIABLE_SET_SCHEMA)
        .map(|r| r.total_count)
        .unwrap_or(0);

    assert_eq!(alice_vs, 1, "Alice must have exactly 1 VariableSet");
    assert_eq!(bob_vs, 1, "Bob must have exactly 1 VariableSet");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
