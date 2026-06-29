// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{VariableSetSpec, VariableSpec};
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_resources::ApplyResourceParams;
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_variable_set_populates_projection_entries() {
    let harness = BaseConfigurationServiceHarness::new();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let spec = VariableSetSpec {
        variables: [
            (
                "DB_HOST".to_string(),
                VariableSpec::Literal("localhost".to_string()),
            ),
            (
                "DB_PORT".to_string(),
                VariableSpec::Literal("5432".to_string()),
            ),
        ]
        .into_iter()
        .collect(),
    };

    let decision = harness
        .apply_variable_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_id, "test-vars"),
            spec,
        })
        .await
        .unwrap();

    let applied_id = decision.expect_applied().id;

    // Reconciliation fires synchronously via OutboxImmediateImpl
    let entries = harness
        .variable_set_projection_repo()
        .get_latest_entries(&applied_id)
        .await
        .unwrap();

    assert_eq!(entries.len(), 2, "expected 2 projection entries");

    let by_key: std::collections::HashMap<_, _> =
        entries.into_iter().map(|e| (e.key.clone(), e)).collect();

    assert_eq!(
        by_key["DB_HOST"].value, "localhost",
        "DB_HOST must carry its literal value"
    );
    assert_eq!(
        by_key["DB_PORT"].value, "5432",
        "DB_PORT must carry its literal value"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_variable_set_preserves_entry_id_across_reconciliations() {
    let harness = BaseConfigurationServiceHarness::new();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let spec = VariableSetSpec {
        variables: [
            (
                "KEY_A".to_string(),
                VariableSpec::Literal("value-a".to_string()),
            ),
            (
                "KEY_B".to_string(),
                VariableSpec::Literal("value-b".to_string()),
            ),
        ]
        .into_iter()
        .collect(),
    };

    // First apply — generation 0 → 1
    let decision = harness
        .apply_variable_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(
                account_id.clone(),
                "test-vars",
            ),
            spec,
        })
        .await
        .unwrap();

    let id = decision.expect_applied().id;

    let entries_gen1 = harness
        .variable_set_projection_repo()
        .get_latest_entries(&id)
        .await
        .unwrap();
    assert_eq!(entries_gen1.len(), 2);

    let ids_gen1: std::collections::HashMap<_, _> = entries_gen1
        .iter()
        .map(|e| (e.key.clone(), (e.entry_id, e.created_at)))
        .collect();

    // Second apply — change value of KEY_A, keep KEY_B the same; generation 1 → 2
    let spec2 = VariableSetSpec {
        variables: [
            (
                "KEY_A".to_string(),
                VariableSpec::Literal("value-a-updated".to_string()),
            ),
            (
                "KEY_B".to_string(),
                VariableSpec::Literal("value-b".to_string()),
            ),
        ]
        .into_iter()
        .collect(),
    };

    let decision2 = harness
        .apply_variable_use_case()
        .apply(ApplyResourceParams {
            id: Some(id),
            headers: BaseResourceServiceHarness::make_headers_input(
                account_id.clone(),
                "test-vars",
            ),
            spec: spec2,
        })
        .await
        .unwrap();

    decision2.expect_applied();

    let entries_gen2 = harness
        .variable_set_projection_repo()
        .get_latest_entries(&id)
        .await
        .unwrap();
    assert_eq!(entries_gen2.len(), 2);

    let by_key_gen2: std::collections::HashMap<_, _> =
        entries_gen2.iter().map(|e| (e.key.clone(), e)).collect();

    for key in ["KEY_A", "KEY_B"] {
        let (prev_id, prev_created_at) = ids_gen1[key];
        let entry = by_key_gen2[key];

        assert_eq!(
            entry.entry_id, prev_id,
            "entry_id for '{key}' must be preserved across reconciliations"
        );
        assert_eq!(
            entry.created_at, prev_created_at,
            "created_at for '{key}' must be preserved across reconciliations"
        );
    }

    assert_eq!(
        by_key_gen2["KEY_A"].value, "value-a-updated",
        "KEY_A value must reflect the update"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
