// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretSetSpec, SecretSpec, SecretValueSpec};
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_resources::{ApplyResourceApplicationDecision, ApplyResourceOutcome, ApplyResourceParams};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_encrypts_literal_values() {
    let harness = BaseConfigurationServiceHarness::new();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let spec = SecretSetSpec {
        secrets: [
            (
                "API_TOKEN".to_string(),
                SecretSpec::Literal("my-secret-token".to_string()),
            ),
            (
                "DB_PASSWORD".to_string(),
                SecretSpec::Value(SecretValueSpec {
                    value: "my-db-password".to_string(),
                }),
            ),
        ]
        .into_iter()
        .collect(),
    };

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_id, "test-secrets"),
            spec,
        })
        .await
        .unwrap();

    let applied_id = match decision {
        ApplyResourceApplicationDecision::Applied(result) => result.id,
        ApplyResourceApplicationDecision::Rejected(r) => {
            panic!("apply was rejected: {}", r.message)
        }
    };

    // Load the stored snapshot and deserialize the spec
    let snapshot = harness
        .generic_query_svc()
        .get_snapshot_by_id(&applied_id)
        .await
        .unwrap()
        .expect("snapshot must exist after apply");

    let stored_spec: SecretSetSpec =
        serde_json::from_value(snapshot.spec.clone()).expect("spec must deserialize");

    // All values must be Encrypted — no Literal or Value variants in stored form
    for (name, secret) in &stored_spec.secrets {
        assert!(
            secret.is_encrypted(),
            "secret '{name}' must be Encrypted in stored spec, got: {secret:?}"
        );
    }

    // Plaintext values must not appear anywhere in the serialized spec JSON
    let spec_json = snapshot.spec.to_string();
    assert!(
        !spec_json.contains("my-secret-token"),
        "plaintext 'my-secret-token' must not appear in stored spec"
    );
    assert!(
        !spec_json.contains("my-db-password"),
        "plaintext 'my-db-password' must not appear in stored spec"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_already_encrypted_passes_through_idempotently() {
    let harness = BaseConfigurationServiceHarness::new();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    // First apply with a Literal value to produce an encrypted snapshot
    let spec = SecretSetSpec {
        secrets: [(
            "API_TOKEN".to_string(),
            SecretSpec::Literal("original-value".to_string()),
        )]
        .into_iter()
        .collect(),
    };

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(
                account_id.clone(),
                "test-secrets",
            ),
            spec,
        })
        .await
        .unwrap();

    let id = match decision {
        ApplyResourceApplicationDecision::Applied(result) => result.id,
        ApplyResourceApplicationDecision::Rejected(r) => {
            panic!("first apply was rejected: {}", r.message)
        }
    };

    // Load the stored spec (already encrypted) and re-apply it as-is
    let snapshot = harness
        .generic_query_svc()
        .get_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();

    let encrypted_spec: SecretSetSpec =
        serde_json::from_value(snapshot.spec).expect("spec must deserialize");

    // Confirm the first apply produced an Encrypted variant
    assert!(
        encrypted_spec.secrets["API_TOKEN"].is_encrypted(),
        "first apply must produce an Encrypted variant"
    );

    // Re-apply the already-encrypted spec — the sanitizer must pass it through
    // unchanged
    let decision2 = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: Some(id),
            headers: BaseResourceServiceHarness::make_headers_input(account_id, "test-secrets"),
            spec: encrypted_spec.clone(),
        })
        .await
        .unwrap();

    match &decision2 {
        ApplyResourceApplicationDecision::Applied(result) => {
            assert_eq!(result.outcome, ApplyResourceOutcome::Untouched);
        }
        ApplyResourceApplicationDecision::Rejected(r) => {
            panic!("second apply was rejected unexpectedly: {}", r.message)
        }
    }

    let snapshot2 = harness
        .generic_query_svc()
        .get_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();

    let stored_spec2: SecretSetSpec =
        serde_json::from_value(snapshot2.spec).expect("spec must deserialize");

    // After re-apply the value must still be an Encrypted variant (not
    // double-wrapped)
    assert!(
        stored_spec2.secrets["API_TOKEN"].is_encrypted(),
        "secret must remain Encrypted after idempotent re-apply"
    );

    // The ciphertext must equal the original — sanitizer is a no-op on
    // already-encrypted specs
    assert_eq!(
        stored_spec2.secrets["API_TOKEN"].as_encrypted(),
        encrypted_spec.secrets["API_TOKEN"].as_encrypted(),
        "ciphertext must be unchanged after idempotent re-apply"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_same_plaintext_is_untouched() {
    let harness = BaseConfigurationServiceHarness::new();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let spec = SecretSetSpec {
        secrets: [(
            "API_TOKEN".to_string(),
            SecretSpec::Literal("original-value".to_string()),
        )]
        .into_iter()
        .collect(),
    };

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(
                account_id.clone(),
                "test-secrets",
            ),
            spec: spec.clone(),
        })
        .await
        .unwrap();

    let id = match decision {
        ApplyResourceApplicationDecision::Applied(result) => result.id,
        ApplyResourceApplicationDecision::Rejected(r) => {
            panic!("first apply was rejected: {}", r.message)
        }
    };

    let snapshot = harness
        .generic_query_svc()
        .get_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();

    let stored_spec: SecretSetSpec =
        serde_json::from_value(snapshot.spec).expect("spec must deserialize");

    let decision2 = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: Some(id),
            headers: BaseResourceServiceHarness::make_headers_input(account_id, "test-secrets"),
            spec,
        })
        .await
        .unwrap();

    match decision2 {
        ApplyResourceApplicationDecision::Applied(result) => {
            assert_eq!(result.outcome, ApplyResourceOutcome::Untouched);
        }
        ApplyResourceApplicationDecision::Rejected(r) => {
            panic!("second apply was rejected unexpectedly: {}", r.message)
        }
    }

    let snapshot2 = harness
        .generic_query_svc()
        .get_snapshot_by_id(&id)
        .await
        .unwrap()
        .unwrap();

    let stored_spec2: SecretSetSpec =
        serde_json::from_value(snapshot2.spec).expect("spec must deserialize");

    assert_eq!(stored_spec2, stored_spec);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
