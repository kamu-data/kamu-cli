// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretExt, SecretSetSpec, SecretSetSpecInput};
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourceParams,
    ResourceSpecFromInput,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn plaintext_secret(value: &str) -> odf::metadata::config::Secret {
    odf::metadata::config::Secret {
        value: value.to_string(),
        content_encoding: None,
    }
}

fn make_spec_input(
    entries: impl IntoIterator<Item = (impl Into<String>, odf::metadata::config::Secret)>,
) -> SecretSetSpecInput {
    SecretSetSpecInput::new(odf::metadata::config::SecretSetSpecInput {
        secrets: odf::metadata::config::Secrets {
            entries: entries.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        },
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_encrypts_literal_values() {
    let harness = BaseConfigurationServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");

    let spec = make_spec_input([
        ("API_TOKEN", plaintext_secret("my-secret-token")),
        ("DB_PASSWORD", plaintext_secret("my-db-password")),
    ]);

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(account_handle, "test-secrets"),
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

    // All values must be encrypted — no plaintext secrets in stored form
    for (name, secret) in &stored_spec.secrets.entries {
        assert!(
            secret.is_encrypted(),
            "secret '{name}' must be encrypted in stored spec, got: {secret:?}"
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
    let account_handle = odf::AccountHandle::new_test("test-owner");

    // First apply with a plaintext value to produce an encrypted snapshot
    let spec = make_spec_input([("API_TOKEN", plaintext_secret("original-value"))]);

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(
                account_handle.clone(),
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

    // Confirm the first apply produced an encrypted secret
    assert!(
        encrypted_spec.secrets.entries["API_TOKEN"].is_encrypted(),
        "first apply must produce an encrypted secret"
    );

    // Re-apply the already-encrypted spec — the sanitizer must pass it through
    // unchanged
    let decision2 = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: Some(id),
            headers: BaseResourceServiceHarness::make_headers_input(account_handle, "test-secrets"),
            spec: encrypted_spec.clone().into_input(),
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

    // After re-apply the value must still be encrypted (not double-wrapped)
    assert!(
        stored_spec2.secrets.entries["API_TOKEN"].is_encrypted(),
        "secret must remain encrypted after idempotent re-apply"
    );

    // The ciphertext must equal the original — sanitizer is a no-op on
    // already-encrypted specs
    assert_eq!(
        stored_spec2.secrets.entries["API_TOKEN"].as_encrypted(),
        encrypted_spec.secrets.entries["API_TOKEN"].as_encrypted(),
        "ciphertext must be unchanged after idempotent re-apply"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_same_plaintext_is_untouched() {
    let harness = BaseConfigurationServiceHarness::new();
    let account_handle = odf::AccountHandle::new_test("test-owner");

    let spec = make_spec_input([("API_TOKEN", plaintext_secret("original-value"))]);

    let decision = harness
        .apply_secret_use_case()
        .apply(ApplyResourceParams {
            id: None,
            headers: BaseResourceServiceHarness::make_headers_input(
                account_handle.clone(),
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
            headers: BaseResourceServiceHarness::make_headers_input(account_handle, "test-secrets"),
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
