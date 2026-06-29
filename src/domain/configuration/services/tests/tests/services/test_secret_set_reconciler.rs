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
use kamu_datasets::SAMPLE_SECRETS_ENCRYPTION_KEY;
use kamu_resources::{ApplyResourceApplicationDecision, ApplyResourceParams};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_secret_set_decrypts_and_reprojects_values() {
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

    // Reconciliation fires synchronously via OutboxImmediateImpl — projection
    // entries should already be populated
    let entries = harness
        .secret_set_projection_repo()
        .get_latest_entries(&applied_id)
        .await
        .unwrap();

    assert_eq!(entries.len(), 2, "expected 2 projection entries");

    use crypto_utils::{AesGcmEncryptor, Encryptor};

    let encryptor = AesGcmEncryptor::try_new(SAMPLE_SECRETS_ENCRYPTION_KEY).unwrap();

    let entries_by_key: std::collections::HashMap<_, _> =
        entries.into_iter().map(|e| (e.key.clone(), e)).collect();

    for (key, expected_plaintext) in [
        ("API_TOKEN", "my-secret-token"),
        ("DB_PASSWORD", "my-db-password"),
    ] {
        let entry = entries_by_key
            .get(key)
            .unwrap_or_else(|| panic!("entry '{key}' not found in projection"));

        let decrypted = encryptor
            .decrypt_bytes(&entry.value, &entry.secret_nonce)
            .unwrap_or_else(|e| panic!("failed to decrypt '{key}': {e}"));

        assert_eq!(
            std::str::from_utf8(&decrypted).unwrap(),
            expected_plaintext,
            "decrypted value for '{key}' does not match original plaintext"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
