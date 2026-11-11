// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::SecretSetSpecInput;
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_datasets::SAMPLE_SECRETS_ENCRYPTION_KEY;
use kamu_resources::{ApplyResourceApplicationDecision, ApplyResourceParams, ResourcePhase};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconcile_secret_set_decrypts_and_reprojects_values() {
    let harness = BaseConfigurationServiceHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("secret-set-owner");

    let spec = SecretSetSpecInput::new(odf::metadata::config::SecretSetSpecInput {
        secrets: odf::metadata::config::Secrets {
            entries: [
                (
                    "API_TOKEN".to_string(),
                    odf::metadata::config::Secret {
                        value: "my-secret-token".to_string(),
                        content_encoding: None,
                    },
                ),
                (
                    "DB_PASSWORD".to_string(),
                    odf::metadata::config::Secret {
                        value: "my-db-password".to_string(),
                        content_encoding: None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
        },
    });

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

#[test_log::test(tokio::test)]
async fn test_cleanup_secret_set_removes_its_projection_entries() {
    let harness = BaseConfigurationServiceHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("secret-set-owner");

    let id = harness
        .apply_secret_set(
            &account_handle,
            "test-secrets",
            [("API_TOKEN", "my-secret-token")],
        )
        .await;
    assert_eq!(
        harness.secret_set_projection_entry_count(&id).await,
        1,
        "precondition: the secret is projected"
    );

    harness.cleanup_secret_set(&id).await;

    assert_eq!(
        harness.secret_set_projection_entry_count(&id).await,
        0,
        "cleanup must remove the projection entries of the resource"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_requested_deletion_of_secret_set_removes_its_projection_entries() {
    let harness = BaseConfigurationServiceHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("secret-set-owner");

    let id_deleted = harness
        .apply_secret_set(
            &account_handle,
            "secrets-deleted",
            [("API_TOKEN", "my-secret-token")],
        )
        .await;
    let id_other = harness
        .apply_secret_set(
            &account_handle,
            "secrets-other",
            [("API_TOKEN", "my-secret-token")],
        )
        .await;

    harness
        .request_secret_sets_deletion(&account_handle.did, vec![id_deleted])
        .await;

    assert_eq!(
        harness.secret_set_phase(&id_deleted).await,
        ResourcePhase::Deleted
    );
    assert!(harness.get_snapshot_by_id(&id_deleted).await.is_none());
    assert_eq!(
        harness.secret_set_projection_entry_count(&id_deleted).await,
        0,
        "completing the deletion must remove the projection entries of the resource"
    );
    assert_eq!(
        harness.secret_set_projection_entry_count(&id_other).await,
        1,
        "entries of other resources must be untouched"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
