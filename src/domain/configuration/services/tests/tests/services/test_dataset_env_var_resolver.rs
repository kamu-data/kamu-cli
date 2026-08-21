// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{Duration, Utc};
use kamu_configuration::{SecretSetEntry, VariableSetEntry};
use uuid::Uuid;

use crate::tests::services::dataset_env_var_service_harness::DatasetEnvVarServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_oldest_labelled_variable_set_wins() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");

    let now = Utc::now();

    // Seeded newest-first so a resolver that preserved insertion order rather
    // than sorting by `created_at` would pick the wrong winner.
    let id_b = harness
        .seed_variable_set_targeting(&account, &dataset_id, "newer-vars", now)
        .await;
    let id_a = harness
        .seed_variable_set_targeting(
            &account,
            &dataset_id,
            "older-vars",
            now - Duration::hours(1),
        )
        .await;

    let var_repo = harness.variable_set_projection_repo();

    var_repo
        .replace_entries(
            &id_a,
            1,
            &[
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "X".to_string(),
                    value: "from-a".to_string(),
                    created_at: now,
                    updated_at: now,
                },
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "Y".to_string(),
                    value: "common".to_string(),
                    created_at: now,
                    updated_at: now,
                },
            ],
        )
        .await
        .unwrap();

    var_repo
        .replace_entries(
            &id_b,
            1,
            &[
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "X".to_string(),
                    value: "from-b".to_string(),
                    created_at: now,
                    updated_at: now,
                },
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "Z".to_string(),
                    value: "extra".to_string(),
                    created_at: now,
                    updated_at: now,
                },
            ],
        )
        .await
        .unwrap();

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    // The older set (id_a) wins for key X
    assert_eq!(
        std::str::from_utf8(&env_map["X"].value).unwrap(),
        "from-a",
        "the oldest labelled set must win on key collision"
    );
    assert_eq!(std::str::from_utf8(&env_map["Y"].value).unwrap(), "common");
    assert_eq!(std::str::from_utf8(&env_map["Z"].value).unwrap(), "extra");
    assert_eq!(env_map.len(), 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_secret_overrides_variable_on_same_key() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let account = odf::AccountHandle::new_test("test-account");
    let now = Utc::now();

    let uid_var = harness
        .seed_variable_set_targeting(&account, &dataset_id, "vars", now)
        .await;
    let uid_sec = harness
        .seed_secret_set_targeting(&account, &dataset_id, "secrets", now)
        .await;

    harness
        .variable_set_projection_repo()
        .replace_entries(
            &uid_var,
            1,
            &[
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "X".to_string(),
                    value: "var-value".to_string(),
                    created_at: now,
                    updated_at: now,
                },
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "Y".to_string(),
                    value: "var-only".to_string(),
                    created_at: now,
                    updated_at: now,
                },
            ],
        )
        .await
        .unwrap();

    // Write a secret entry for key X with dummy encrypted bytes
    harness
        .secret_set_projection_repo()
        .replace_entries(
            &uid_sec,
            1,
            &[SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                key: "X".to_string(),
                value: b"encrypted-value".to_vec(),
                secret_nonce: b"nonce".to_vec(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    // X must come from the secret (has secret_nonce)
    let x = &env_map["X"];
    assert!(
        x.secret_nonce.is_some(),
        "X must be overridden by secret entry"
    );
    assert_eq!(x.value, b"encrypted-value".to_vec());

    // Y comes from the variable (no secret_nonce)
    let y = &env_map["Y"];
    assert!(y.secret_nonce.is_none());
    assert_eq!(std::str::from_utf8(&y.value).unwrap(), "var-only");

    assert_eq!(env_map.len(), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_variable_sets_labelled_for_other_datasets_are_ignored() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, other_dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");

    let now = Utc::now();

    let mine = harness
        .seed_variable_set_targeting(&account, &dataset_id, "mine", now)
        .await;
    // Older, so it would win on collision if the label were ever ignored.
    let theirs = harness
        .seed_variable_set_targeting(
            &account,
            &other_dataset_id,
            "theirs",
            now - Duration::hours(1),
        )
        .await;

    let var_repo = harness.variable_set_projection_repo();

    for (resource_id, value) in [(mine, "mine"), (theirs, "theirs")] {
        var_repo
            .replace_entries(
                &resource_id,
                1,
                &[VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "X".to_string(),
                    value: value.to_string(),
                    created_at: now,
                    updated_at: now,
                }],
            )
            .await
            .unwrap();
    }

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    assert_eq!(
        std::str::from_utf8(&env_map["X"].value).unwrap(),
        "mine",
        "a set labelled for another dataset must not resolve here"
    );
    assert_eq!(env_map.len(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
