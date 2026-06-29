// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_configuration::{SecretSetEntry, VariableSetEntry};
use uuid::Uuid;

use crate::tests::services::dataset_env_var_service_harness::DatasetEnvVarServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_variable_binding_order_first_wins() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let id_a = harness.allocate_resource_id().await;
    let id_b = harness.allocate_resource_id().await;

    let now = Utc::now();

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

    // Bind id_a first, id_b second
    harness
        .variable_set_binding_repo()
        .replace_bindings(&dataset_id, &[id_a, id_b])
        .await
        .unwrap();

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    // First binding (uid_a) wins for key X
    assert_eq!(
        std::str::from_utf8(&env_map["X"].value).unwrap(),
        "from-a",
        "first binding must win on key collision"
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

    let uid_var = harness.allocate_resource_id().await;
    let uid_sec = harness.allocate_resource_id().await;
    let now = Utc::now();

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

    harness
        .variable_set_binding_repo()
        .replace_bindings(&dataset_id, &[uid_var])
        .await
        .unwrap();

    harness
        .secret_set_binding_repo()
        .replace_bindings(&dataset_id, &[uid_sec])
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
