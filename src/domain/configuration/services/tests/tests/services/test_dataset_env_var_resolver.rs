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
    let account = odf::AccountHandle::new_test("test-account");

    let now = Utc::now();

    harness.seed_dataset_entry(&dataset_id, &account.did).await;

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
                    account_id: account.did.clone(),
                    key: "X".to_string(),
                    value: "from-a".to_string(),
                    created_at: now,
                    updated_at: now,
                },
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account.did.clone(),
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
                    account_id: account.did.clone(),
                    key: "X".to_string(),
                    value: "from-b".to_string(),
                    created_at: now,
                    updated_at: now,
                },
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account.did.clone(),
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

    let account = odf::AccountHandle::new_test("test-account");
    let now = Utc::now();

    harness.seed_dataset_entry(&dataset_id, &account.did).await;

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
                    account_id: account.did.clone(),
                    key: "X".to_string(),
                    value: "var-value".to_string(),
                    created_at: now,
                    updated_at: now,
                },
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account.did.clone(),
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
                account_id: account.did.clone(),
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

// Both read methods must agree on precedence. `get_env_var_by_entry_key` used
// to scan variable sets first and return on the first hit, so a key shadowed by
// a secret came back as plaintext through `exposedValue` while
// `listEnvVariables` showed the secret — two sibling GraphQL fields disagreeing
// about the same key.
#[test_log::test(tokio::test)]
async fn test_single_key_lookup_prefers_the_secret_like_the_merged_map() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();

    let account = odf::AccountHandle::new_test("test-account");
    let now = Utc::now();

    harness.seed_dataset_entry(&dataset_id, &account.did).await;

    // Seed the variable set first so it is also the *older* of the two: the
    // secret must win on kind, not merely on ordering.
    let uid_var = harness
        .seed_variable_set_targeting(&account, &dataset_id, "vars", now)
        .await;
    let uid_sec = harness
        .seed_secret_set_targeting(
            &account,
            &dataset_id,
            "secrets",
            now + chrono::Duration::seconds(10),
        )
        .await;

    harness
        .variable_set_projection_repo()
        .replace_entries(
            &uid_var,
            1,
            &[
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account.did.clone(),
                    key: "SHADOWED".to_string(),
                    value: "plaintext-must-not-win".to_string(),
                    created_at: now,
                    updated_at: now,
                },
                VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account.did.clone(),
                    key: "VAR_ONLY".to_string(),
                    value: "var-only".to_string(),
                    created_at: now,
                    updated_at: now,
                },
            ],
        )
        .await
        .unwrap();

    harness
        .secret_set_projection_repo()
        .replace_entries(
            &uid_sec,
            1,
            &[SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account.did.clone(),
                key: "SHADOWED".to_string(),
                value: b"encrypted-value".to_vec(),
                secret_nonce: b"nonce".to_vec(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    // The single-key path must return the secret, not the variable.
    let shadowed = harness
        .resolver()
        .get_env_var_by_entry_key(&dataset_id, "SHADOWED")
        .await
        .unwrap();
    assert!(
        shadowed.secret_nonce.is_some(),
        "a key carried by both kinds must resolve to the secret"
    );
    assert_eq!(shadowed.value, b"encrypted-value".to_vec());

    // And it must agree with the merged map for the same key.
    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert_eq!(env_map["SHADOWED"].value, shadowed.value);
    assert_eq!(
        env_map["SHADOWED"].secret_nonce, shadowed.secret_nonce,
        "both read paths must agree on the same key"
    );

    // A variable-only key is still reachable through the single-key path.
    let var_only = harness
        .resolver()
        .get_env_var_by_entry_key(&dataset_id, "VAR_ONLY")
        .await
        .unwrap();
    assert!(var_only.secret_nonce.is_none());
    assert_eq!(std::str::from_utf8(&var_only.value).unwrap(), "var-only");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_variable_sets_labelled_for_other_datasets_are_ignored() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, other_dataset_id) = odf::DatasetID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");

    let now = Utc::now();

    harness.seed_dataset_entry(&dataset_id, &account.did).await;

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
                    account_id: account.did.clone(),
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

// A stranger may stamp any dataset DID on a resource they own -- nothing
// validates the label value on write. Only the dataset owner's resources may
// resolve, or a stranger could inject variables into someone else's ingest, or
// shadow the owner's with a `SecretSet`.
#[test_log::test(tokio::test)]
async fn test_sets_owned_by_another_account_are_ignored() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();

    let owner = odf::AccountHandle::new_test("dataset-owner");
    let attacker = odf::AccountHandle::new_test("attacker");

    let now = Utc::now();

    harness.seed_dataset_entry(&dataset_id, &owner.did).await;

    let owned = harness
        .seed_variable_set_targeting(&owner, &dataset_id, "owned", now)
        .await;
    // Older, and a secret set: it would win on both counts if ownership were
    // not enforced.
    let foreign = harness
        .seed_secret_set_targeting(&attacker, &dataset_id, "foreign", now - Duration::hours(1))
        .await;

    harness
        .variable_set_projection_repo()
        .replace_entries(
            &owned,
            1,
            &[VariableSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: owner.did.clone(),
                key: "TOKEN".to_string(),
                value: "legitimate".to_string(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    harness
        .secret_set_projection_repo()
        .replace_entries(
            &foreign,
            1,
            &[SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: attacker.did.clone(),
                key: "TOKEN".to_string(),
                value: b"injected".to_vec(),
                secret_nonce: b"nonce".to_vec(),
                created_at: now - Duration::hours(1),
                updated_at: now - Duration::hours(1),
            }],
        )
        .await
        .unwrap();

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    assert_eq!(
        std::str::from_utf8(&env_map["TOKEN"].value).unwrap(),
        "legitimate",
        "a foreign account's labelled set must not override the owner's"
    );
    assert_eq!(env_map.len(), 1);

    // The single-key path must agree; it is the one backing `exposedValue`.
    let single = harness
        .resolver()
        .get_env_var_by_entry_key(&dataset_id, "TOKEN")
        .await
        .unwrap();
    assert!(
        single.secret_nonce.is_none(),
        "the foreign SecretSet must not be reachable through the single-key path"
    );
    assert_eq!(std::str::from_utf8(&single.value).unwrap(), "legitimate");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
