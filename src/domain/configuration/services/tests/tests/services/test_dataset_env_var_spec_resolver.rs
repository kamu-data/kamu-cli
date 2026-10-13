// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_datasets::GetDatasetEnvVarError;
use pretty_assertions::assert_eq;

use crate::tests::services::dataset_env_var_service_harness::DatasetEnvVarServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Semantics of the spec-backed resolver, which must stay identical to the
// projection-backed one in `test_dataset_env_var_resolver.rs`.
//
// This harness reconciles inline, so the read-after-write divergence itself is
// covered at the GraphQL layer by the `test_unreconciled_*` tests.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reads_a_variable_set_from_its_spec() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");
    harness.seed_dataset_entry(&dataset_id, &account.did).await;

    harness
        .apply_variable_set_targeting(&account, &dataset_id, "vars", [("X", "x-value")], [])
        .await;

    let resolved = harness
        .spec_resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved["X"].value, b"x-value".to_vec());
    assert!(resolved["X"].secret_nonce.is_none());
}

#[test_log::test(tokio::test)]
async fn test_multiple_labelled_variable_sets_merge_with_one_consistent_winner() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");
    harness.seed_dataset_entry(&dataset_id, &account.did).await;

    // Applied oldest-first, which is also the order the label index returns.
    harness
        .apply_variable_set_targeting(
            &account,
            &dataset_id,
            "older-vars",
            [("X", "from-older"), ("Y", "common")],
            [],
        )
        .await;
    harness
        .apply_variable_set_targeting(
            &account,
            &dataset_id,
            "newer-vars",
            [("X", "from-newer"), ("Z", "extra")],
            [],
        )
        .await;

    let resolved = harness
        .spec_resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    // Keys unique to either set are always present; only `X` is contested.
    assert_eq!(resolved.len(), 3);
    assert_eq!(resolved["Y"].value, b"common".to_vec());
    assert_eq!(resolved["Z"].value, b"extra".to_vec());

    // Both sets tie on `created_at` here, so the winner falls to the id
    // tiebreak. What matters is not which one wins but that both resolvers
    // agree — they must never disagree about precedence.
    let via_projection = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    assert_eq!(resolved["X"].value, via_projection["X"].value);
    assert!(
        resolved["X"].value == b"from-older".to_vec()
            || resolved["X"].value == b"from-newer".to_vec()
    );
}

#[test_log::test(tokio::test)]
async fn test_secret_overrides_variable_on_same_key() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");
    harness.seed_dataset_entry(&dataset_id, &account.did).await;

    harness
        .apply_variable_set_targeting(
            &account,
            &dataset_id,
            "vars",
            [("SHARED", "plain"), ("ONLY_VAR", "var-only")],
            [],
        )
        .await;
    harness
        .apply_secret_set_targeting(&account, &dataset_id, "secrets", [("SHARED", "secret")], [])
        .await;

    let resolved = harness
        .spec_resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    assert_eq!(resolved.len(), 2);
    // Secrets shadow variables wholesale, regardless of relative age.
    assert!(resolved["SHARED"].secret_nonce.is_some());
    assert!(resolved["ONLY_VAR"].secret_nonce.is_none());

    let exposed = resolved["SHARED"]
        .get_exposed_decrypted_value(kamu_datasets::SAMPLE_SECRETS_ENCRYPTION_KEY)
        .unwrap();
    assert_eq!(exposed, "secret");
}

#[test_log::test(tokio::test)]
async fn test_single_key_lookup_prefers_the_secret_like_the_merged_map() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");
    harness.seed_dataset_entry(&dataset_id, &account.did).await;

    harness
        .apply_variable_set_targeting(&account, &dataset_id, "vars", [("SHARED", "plain")], [])
        .await;
    harness
        .apply_secret_set_targeting(&account, &dataset_id, "secrets", [("SHARED", "secret")], [])
        .await;

    // Returning the variable here would hand back plaintext for a key the user
    // believes they have shadowed with a secret.
    let found = harness
        .spec_resolver()
        .get_env_var_by_entry_key(&dataset_id, "SHARED")
        .await
        .unwrap();

    assert!(found.secret_nonce.is_some());
    assert_eq!(
        found
            .get_exposed_decrypted_value(kamu_datasets::SAMPLE_SECRETS_ENCRYPTION_KEY)
            .unwrap(),
        "secret"
    );
}

#[test_log::test(tokio::test)]
async fn test_single_key_lookup_reports_missing_key() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");
    harness.seed_dataset_entry(&dataset_id, &account.did).await;

    harness
        .apply_variable_set_targeting(&account, &dataset_id, "vars", [("X", "x-value")], [])
        .await;

    let err = harness
        .spec_resolver()
        .get_env_var_by_entry_key(&dataset_id, "MISSING")
        .await
        .unwrap_err();

    assert_matches!(err, GetDatasetEnvVarError::NotFound(_));
}

/// Ownership scoping is a security boundary, not a filter for tidiness.
///
/// Nothing validates the label value on write, so any account may stamp any
/// dataset DID on a resource it owns. An unscoped lookup would let a stranger
/// inject variables into someone else's ingest — or shadow them with a
/// `SecretSet`, which overrides variables regardless of age.
#[test_log::test(tokio::test)]
async fn test_sets_owned_by_another_account_are_ignored() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let owner = odf::AccountHandle::new_test("owner-account");
    let stranger = odf::AccountHandle::new_test("stranger-account");
    harness.seed_dataset_entry(&dataset_id, &owner.did).await;

    harness
        .apply_variable_set_targeting(&owner, &dataset_id, "owner-vars", [("X", "owned")], [])
        .await;
    // Same dataset DID on the label, different owner.
    harness
        .apply_variable_set_targeting(
            &stranger,
            &dataset_id,
            "stranger-vars",
            [("X", "injected"), ("EVIL", "injected")],
            [],
        )
        .await;

    let resolved = harness
        .spec_resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved["X"].value, b"owned".to_vec());
    assert!(!resolved.contains_key("EVIL"));
}

#[test_log::test(tokio::test)]
async fn test_unlabelled_dataset_resolves_to_nothing() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let account = odf::AccountHandle::new_test("test-account");
    harness.seed_dataset_entry(&dataset_id, &account.did).await;

    let resolved = harness
        .spec_resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    assert!(resolved.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
