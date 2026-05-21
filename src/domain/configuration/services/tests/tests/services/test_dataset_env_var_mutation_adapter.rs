// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::{SecretSetResource, VariableSetResource};
use kamu_configuration_services::DatasetEnvVarMutationAdapterImpl;
use kamu_datasets::{DatasetEnvVarValue, DeleteDatasetEnvVarError};
use secrecy::SecretString;

use crate::tests::services::dataset_env_var_service_harness::DatasetEnvVarServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_lazy_creation_of_variable_resource_on_first_upsert() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    harness.seed_dataset_entry(&dataset_id, &account_id).await;

    // No resources exist yet — first upsert creates them lazily
    let result = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "FOO",
            &DatasetEnvVarValue::Regular("bar".into()),
        )
        .await
        .unwrap();

    DatasetEnvVarServiceHarness::assert_upsert_created(&result);

    // A managed VariableSet resource must have been created
    let bindings = harness.variable_bindings(&dataset_id).await;
    assert_eq!(bindings.len(), 1, "exactly one variable binding must exist");

    let resource_uid = bindings[0].resource_uid;
    let resource_name =
        DatasetEnvVarMutationAdapterImpl::legacy_variable_set_resource_name(&dataset_id);

    let found = harness
        .resource_uid_by_name(
            &account_id,
            VariableSetResource::RESOURCE_TYPE,
            &resource_name,
        )
        .await;
    assert_eq!(found, Some(resource_uid));

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert_eq!(std::str::from_utf8(&env_map["FOO"].value).unwrap(), "bar");

    // Second upsert for the same key → Updated status, still one binding
    let result2 = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "FOO",
            &DatasetEnvVarValue::Regular("bar2".into()),
        )
        .await
        .unwrap();

    DatasetEnvVarServiceHarness::assert_upsert_updated(&result2);
    assert_eq!(
        harness.variable_bindings(&dataset_id).await.len(),
        1,
        "still exactly one binding after update"
    );

    let env_map2 = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert_eq!(std::str::from_utf8(&env_map2["FOO"].value).unwrap(), "bar2");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_lazy_creation_of_secret_resource_on_first_upsert() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    harness.seed_dataset_entry(&dataset_id, &account_id).await;

    let result = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "MY_SECRET",
            &DatasetEnvVarValue::Secret(SecretString::from("s3cr3t".to_string())),
        )
        .await
        .unwrap();

    DatasetEnvVarServiceHarness::assert_upsert_created(&result);
    assert!(result.dataset_env_var.secret_nonce.is_some());

    let sec_bindings = harness.secret_bindings(&dataset_id).await;
    assert_eq!(
        sec_bindings.len(),
        1,
        "exactly one secret binding must exist"
    );

    let resource_name =
        DatasetEnvVarMutationAdapterImpl::legacy_secret_set_resource_name(&dataset_id);

    let found = harness
        .resource_uid_by_name(
            &account_id,
            SecretSetResource::RESOURCE_TYPE,
            &resource_name,
        )
        .await;
    assert_eq!(found, Some(sec_bindings[0].resource_uid));

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert!(
        env_map["MY_SECRET"].secret_nonce.is_some(),
        "resolved entry must be a secret"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_last_variable_removes_resource_and_binding() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    harness.seed_dataset_entry(&dataset_id, &account_id).await;

    // Upsert two vars
    let r_a = harness
        .mutation_adapter()
        .upsert_env_var(&dataset_id, "A", &DatasetEnvVarValue::Regular("va".into()))
        .await
        .unwrap();
    DatasetEnvVarServiceHarness::assert_upsert_created(&r_a);

    let r_b = harness
        .mutation_adapter()
        .upsert_env_var(&dataset_id, "B", &DatasetEnvVarValue::Regular("vb".into()))
        .await
        .unwrap();
    DatasetEnvVarServiceHarness::assert_upsert_created(&r_b);

    // Binding and resource must exist at this point
    assert_eq!(harness.variable_bindings(&dataset_id).await.len(), 1);

    // Delete A — resource stays, binding stays, B is still visible
    harness
        .mutation_adapter()
        .delete_env_var(&dataset_id, "A")
        .await
        .unwrap();

    assert_eq!(
        harness.variable_bindings(&dataset_id).await.len(),
        1,
        "binding must still exist after partial deletion"
    );
    let env_after_a = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert!(!env_after_a.contains_key("A"));
    assert!(env_after_a.contains_key("B"));

    // Delete B — last entry, resource and binding must be removed
    harness
        .mutation_adapter()
        .delete_env_var(&dataset_id, "B")
        .await
        .unwrap();

    assert_eq!(
        harness.variable_bindings(&dataset_id).await.len(),
        0,
        "binding must be removed when last entry is deleted"
    );

    let resource_name =
        DatasetEnvVarMutationAdapterImpl::legacy_variable_set_resource_name(&dataset_id);

    let found = harness
        .resource_uid_by_name(
            &account_id,
            VariableSetResource::RESOURCE_TYPE,
            &resource_name,
        )
        .await;
    assert_eq!(found, None, "managed resource must be deleted");

    let env_empty = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert!(env_empty.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_last_secret_removes_resource_and_binding() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    harness.seed_dataset_entry(&dataset_id, &account_id).await;

    // Upsert two secrets
    let r_a = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "SEC_A",
            &DatasetEnvVarValue::Secret(SecretString::from("secret-a".to_string())),
        )
        .await
        .unwrap();
    DatasetEnvVarServiceHarness::assert_upsert_created(&r_a);

    let r_b = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "SEC_B",
            &DatasetEnvVarValue::Secret(SecretString::from("secret-b".to_string())),
        )
        .await
        .unwrap();
    DatasetEnvVarServiceHarness::assert_upsert_created(&r_b);

    assert_eq!(harness.secret_bindings(&dataset_id).await.len(), 1);

    // Delete SEC_A — resource and binding must still exist
    harness
        .mutation_adapter()
        .delete_env_var(&dataset_id, "SEC_A")
        .await
        .unwrap();

    assert_eq!(
        harness.secret_bindings(&dataset_id).await.len(),
        1,
        "binding must still exist after partial deletion"
    );
    let env_after_a = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert!(!env_after_a.contains_key("SEC_A"));
    assert!(env_after_a.contains_key("SEC_B"));

    // Delete SEC_B — last secret; resource and binding must be removed
    harness
        .mutation_adapter()
        .delete_env_var(&dataset_id, "SEC_B")
        .await
        .unwrap();

    assert_eq!(
        harness.secret_bindings(&dataset_id).await.len(),
        0,
        "binding must be removed when last secret is deleted"
    );

    let resource_name =
        DatasetEnvVarMutationAdapterImpl::legacy_secret_set_resource_name(&dataset_id);
    let found = harness
        .resource_uid_by_name(
            &account_id,
            SecretSetResource::RESOURCE_TYPE,
            &resource_name,
        )
        .await;
    assert_eq!(found, None, "managed secret resource must be deleted");

    let env_empty = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();
    assert!(env_empty.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_upsert_converts_variable_to_secret() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    harness.seed_dataset_entry(&dataset_id, &account_id).await;

    // Start as a regular variable
    let r1 = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "FOO",
            &DatasetEnvVarValue::Regular("plain".into()),
        )
        .await
        .unwrap();
    DatasetEnvVarServiceHarness::assert_upsert_created(&r1);
    assert_eq!(harness.variable_bindings(&dataset_id).await.len(), 1);
    assert_eq!(harness.secret_bindings(&dataset_id).await.len(), 0);

    // Re-upsert the same key as a secret — must convert
    let r2 = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "FOO",
            &DatasetEnvVarValue::Secret(SecretString::from("s3cr3t".to_string())),
        )
        .await
        .unwrap();
    // Key existed before (as a variable), so status is Updated
    DatasetEnvVarServiceHarness::assert_upsert_updated(&r2);
    assert!(
        r2.dataset_env_var.secret_nonce.is_some(),
        "converted entry must carry a secret_nonce"
    );

    // Variable resource must be gone (or at least not hold FOO anymore)
    assert_eq!(
        harness.secret_bindings(&dataset_id).await.len(),
        1,
        "secret binding must exist after conversion"
    );

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    let foo = env_map.get("FOO").expect("FOO must still be resolvable");
    assert!(
        foo.secret_nonce.is_some(),
        "FOO must now be a secret in the effective env map"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_upsert_converts_secret_to_variable() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    harness.seed_dataset_entry(&dataset_id, &account_id).await;

    // Start as a secret
    let r1 = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "BAR",
            &DatasetEnvVarValue::Secret(SecretString::from("topsecret".to_string())),
        )
        .await
        .unwrap();
    DatasetEnvVarServiceHarness::assert_upsert_created(&r1);
    assert_eq!(harness.secret_bindings(&dataset_id).await.len(), 1);
    assert_eq!(harness.variable_bindings(&dataset_id).await.len(), 0);

    // Re-upsert the same key as a regular variable — must convert
    let r2 = harness
        .mutation_adapter()
        .upsert_env_var(
            &dataset_id,
            "BAR",
            &DatasetEnvVarValue::Regular("open".into()),
        )
        .await
        .unwrap();
    // Key existed before (as a secret), so status is Updated
    DatasetEnvVarServiceHarness::assert_upsert_updated(&r2);
    assert!(
        r2.dataset_env_var.secret_nonce.is_none(),
        "converted entry must not carry a secret_nonce"
    );

    assert_eq!(
        harness.variable_bindings(&dataset_id).await.len(),
        1,
        "variable binding must exist after conversion"
    );

    let env_map = harness
        .resolver()
        .resolve_effective_env_vars(&dataset_id)
        .await
        .unwrap();

    let bar = env_map.get("BAR").expect("BAR must still be resolvable");
    assert!(
        bar.secret_nonce.is_none(),
        "BAR must now be a plain variable in the effective env map"
    );
    assert_eq!(std::str::from_utf8(&bar.value).unwrap(), "open");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_nonexistent_key_returns_not_found() {
    let harness = DatasetEnvVarServiceHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    harness.seed_dataset_entry(&dataset_id, &account_id).await;

    let err = harness
        .mutation_adapter()
        .delete_env_var(&dataset_id, "NONEXISTENT")
        .await
        .unwrap_err();

    assert!(
        matches!(err, DeleteDatasetEnvVarError::NotFound(_)),
        "expected NotFound error, got: {err:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
