// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use crypto_utils::SecretCryptor;
use internal_error::InternalError;
use kamu_configuration::{
    CONTENT_ENCODING_AES256GCM,
    CONTENT_ENCODING_JWE,
    SecretExt,
    SecretSetResource,
    SecretSetSpec,
    SecretSetSpecInput,
};
use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_datasets::SAMPLE_SECRETS_ENCRYPTION_KEY;
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourceParams,
    ApplyResourceRejectionCategory,
    ResourceID,
    ResourceSpecFromInput,
    ResourceSpecPreApply,
    ResourceSpecPreApplyContext,
    ResourceSpecPreApplyOutcome,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn plaintext_secret(value: &str) -> odf::metadata::config::Secret {
    odf::metadata::config::Secret {
        value: value.to_string(),
        content_encoding: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Encrypt with the legacy AES-GCM scheme and pack as `hex(nonce ‖
/// ciphertext)` — the exact wire form the env-var backfill migrations emit.
fn legacy_aes256gcm_secret(plaintext: &[u8]) -> odf::metadata::config::Secret {
    use crypto_utils::{AesGcmEncryptor, Encryptor};

    let aes = AesGcmEncryptor::try_new(SAMPLE_SECRETS_ENCRYPTION_KEY).unwrap();
    let (ciphertext, nonce) = aes.encrypt_bytes(plaintext).unwrap();
    let mut combined = nonce;
    combined.extend_from_slice(&ciphertext);

    odf::metadata::config::Secret {
        value: hex::encode(combined),
        content_encoding: Some(CONTENT_ENCODING_AES256GCM.to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A well-formed JWE secret encrypted under a key other than the current one
fn wrong_key_jwe_secret(plaintext: &[u8]) -> odf::metadata::config::Secret {
    let wrong_key_cryptor = SecretCryptor::try_new("ffffffffffffffffffffffffffffffff").unwrap();

    odf::metadata::config::Secret {
        value: wrong_key_cryptor.encrypt_to_jwe(plaintext).unwrap(),
        content_encoding: Some(CONTENT_ENCODING_JWE.to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    let harness = SecretSetPreApplyHarness::new();

    let id = harness
        .apply_new(make_spec_input([
            ("API_TOKEN", plaintext_secret("my-secret-token")),
            ("DB_PASSWORD", plaintext_secret("my-db-password")),
        ]))
        .await;

    let (stored_spec, spec_json) = harness.stored_spec(&id).await;

    // All values must be encrypted — no plaintext secrets in stored form
    for (name, secret) in &stored_spec.secrets.entries {
        assert!(
            secret.is_encrypted(),
            "secret '{name}' must be encrypted in stored spec, got: {secret:?}"
        );
    }
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
    let harness = SecretSetPreApplyHarness::new();

    let id = harness
        .apply_new(make_spec_input([(
            "API_TOKEN",
            plaintext_secret("original-value"),
        )]))
        .await;
    let (encrypted_spec, _) = harness.stored_spec(&id).await;
    assert!(
        encrypted_spec.secrets.entries["API_TOKEN"].is_encrypted(),
        "first apply must produce an encrypted secret"
    );

    // Re-applying the already-encrypted spec must pass it through unchanged
    assert_eq!(
        harness
            .reapply(id, encrypted_spec.clone().into_input())
            .await,
        ApplyResourceOutcome::Untouched
    );

    let (stored_spec, _) = harness.stored_spec(&id).await;
    assert!(
        stored_spec.secrets.entries["API_TOKEN"].is_encrypted(),
        "secret must remain encrypted after idempotent re-apply"
    );
    assert_eq!(
        stored_spec.secrets.entries["API_TOKEN"].as_encrypted(),
        encrypted_spec.secrets.entries["API_TOKEN"].as_encrypted(),
        "ciphertext must be unchanged after idempotent re-apply"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_same_plaintext_is_untouched() {
    let harness = SecretSetPreApplyHarness::new();
    let spec = make_spec_input([("API_TOKEN", plaintext_secret("original-value"))]);

    let id = harness.apply_new(spec.clone()).await;
    let (stored_spec, _) = harness.stored_spec(&id).await;

    assert_eq!(
        harness.reapply(id, spec).await,
        ApplyResourceOutcome::Untouched
    );
    assert_eq!(harness.stored_spec(&id).await.0, stored_spec);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_rejects_jwe_that_does_not_decrypt_under_current_key() {
    // A `contentEncoding: jwe` secret that is well-formed but was encrypted under a
    // different key (or tampered) must be rejected as bad business input, not
    // persisted only to fail later in reconciliation or reveal.
    let harness = SecretSetPreApplyHarness::new();

    let category = harness
        .apply_new_rejected(make_spec_input([(
            "API_TOKEN",
            wrong_key_jwe_secret(b"encrypted-under-the-wrong-key"),
        )]))
        .await;

    assert_eq!(
        category,
        ApplyResourceRejectionCategory::BusinessValidationFailed
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_upgrades_legacy_aes256gcm_to_jwe() {
    // A legacy `aes256gcm` secret (as the env-var backfill migrations produce) is
    // not the canonical write-path encoding, so applying the resource as-is must
    // decrypt and re-encrypt it into `jwe` rather than leaving it untouched.
    let harness = SecretSetPreApplyHarness::new();

    let id = harness
        .apply_new(make_spec_input([(
            "API_TOKEN",
            legacy_aes256gcm_secret(b"legacy-plaintext-token"),
        )]))
        .await;

    let (stored_spec, spec_json) = harness.stored_spec(&id).await;
    let stored_secret = &stored_spec.secrets.entries["API_TOKEN"];
    assert_eq!(
        stored_secret.content_encoding.as_deref(),
        Some(CONTENT_ENCODING_JWE),
        "a legacy aes256gcm secret must be upgraded to jwe on apply, got: {stored_secret:?}"
    );

    // Plaintext must not leak into the stored spec despite the round trip through
    // the legacy encoding.
    assert!(
        !spec_json.contains("legacy-plaintext-token"),
        "plaintext must not appear in stored spec after upgrading from aes256gcm"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#[test_log::test(tokio::test)]
async fn test_apply_secret_set_current_secret_that_does_not_decrypt_is_internal_error() {
    // The current secret was accepted once, so a failure to decrypt it is a
    // technical fault, such as a changed key, and not bad input
    let harness = SecretSetPreApplyHarness::new();

    let result = harness
        .pre_apply_over(
            make_spec_input([("API_TOKEN", wrong_key_jwe_secret(b"current-value"))]),
            make_spec_input([("API_TOKEN", plaintext_secret("current-value"))]),
        )
        .await;

    assert_matches!(result, Err(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_apply_secret_set_same_plaintext_over_legacy_secret_is_reencrypted() {
    let harness = SecretSetPreApplyHarness::new();
    let current_secret = legacy_aes256gcm_secret(b"current-value");

    let prepared = harness
        .pre_apply_over(
            make_spec_input([("API_TOKEN", current_secret.clone())]),
            make_spec_input([("API_TOKEN", plaintext_secret("current-value"))]),
        )
        .await
        .unwrap();

    let prepared_secret = &prepared.secrets.entries["API_TOKEN"];
    assert_ne!(prepared_secret.value, current_secret.value);
    assert_eq!(
        prepared_secret.content_encoding.as_deref(),
        Some(CONTENT_ENCODING_JWE)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SecretSetPreApplyHarness {
    base: BaseConfigurationServiceHarness,
    account: odf::ResourceOwnerHandle,
}

impl SecretSetPreApplyHarness {
    const NAME: &str = "test-secrets";

    fn new() -> Self {
        Self {
            base: BaseConfigurationServiceHarness::new(),
            account: odf::ResourceOwnerHandle::new_test("test-owner"),
        }
    }

    async fn apply(
        &self,
        params: ApplyResourceParams<SecretSetResource>,
    ) -> ApplyResourceApplicationDecision<SecretSetResource> {
        self.base
            .apply_secret_use_case()
            .apply(params)
            .await
            .unwrap()
    }

    fn params(&self, spec: SecretSetSpecInput) -> ApplyResourceParams<SecretSetResource> {
        BaseResourceServiceHarness::make_apply_params(self.account.clone(), Some(Self::NAME), spec)
    }

    /// Applies a new secret set and returns its ID, failing the test on a rejection
    async fn apply_new(&self, spec: SecretSetSpecInput) -> ResourceID {
        match self.apply(self.params(spec)).await {
            ApplyResourceApplicationDecision::Applied(result) => result.id,
            ApplyResourceApplicationDecision::Rejected(r) => {
                panic!("apply was rejected: {}", r.message)
            }
        }
    }

    /// Applies a new secret set that must be rejected, and returns why
    async fn apply_new_rejected(&self, spec: SecretSetSpecInput) -> ApplyResourceRejectionCategory {
        match self.apply(self.params(spec)).await {
            ApplyResourceApplicationDecision::Applied(result) => {
                panic!("apply must be rejected, not applied: {result:?}")
            }
            ApplyResourceApplicationDecision::Rejected(r) => r.category,
        }
    }

    /// Applies the spec to an existing secret set, failing the test on a rejection
    async fn reapply(&self, id: ResourceID, spec: SecretSetSpecInput) -> ApplyResourceOutcome {
        let params = ApplyResourceParams {
            id: Some(id),
            ..self.params(spec)
        };
        match self.apply(params).await {
            ApplyResourceApplicationDecision::Applied(result) => result.outcome,
            ApplyResourceApplicationDecision::Rejected(r) => {
                panic!("re-apply was rejected: {}", r.message)
            }
        }
    }

    /// Prepares `new_spec` over a stored `current_spec` that the apply path
    /// does not write: a legacy or undecryptable one. Fails the test on a
    /// rejection
    async fn pre_apply_over(
        &self,
        current_spec: SecretSetSpecInput,
        new_spec: SecretSetSpecInput,
    ) -> Result<SecretSetSpecInput, InternalError> {
        let current_spec = SecretSetSpec::from_input(current_spec);
        let outcome = self
            .base
            .catalog()
            .get_one::<dyn ResourceSpecPreApply<SecretSetResource>>()
            .unwrap()
            .pre_apply(
                new_spec,
                ResourceSpecPreApplyContext {
                    account_id: &self.account.did,
                    maybe_current_spec: Some(&current_spec),
                },
            )
            .await?;
        match outcome {
            ResourceSpecPreApplyOutcome::Prepared(spec) => Ok(spec),
            ResourceSpecPreApplyOutcome::Rejected(r) => {
                panic!("pre-apply was rejected: {}", r.message)
            }
        }
    }

    /// The stored spec, and its JSON as persisted
    async fn stored_spec(&self, id: &ResourceID) -> (SecretSetSpec, String) {
        let snapshot = self
            .base
            .generic_query_svc()
            .get_snapshot_by_id(id)
            .await
            .unwrap()
            .expect("snapshot must exist after apply");
        let json = snapshot.spec.to_string();
        (serde_json::from_value(snapshot.spec).unwrap(), json)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
