// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Spec view mode contract tests (RF-040..043).
//!
//! `SecretSetResource` is used because its spec has secret fields affected by
//! `SpecViewMode::Encrypted` vs `SpecViewMode::Revealed`.  After `apply`, the
//! `Value` variant is automatically encrypted and stored as `Encrypted`.
//! `Encrypted` (default) returns the ciphertext blob; `Revealed` decrypts it
//! back to the `Literal` plaintext.

use kamu_resources::ApplyResourceOutcome;
use kamu_resources_facade::{
    ApplyManifestRequest,
    ResourceBatchSelector,
    ResourceManifestFormat,
    ResourceRef,
    ResourceSelector,
    SpecViewMode,
};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_API_VERSION,
    SECRET_SET_KIND,
    assert_applied_outcome,
    assert_batch_indexes,
    secret_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn secret_selector(name: &str) -> ResourceSelector {
    ResourceSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        api_version: Some(SECRET_SET_API_VERSION.to_string()),
        resource_ref: ResourceRef::ByName(name.to_string()),
    }
}

async fn create_secret_resource(
    h: &impl FacadeContractHarness,
    name: &str,
    secrets: &[(&str, &str)],
) -> kamu_resources::ResourceUID {
    let facade = h.facade_for(TestAccount::Alice);
    let decision = facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: secret_set_manifest_json(name, None, secrets),
        })
        .await
        .unwrap();
    assert_applied_outcome(&decision, ApplyResourceOutcome::Created)
        .headers
        .uid
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-040
contract_test!(
    encrypted_spec_view_hides_secret_material,
    super::test_encrypted_spec_view_hides_secret_material
);

pub async fn test_encrypted_spec_view_hides_secret_material(h: &impl FacadeContractHarness) {
    create_secret_resource(h, "sv-encrypted", &[("API_TOKEN", "my-plaintext-secret")]).await;
    let facade = h.facade_for(TestAccount::Alice);

    let view = facade
        .get(secret_selector("sv-encrypted"), SpecViewMode::Encrypted)
        .await
        .unwrap();

    // The spec must NOT contain the raw plaintext
    let spec_str = serde_json::to_string(&view.spec).unwrap();
    assert!(
        !spec_str.contains("my-plaintext-secret"),
        "Encrypted view must not expose plaintext secret; spec: {spec_str}"
    );

    // The secret entry must be present as an encrypted blob (has "encrypted" key)
    let secrets = &view.spec["secrets"];
    let token = &secrets["API_TOKEN"];
    assert!(
        !token["encrypted"].is_null(),
        "Encrypted view must return an encrypted blob; spec: {spec_str}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-041
contract_test!(
    revealed_spec_view_exposes_plaintext,
    super::test_revealed_spec_view_exposes_plaintext
);

pub async fn test_revealed_spec_view_exposes_plaintext(h: &impl FacadeContractHarness) {
    create_secret_resource(h, "sv-revealed", &[("API_TOKEN", "reveal-me-secret")]).await;
    let facade = h.facade_for(TestAccount::Alice);

    let view = facade
        .get(secret_selector("sv-revealed"), SpecViewMode::Revealed)
        .await
        .unwrap();

    let spec_str = serde_json::to_string(&view.spec).unwrap();
    assert!(
        spec_str.contains("reveal-me-secret"),
        "Revealed view must expose plaintext secret; spec: {spec_str}"
    );

    // Non-secret identity fields are unchanged
    assert_eq!(view.headers.name, "sv-revealed");
    assert_eq!(view.kind, SECRET_SET_KIND);
    assert_eq!(view.api_version, SECRET_SET_API_VERSION);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-042
contract_test!(
    spec_view_mode_applies_to_batch_get,
    super::test_spec_view_mode_applies_to_batch_get
);

pub async fn test_spec_view_mode_applies_to_batch_get(h: &impl FacadeContractHarness) {
    let uid_a = create_secret_resource(h, "sv-batch-a", &[("TOKEN_A", "secret-a-value")]).await;
    let uid_b = create_secret_resource(h, "sv-batch-b", &[("TOKEN_B", "secret-b-value")]).await;
    let facade = h.facade_for(TestAccount::Alice);

    let batch_selector = ResourceBatchSelector {
        account: None,
        kind: SECRET_SET_KIND.to_string(),
        api_version: Some(SECRET_SET_API_VERSION.to_string()),
        resource_refs: vec![ResourceRef::ById(uid_a), ResourceRef::ById(uid_b)],
    };

    // Encrypted view — no plaintext
    let enc_resp = facade
        .get_many(batch_selector.clone(), SpecViewMode::Encrypted)
        .await
        .unwrap();
    assert_batch_indexes(&enc_resp, &[0, 1], &[]);
    for s in &enc_resp.successes {
        let spec_str = serde_json::to_string(&s.item.spec).unwrap();
        assert!(
            !spec_str.contains("secret-a-value") && !spec_str.contains("secret-b-value"),
            "Encrypted batch view must not expose plaintext; spec: {spec_str}"
        );
    }

    // Revealed view — plaintext visible
    let rev_resp = facade
        .get_many(batch_selector, SpecViewMode::Revealed)
        .await
        .unwrap();
    assert_batch_indexes(&rev_resp, &[0, 1], &[]);
    let all_specs: String = rev_resp
        .successes
        .iter()
        .map(|s| serde_json::to_string(&s.item.spec).unwrap())
        .collect();
    assert!(
        all_specs.contains("secret-a-value") && all_specs.contains("secret-b-value"),
        "Revealed batch view must expose both plaintext secrets; combined specs: {all_specs}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-043
contract_test!(
    spec_view_mode_applies_to_render,
    super::test_spec_view_mode_applies_to_render
);

pub async fn test_spec_view_mode_applies_to_render(h: &impl FacadeContractHarness) {
    create_secret_resource(h, "sv-render", &[("RENDER_SECRET", "render-secret-value")]).await;
    let facade = h.facade_for(TestAccount::Alice);

    // Encrypted render — no plaintext
    let enc_result = facade
        .render_manifest(
            secret_selector("sv-render"),
            ResourceManifestFormat::Json,
            SpecViewMode::Encrypted,
        )
        .await
        .unwrap();
    assert!(
        !enc_result.manifest.contains("render-secret-value"),
        "Encrypted rendered manifest must not expose plaintext; manifest: {}",
        enc_result.manifest
    );

    // Revealed render — plaintext visible
    let rev_result = facade
        .render_manifest(
            secret_selector("sv-render"),
            ResourceManifestFormat::Json,
            SpecViewMode::Revealed,
        )
        .await
        .unwrap();
    assert!(
        rev_result.manifest.contains("render-secret-value"),
        "Revealed rendered manifest must expose plaintext; manifest: {}",
        rev_result.manifest
    );

    // Parsed revealed manifest has the secret in the spec
    let parsed: serde_json::Value =
        serde_json::from_str(&rev_result.manifest).expect("must be valid JSON");
    assert_eq!(parsed["kind"], SECRET_SET_KIND);
    assert!(
        parsed["headers"]["uid"].is_null(),
        "rendered manifest must not include uid"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
