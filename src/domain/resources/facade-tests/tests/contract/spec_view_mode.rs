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

use kamu_configuration::SecretSetResource;
use kamu_resources::{ResourceRef, ResourceSchemaProvider, TypeName};
use kamu_resources_facade::{ApplyManifestRequest, ResourceManifestFormat, SpecViewMode};
use pretty_assertions::assert_eq;

use crate::contract_test;
use crate::harness::{FacadeContractHarness, TestAccount};
use crate::helpers::{
    SECRET_SET_CANONICAL_SELECTOR,
    SECRET_SET_SCHEMA_STR,
    VARIABLE_SET_CANONICAL_SELECTOR,
    assert_batch_indexes,
    assert_single_batch_success,
    create_secret_set,
    variable_set_manifest_json,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn secret_selector(name: &str) -> ResourceRef {
    ResourceRef {
        account: None,
        r#type: Some(
            SECRET_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: None,
        did: None,
        name: Some(name.parse().unwrap()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-040
contract_test!(
    encrypted_spec_view_hides_secret_material,
    super::test_encrypted_spec_view_hides_secret_material
);

pub async fn test_encrypted_spec_view_hides_secret_material(h: &impl FacadeContractHarness) {
    create_secret_set(
        h,
        TestAccount::Alice,
        "sv-encrypted",
        &[("API_TOKEN", "my-plaintext-secret")],
    )
    .await;
    let facade = h.facade_for(TestAccount::Alice);

    let view = assert_single_batch_success(
        facade
            .get(
                vec![secret_selector("sv-encrypted")],
                SpecViewMode::Encrypted,
            )
            .await
            .unwrap(),
    );

    // The spec must NOT contain the raw plaintext
    let spec_str = serde_json::to_string(&view.spec).unwrap();
    assert!(
        !spec_str.contains("my-plaintext-secret"),
        "Encrypted view must not expose plaintext secret; spec: {spec_str}"
    );

    // The secret entry must be present as an encrypted JWE value
    // (`{ value, contentEncoding: "jwe" }`).
    let secrets = &view.spec["secrets"];
    let token = &secrets["API_TOKEN"];
    assert_eq!(
        token["contentEncoding"], "jwe",
        "Encrypted view must tag the secret as JWE; spec: {spec_str}"
    );
    assert!(
        token["value"].as_str().is_some_and(|v| !v.is_empty()),
        "Encrypted view must carry a non-empty JWE token; spec: {spec_str}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-041
contract_test!(
    revealed_spec_view_exposes_plaintext,
    super::test_revealed_spec_view_exposes_plaintext
);

pub async fn test_revealed_spec_view_exposes_plaintext(h: &impl FacadeContractHarness) {
    create_secret_set(
        h,
        TestAccount::Alice,
        "sv-revealed",
        &[("API_TOKEN", "reveal-me-secret")],
    )
    .await;
    let facade = h.facade_for(TestAccount::Alice);

    let view = assert_single_batch_success(
        facade
            .get(vec![secret_selector("sv-revealed")], SpecViewMode::Revealed)
            .await
            .unwrap(),
    );

    let spec_str = serde_json::to_string(&view.spec).unwrap();
    assert!(
        spec_str.contains("reveal-me-secret"),
        "Revealed view must expose plaintext secret; spec: {spec_str}"
    );

    // Non-secret identity fields are unchanged
    assert_eq!(view.headers.name, "sv-revealed");
    assert_eq!(view.schema, *SecretSetResource::schema());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-042
contract_test!(
    spec_view_mode_applies_to_batch_get,
    super::test_spec_view_mode_applies_to_batch_get
);

pub async fn test_spec_view_mode_applies_to_batch_get(h: &impl FacadeContractHarness) {
    let id_a = create_secret_set(
        h,
        TestAccount::Alice,
        "sv-batch-a",
        &[("TOKEN_A", "secret-a-value")],
    )
    .await;
    let id_b = create_secret_set(
        h,
        TestAccount::Alice,
        "sv-batch-b",
        &[("TOKEN_B", "secret-b-value")],
    )
    .await;
    let facade = h.facade_for(TestAccount::Alice);

    let batch_selector = vec![
        ResourceRef {
            account: None,
            r#type: Some(
                SECRET_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: Some(id_a),
            did: None,
            name: None,
        },
        ResourceRef {
            account: None,
            r#type: Some(
                SECRET_SET_CANONICAL_SELECTOR
                    .parse::<TypeName>()
                    .unwrap()
                    .into(),
            ),
            id: Some(id_b),
            did: None,
            name: None,
        },
    ];

    // Encrypted view — no plaintext
    let enc_resp = facade
        .get(batch_selector.clone(), SpecViewMode::Encrypted)
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
        .get(batch_selector, SpecViewMode::Revealed)
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
    create_secret_set(
        h,
        TestAccount::Alice,
        "sv-render",
        &[("RENDER_SECRET", "render-secret-value")],
    )
    .await;
    let facade = h.facade_for(TestAccount::Alice);

    // Encrypted render — no plaintext
    let enc_result = assert_single_batch_success(
        facade
            .render_manifests(
                vec![secret_selector("sv-render")],
                ResourceManifestFormat::Json,
                SpecViewMode::Encrypted,
            )
            .await
            .unwrap(),
    );
    assert!(
        !enc_result.manifest.contains("render-secret-value"),
        "Encrypted rendered manifest must not expose plaintext; manifest: {}",
        enc_result.manifest
    );

    // Revealed render — plaintext visible
    let rev_result = assert_single_batch_success(
        facade
            .render_manifests(
                vec![secret_selector("sv-render")],
                ResourceManifestFormat::Json,
                SpecViewMode::Revealed,
            )
            .await
            .unwrap(),
    );
    assert!(
        rev_result.manifest.contains("render-secret-value"),
        "Revealed rendered manifest must expose plaintext; manifest: {}",
        rev_result.manifest
    );

    // Parsed revealed manifest has the secret in the spec
    let parsed: serde_json::Value =
        serde_json::from_str(&rev_result.manifest).expect("must be valid JSON");
    assert_eq!(parsed["$schema"], SECRET_SET_SCHEMA_STR, "schema mismatch");
    assert!(
        parsed["headers"]["id"].is_null(),
        "rendered manifest must not include id"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// RF-173
contract_test!(
    spec_view_mode_applies_per_schema_in_a_mixed_batch,
    super::test_spec_view_mode_applies_per_schema_in_a_mixed_batch
);

/// Both orders are asserted because the two directions fail differently: with
/// the `VariableSet` first the secret stays encrypted, and with the `SecretSet`
/// first the secret dispatcher is handed a `VariableSet` spec it cannot parse.
/// A per-schema lookup is the only thing that satisfies both.
pub async fn test_spec_view_mode_applies_per_schema_in_a_mixed_batch(
    h: &impl FacadeContractHarness,
) {
    create_secret_set(
        h,
        TestAccount::Alice,
        "sv-mixed-secret",
        &[("MIXED_TOKEN", "mixed-secret-value")],
    )
    .await;

    let facade = h.facade_for(TestAccount::Alice);
    facade
        .apply_manifest(ApplyManifestRequest {
            format: ResourceManifestFormat::Json,
            manifest: variable_set_manifest_json("sv-mixed-vars", None, &[("MIXED_VAR", "plain")]),
        })
        .await
        .unwrap();

    let secret_ref = secret_selector("sv-mixed-secret");
    let variable_ref = ResourceRef {
        account: None,
        r#type: Some(
            VARIABLE_SET_CANONICAL_SELECTOR
                .parse::<TypeName>()
                .unwrap()
                .into(),
        ),
        id: None,
        did: None,
        name: Some("sv-mixed-vars".parse().unwrap()),
    };

    for (label, refs) in [
        (
            "variable set first",
            vec![variable_ref.clone(), secret_ref.clone()],
        ),
        (
            "secret set first",
            vec![secret_ref.clone(), variable_ref.clone()],
        ),
    ] {
        let response = facade
            .get(refs.clone(), SpecViewMode::Revealed)
            .await
            .unwrap_or_else(|e| panic!("{label}: revealed get_many must succeed, got {e:?}"));
        assert_batch_indexes(&response, &[0, 1], &[]);

        let specs: String = response
            .successes
            .iter()
            .map(|s| serde_json::to_string(&s.item.spec).unwrap())
            .collect();
        assert!(
            specs.contains("mixed-secret-value"),
            "{label}: the secret must be revealed even beside another type; specs: {specs}"
        );

        let rendered = facade
            .render_manifests(refs, ResourceManifestFormat::Json, SpecViewMode::Revealed)
            .await
            .unwrap_or_else(|e| {
                panic!("{label}: revealed render_manifests must succeed, got {e:?}")
            });
        assert_batch_indexes(&rendered, &[0, 1], &[]);

        let manifests: String = rendered
            .successes
            .iter()
            .map(|s| s.item.manifest.clone())
            .collect();
        assert!(
            manifests.contains("mixed-secret-value"),
            "{label}: rendering must reveal the secret beside another type; manifests: {manifests}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
