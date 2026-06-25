// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{ResourceCtx, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: golden full-document `get -o json` shape (drift guard)
//
// This is the *one* place that pins the entire `get -o json` document shape:
// the `RenderedResourceViewJson` envelope, the metadata block, and the
// reconciler `status` block. Its job is to fail loudly when the render struct
// in `get_resource_command.rs` changes shape, so a human decides whether the
// change was intended.
//
// Everywhere else, resource tests assert identity (`(kind, name)`) plus the one
// field under test via `ResourceView` accessors — deliberately ignoring the
// status block and volatile fields so they don't break for the wrong reason.
// Concentrating the full-shape assertion here keeps that rigor without paying
// its brittleness at every call site.
//
// Volatile fields are stripped from the actual document before comparison:
//   - metadata.uid / account / generation / createdAt / updatedAt — per-run /
//     per-context
//   - lastReconciledAt — reconciler timestamp
//   - status.conditions[*].lastTransitionTime — reconciler timestamps
// Everything else is asserted verbatim against an expected document built from
// the fixture constants.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_golden_view(ctx: ResourceCtx) {
    // ── VariableSet ───────────────────────────────────────────────────────────

    ctx.apply_variable_set("app-vars", "hello").await;

    let actual_vs = strip_volatile(ctx.get_one(["get", "vs", "app-vars"]).await.into_json());
    pretty_assertions::assert_eq!(actual_vs, expected_variable_set("app-vars", "hello"));

    // ── SecretSet ─────────────────────────────────────────────────────────────
    //
    // The encrypted secret values (and their nonces) are non-deterministic, so
    // we cannot pin `spec.secrets.*` verbatim. We assert the full envelope +
    // status, and that the spec carries exactly the two expected secret keys in
    // encrypted form (`encrypted`/`nonce`), without pinning the ciphertext.

    ctx.apply_secret_set("app-secrets", "tok", "pw").await;

    let mut actual_ss = strip_volatile(ctx.get_one(["get", "ss", "app-secrets"]).await.into_json());
    let secrets = take_secret_keys_encrypted(&mut actual_ss);
    assert_eq!(
        secrets,
        ["API_TOKEN".to_string(), "DB_PASSWORD".to_string()],
        "SecretSet spec should carry exactly the two encrypted secret keys"
    );
    pretty_assertions::assert_eq!(
        actual_ss,
        expected_secret_set_without_secrets("app-secrets")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Expected documents
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn expected_variable_set(name: &str, message_value: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": fixtures::RESOURCE_API_VERSION,
        "kind": fixtures::VARIABLE_SET_KIND,
        "metadata": {
            "name": name,
            "description": fixtures::DEFAULT_DESCRIPTION,
            "labels": {},
            "annotations": {},
            "deletedAt": null,
        },
        "spec": { "variables": { "MESSAGE": message_value } },
        "status": {
            "conditions": [
                { "reason": "Idle", "status": "False", "type": "Reconciling", "message": null },
                { "reason": "ValidationPassed", "status": "True", "type": "Accepted", "message": null },
                { "reason": "Reconciled", "status": "True", "type": "Ready", "message": null }
            ],
            "observedGeneration": 1,
            "phase": "Ready",
            "stats": { "invalidVariables": 0, "totalVariables": 1, "validVariables": 1 }
        }
    })
}

fn expected_secret_set_without_secrets(name: &str) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": fixtures::RESOURCE_API_VERSION,
        "kind": fixtures::SECRET_SET_KIND,
        "metadata": {
            "name": name,
            "description": fixtures::DEFAULT_DESCRIPTION,
            "labels": {},
            "annotations": {},
            "deletedAt": null,
        },
        // `spec` is asserted separately: `take_secret_keys_encrypted` removes
        // the non-deterministic encrypted values, leaving `spec` empty here.
        "spec": {},
        "status": {
            "conditions": [
                { "reason": "Idle", "status": "False", "type": "Reconciling", "message": null },
                { "reason": "ValidationPassed", "status": "True", "type": "Accepted", "message": null },
                { "reason": "Reconciled", "status": "True", "type": "Ready", "message": null }
            ],
            "observedGeneration": 1,
            "phase": "Ready",
            "stats": { "totalSecrets": 2, "validSecrets": 2, "invalidSecrets": 0 }
        }
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers (local to the golden test — the only place that touches whole docs)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Remove fields that vary per run or per context so the remaining document is
/// deterministic and can be asserted verbatim.
fn strip_volatile(mut doc: serde_json::Value) -> serde_json::Value {
    if let Some(meta) = doc.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        for k in ["uid", "account", "generation", "createdAt", "updatedAt"] {
            meta.remove(k);
        }
    }
    if let Some(obj) = doc.as_object_mut() {
        obj.remove("lastReconciledAt");
        if let Some(conditions) = obj
            .get_mut("status")
            .and_then(|s| s.get_mut("conditions"))
            .and_then(|c| c.as_array_mut())
        {
            for condition in conditions {
                if let Some(c) = condition.as_object_mut() {
                    c.remove("lastTransitionTime");
                }
            }
        }
    }
    doc
}

/// Take `spec.secrets` out of a `SecretSet` document and return the sorted
/// secret keys, asserting each value is in encrypted form
/// (`encrypted`/`nonce`). The ciphertext itself is non-deterministic, so it is
/// dropped rather than pinned; after this call `spec` is empty and the rest of
/// the doc can be compared verbatim.
fn take_secret_keys_encrypted(doc: &mut serde_json::Value) -> Vec<String> {
    let rendered = doc.to_string();
    let spec = doc
        .get_mut("spec")
        .and_then(|s| s.as_object_mut())
        .unwrap_or_else(|| panic!("SecretSet doc has no `spec` object:\n{rendered}"));

    let secrets = spec
        .remove("secrets")
        .unwrap_or_else(|| panic!("SecretSet spec has no `secrets`:\n{rendered}"));
    let secrets = secrets
        .as_object()
        .unwrap_or_else(|| panic!("`spec.secrets` is not an object:\n{secrets}"));

    let mut keys: Vec<String> = secrets
        .iter()
        .map(|(key, value)| {
            assert!(
                value.get("encrypted").is_some() && value.get("nonce").is_some(),
                "secret `{key}` should be in encrypted form (encrypted/nonce):\n{value}"
            );
            key.clone()
        })
        .collect();
    keys.sort();
    keys
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
