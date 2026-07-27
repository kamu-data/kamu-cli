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
// Scenario: extension-schema behavior via the real CLI/facade path (Phase 6)
//
// Locks in, end-to-end, the schema-driven label/annotation behavior already
// implemented at the resolver/facade level (Phases 1-5) — canonicalization,
// canonicalization-before-diffing, the "unknown URI is an error" rule,
// schema-driven description length, and the free-form/structured warnings.
// None of this depends on `LabelFilter`/`--selector`, so it does not need to
// wait for the label-filtering phases. Wired (via
// `kamu_cli_resource_e2e_test!`) to run against both the local and remote
// contexts.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_extension_schema_canonicalization(ctx: ResourceCtx) {
    let name = "env-label-vars";

    // ── 1. Canonicalization on write: author by short name ───────────────────
    let manifest_by_short_name =
        fixtures::variable_set_manifest_with_environment_label(name, "production", "environment");
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest_by_short_name,
        Some(&[r#"Created: STDIN -> VariableSet/env-label-vars"#]),
    )
    .await;

    // Stored under the canonical schema URI, not the short name: there is
    // exactly one persisted key per extension, so the short name must not
    // appear at rest at all.
    let view = ctx.get_one(["get", "vs", name]).await;
    assert_eq!(
        view.label_str(fixtures::ENVIRONMENT_LABEL_SCHEMA),
        Some("production"),
        "environment label must be stored under its canonical schema URI"
    );
    assert_eq!(
        view.label("environment"),
        None,
        "the authored short name must not survive as a separate stored key"
    );

    // ── 2. Canonicalization precedes diffing: re-apply, still by short name ──
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest_by_short_name,
        Some(&[
            r#"Unchanged: STDIN -> VariableSet/env-label-vars"#,
            r#"Summary 1 item\(s\): 0 created, 0 updated, 1 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;

    // ── 3. Re-apply once more authoring the already-canonical URI key ────────
    let manifest_by_uri = fixtures::variable_set_manifest_with_environment_label(
        name,
        "production",
        fixtures::ENVIRONMENT_LABEL_SCHEMA,
    );
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest_by_uri,
        Some(&[
            r#"Unchanged: STDIN -> VariableSet/env-label-vars"#,
            r#"Summary 1 item\(s\): 0 created, 0 updated, 1 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_extension_schema_rejections(ctx: ResourceCtx) {
    // ── Unknown URI label key is rejected ─────────────────────────────────────
    let unknown_uri_manifest =
        fixtures::variable_set_manifest_with_unknown_label_uri("unknown-uri-vars");
    ctx.assert_failure_with_stdin(
        ["apply", "--stdin"],
        &unknown_uri_manifest,
        Some(&[
            // Header validation fails before the resource type/name is
            // resolved, so the status line carries only the source, not a
            // `-> Type/name` suffix (unlike spec-validation-time failures).
            r#"Failed: STDIN"#,
            r#"unknown Label extension URI"#,
        ]),
    )
    .await;
    ctx.assert_resource_absent("vs", "unknown-uri-vars").await;

    // ── Over-long description is rejected via the schema path ────────────────
    let overlong_description_manifest =
        fixtures::variable_set_manifest_with_overlong_description("overlong-desc-vars");
    ctx.assert_failure_with_stdin(
        ["apply", "--stdin"],
        &overlong_description_manifest,
        Some(&[
            r#"Failed: STDIN"#,
            r#"Annotation extension 'description' value is invalid"#,
        ]),
    )
    .await;
    ctx.assert_resource_absent("vs", "overlong-desc-vars").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_extension_schema_warnings(ctx: ResourceCtx) {
    // ── Free-form short-name label: accepted, stored opaquely, with warning ──
    let name = "freeform-label-vars";
    let freeform_manifest = fixtures::variable_set_manifest_with_freeform_label(name);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &freeform_manifest,
        Some(&[
            r#"Created: STDIN -> VariableSet/freeform-label-vars"#,
            r#"\[resource_freeform_labels\]"#,
        ]),
    )
    .await;

    let view = ctx.get_one(["get", "vs", name]).await;
    assert_eq!(
        view.label_str("team"),
        Some("data-platform"),
        "unregistered short-name label stays free-form and opaque"
    );

    // ── Structured (non-string) label: accepted, with the not-indexed warning ─
    let structured_name = "structured-label-vars";
    let structured_manifest =
        fixtures::variable_set_manifest_with_structured_label(structured_name);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &structured_manifest,
        Some(&[
            r#"Created: STDIN -> VariableSet/structured-label-vars"#,
            r#"\[resource_label_not_indexed\]"#,
        ]),
    )
    .await;

    let view = ctx.get_one(["get", "vs", structured_name]).await;
    assert_eq!(
        view.label("coordinates").cloned(),
        Some(serde_json::json!({ "lat": 1, "lon": 2 })),
        "structured label value must be preserved as-is by `get`"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
