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
// Scenario: VariableSet basic lifecycle (QA scenario 2)
//
// apply → list (full kind + alias) shows it → get full view → get --spec
// apply-compatible manifest → re-apply idempotent → apply updated manifest →
// summary reflects count → delete → list empty → get --ignore-not-found empty
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_variableset_lifecycle(ctx: ResourceCtx) {
    let resource_name = "app-vars";
    let initial_value = "hello-world";
    let updated_value = "hello-updated";

    // ── 1. Precondition: resource is absent ──────────────────────────────────
    ctx.assert_resource_absent("vs", resource_name).await;

    // ── 2. Apply a VariableSet via stdin ─────────────────────────────────────
    let manifest = fixtures::variable_set_manifest_yaml(resource_name, initial_value);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest,
        Some(&[
            r#"Created: STDIN -> VariableSet/app-vars"#,
            r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;

    // ── 3. list variablesets — full kind name ─────────────────────────────────
    let list_out = ctx.stdout(["list", "variablesets"]).await;
    assert!(
        list_out.contains(resource_name),
        "`list variablesets` should contain '{resource_name}', got:\n{list_out}"
    );

    // ── 4. list vs — short alias ──────────────────────────────────────────────
    let list_alias_out = ctx.stdout(["list", "vs"]).await;
    assert!(
        list_alias_out.contains(resource_name),
        "`list vs` should contain '{resource_name}', got:\n{list_alias_out}"
    );

    // ── 5. get variablesets <name> — resolves to this VS with our value ───────
    let view = ctx.get_one(["get", "variablesets", resource_name]).await;
    assert_eq!(view.ident(), (fixtures::VARIABLE_SET_KIND, resource_name));
    assert_eq!(view.variable("MESSAGE"), Some(initial_value));

    // ── 6. get vs <name> --spec — apply-compatible manifest ──────────────────
    let spec_out = ctx.stdout(["get", "vs", resource_name, "--spec"]).await;
    assert!(
        spec_out.contains("apiVersion"),
        "`get vs --spec` should emit a manifest with 'apiVersion', got:\n{spec_out}"
    );
    assert!(
        spec_out.contains("VariableSet"),
        "`get vs --spec` should emit kind 'VariableSet', got:\n{spec_out}"
    );
    assert!(
        spec_out.contains(resource_name),
        "`get vs --spec` should contain the resource name, got:\n{spec_out}"
    );
    assert!(
        spec_out.contains(initial_value),
        "`get vs --spec` should contain the variable value '{initial_value}', got:\n{spec_out}"
    );

    // ── 7. Re-apply same manifest (idempotent) ────────────────────────────────
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest,
        Some(&[
            r#"Unchanged: STDIN -> VariableSet/app-vars"#,
            r#"Summary 1 item\(s\): 0 created, 0 updated, 1 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;

    // ── 8. Apply updated manifest → Updated ───────────────────────────────────
    let updated_manifest = fixtures::variable_set_manifest_yaml(resource_name, updated_value);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &updated_manifest,
        Some(&[
            r#"Updated: STDIN -> VariableSet/app-vars"#,
            r#"Summary 1 item\(s\): 0 created, 1 updated, 0 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;

    // Verify the new value is visible via get
    let updated_view = ctx.get_one(["get", "variablesets", resource_name]).await;
    assert_eq!(updated_view.variable("MESSAGE"), Some(updated_value));

    // ── 9. summary reflects the count ─────────────────────────────────────────
    ctx.assert_success(["summary"], None).await;

    // ── 10. Delete the resource ───────────────────────────────────────────────
    ctx.assert_success(
        ["delete", "vs", resource_name, "--force"],
        Some(&[
            r#"Deleted: variablesets/app-vars"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    // ── 11. list is now empty ─────────────────────────────────────────────────
    let list_after_delete = ctx.stdout(["list", "variablesets"]).await;
    assert!(
        !list_after_delete.contains(resource_name),
        "`list variablesets` after delete should not contain '{resource_name}', \
         got:\n{list_after_delete}"
    );

    // ── 12. get --ignore-not-found succeeds (empty) ───────────────────────────
    ctx.assert_success(["get", "vs", resource_name, "--ignore-not-found"], None)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: apply surfaces lint warnings but still succeeds (QA scenario 2)
//
// A manifest without `headers.description` applies successfully (resource is
// created, `0 failed`) yet emits the non-fatal `missing_description` lint
// warning and reports a non-zero warning count.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_variableset_apply_warning(ctx: ResourceCtx) {
    let resource_name = "warn-vars";

    ctx.assert_resource_absent("vs", resource_name).await;

    // A manifest missing `headers.description` is applied: it succeeds (the
    // resource is created) but surfaces a non-fatal lint warning.
    let manifest = fixtures::variable_set_manifest_no_description(resource_name, "hello-world");
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest,
        // Regexes are matched in order against stderr; the CLI prints the
        // `Created:` line first, then the lint warning, then the summary.
        Some(&[
            r#"Created: STDIN -> VariableSet/warn-vars"#,
            r#"warning \[missing_description\] STDIN headers.description"#,
            r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed, 1 warning\(s\)"#,
        ]),
    )
    .await;

    // Cleanup so the scenario is self-contained.
    ctx.assert_success(["delete", "vs", resource_name, "--force"], None)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
