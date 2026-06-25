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
// Scenario: dry-run safety (QA scenario 5)
//
// Proves dry-run previews do not mutate state for create, update, and delete
// paths. Wired (via `kamu_cli_resource_e2e_test!`) to run against both the
// local and remote contexts.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_dry_run_safety(ctx: ResourceCtx) {
    let resource_name = "dry-run-vars";
    let initial_value = "initial-value";
    let updated_value = "updated-value";

    // ── 1. Create dry-run previews creation but leaves the resource absent ───
    ctx.assert_resource_absent("vs", resource_name).await;

    let initial_manifest = fixtures::variable_set_manifest_yaml(resource_name, initial_value);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin", "--dry-run"],
        &initial_manifest,
        Some(&[
            r#"Created \(dry-run\): STDIN -> VariableSet/dry-run-vars"#,
            r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed, 0 warning\(s\)"#,
        ]),
    )
    .await;

    ctx.assert_resource_absent("vs", resource_name).await;

    // ── 2. Real create persists the initial value ────────────────────────────
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &initial_manifest,
        Some(&[
            r#"Created: STDIN -> VariableSet/dry-run-vars"#,
            r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed, 0 warning\(s\)"#,
        ]),
    )
    .await;

    let view = ctx.get_one(["get", "vs", resource_name]).await;
    assert_eq!(view.variable("MESSAGE"), Some(initial_value));

    // ── 3. Update dry-run previews update but preserves old state ────────────
    let updated_manifest = fixtures::variable_set_manifest_yaml(resource_name, updated_value);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin", "--dry-run"],
        &updated_manifest,
        Some(&[
            r#"Updated \(dry-run\): STDIN -> VariableSet/dry-run-vars"#,
            r#"Summary 1 item\(s\): 0 created, 1 updated, 0 unchanged, 0 rejected, 0 failed, 0 warning\(s\)"#,
        ]),
    )
    .await;

    // Update dry-run must not have touched the stored value.
    let view = ctx.get_one(["get", "vs", resource_name]).await;
    assert_eq!(
        view.variable("MESSAGE"),
        Some(initial_value),
        "update dry-run must preserve the original MESSAGE"
    );

    // ── 4. Delete dry-run previews deletion but leaves resource present ──────
    ctx.assert_success(
        ["delete", "vs", resource_name, "--dry-run"],
        Some(&[
            r#"Would delete: variablesets/dry-run-vars"#,
            r#"Summary 1 item\(s\): 1 would delete, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    // Delete dry-run must leave the resource (and its value) in place.
    let view = ctx.get_one(["get", "vs", resource_name]).await;
    assert_eq!(view.variable("MESSAGE"), Some(initial_value));

    // ── 5. Real delete removes the resource ──────────────────────────────────
    ctx.assert_success(
        ["delete", "vs", resource_name, "--force"],
        Some(&[
            r#"Deleted: variablesets/dry-run-vars"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    ctx.assert_resource_absent("vs", resource_name).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
