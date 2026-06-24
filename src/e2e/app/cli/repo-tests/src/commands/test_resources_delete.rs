// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{ResourceCtx, assert_output_contains_all};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: resource delete semantics (QA scenario 10)
//
// Covers explicit selectors, slash selectors, name patterns, kind-scoped
// `--all`, and cross-kind `all` deletion. Wired (via
// `kamu_cli_resource_e2e_test!`) to run against both local and remote contexts.
//
// IMPORTANT: this scenario applies `SecretSet` resources, so it must be wired
// with `Options::with_kamu_config(fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG)`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_delete_semantics(ctx: ResourceCtx) {
    let app_vars = "app-vars";
    let temp_vars = "temp-vars";
    let app_secrets = "app-secrets";
    let temp_secrets = "temp-secrets";

    // -- 1. Seed two VariableSets and two SecretSets --------------------------
    seed_delete_fixtures(&ctx, app_vars, temp_vars, app_secrets, temp_secrets).await;

    // -- 2. Explicit kind/name selector deletes only the selected VariableSet -
    ctx.assert_success(
        ["delete", "vs", app_vars, "--force"],
        Some(&[
            r#"Deleted: variablesets/app-vars"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    ctx.assert_resource_absent("vs", app_vars).await;
    ctx.assert_resource_present("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_present("ss", temp_secrets).await;

    // -- 3. Slash selector deletes the other VariableSet ----------------------
    ctx.assert_success(
        ["delete", "vs/temp-vars", "--force"],
        Some(&[
            r#"Deleted: variablesets/temp-vars"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    ctx.assert_resource_absent("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_present("ss", temp_secrets).await;

    // -- 4. Pattern selector deletes matching SecretSets only -----------------
    ctx.assert_success(
        ["delete", "ss", "temp-%", "--force"],
        Some(&[
            r#"Deleted: secretsets/temp-secrets"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_absent("ss", temp_secrets).await;

    // -- 5. Kind-scoped --all dry-run previews only VariableSets --------------
    ctx.apply_variable_set(app_vars, "app-value").await;
    ctx.apply_variable_set(temp_vars, "temp-value").await;

    let variableset_dry_run = ctx
        .stderr(["delete", "variablesets", "--all", "--dry-run"])
        .await;
    assert_output_contains_all(
        &variableset_dry_run,
        &[
            "Would delete: variablesets/app-vars",
            "Would delete: variablesets/temp-vars",
            "Summary 2 item(s): 2 would delete, 0 ignored, 0 failed",
        ],
        "delete variablesets --all --dry-run",
    );

    ctx.assert_resource_present("vs", app_vars).await;
    ctx.assert_resource_present("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_absent("ss", temp_secrets).await;

    // -- 6. Kind-scoped --all deletes only VariableSets -----------------------
    let variableset_delete = ctx
        .stderr(["delete", "variablesets", "--all", "--force"])
        .await;
    assert_output_contains_all(
        &variableset_delete,
        &[
            "Deleted: variablesets/app-vars",
            "Deleted: variablesets/temp-vars",
            "Summary 2 item(s): 2 deleted, 0 ignored, 0 failed",
        ],
        "delete variablesets --all --force",
    );

    ctx.assert_resource_absent("vs", app_vars).await;
    ctx.assert_resource_absent("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;

    // -- 7. Cross-kind all dry-run previews every remaining resource ----------
    ctx.apply_variable_set(app_vars, "app-value").await;
    ctx.apply_variable_set(temp_vars, "temp-value").await;
    ctx.apply_secret_set(temp_secrets, "temp-token", "temp-password")
        .await;

    let all_dry_run = ctx.stderr(["delete", "all", "--dry-run"]).await;
    assert_output_contains_all(
        &all_dry_run,
        &[
            "Would delete: variablesets/app-vars",
            "Would delete: variablesets/temp-vars",
            "Would delete: secretsets/app-secrets",
            "Would delete: secretsets/temp-secrets",
            "Summary 4 item(s): 4 would delete, 0 ignored, 0 failed",
        ],
        "delete all --dry-run",
    );

    ctx.assert_resource_present("vs", app_vars).await;
    ctx.assert_resource_present("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_present("ss", temp_secrets).await;

    // -- 8. Cross-kind all deletes everything ---------------------------------
    let all_delete = ctx.stderr(["delete", "all", "--force"]).await;
    assert_output_contains_all(
        &all_delete,
        &[
            "Deleted: variablesets/app-vars",
            "Deleted: variablesets/temp-vars",
            "Deleted: secretsets/app-secrets",
            "Deleted: secretsets/temp-secrets",
            "Summary 4 item(s): 4 deleted, 0 ignored, 0 failed",
        ],
        "delete all --force",
    );

    ctx.assert_resource_absent("vs", app_vars).await;
    ctx.assert_resource_absent("vs", temp_vars).await;
    ctx.assert_resource_absent("ss", app_secrets).await;
    ctx.assert_resource_absent("ss", temp_secrets).await;

    // Summary succeeds and `list all` is back to the empty JSON array.
    ctx.assert_success(["summary"], None).await;
    let list_all = ctx.stdout(["list", "all"]).await;
    assert_eq!(
        list_all.trim(),
        "[]",
        "`list all` after deleting every resource should be empty, got:\n{list_all}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn seed_delete_fixtures(
    ctx: &ResourceCtx,
    app_vars: &str,
    temp_vars: &str,
    app_secrets: &str,
    temp_secrets: &str,
) {
    ctx.apply_variable_set(app_vars, "app-value").await;
    ctx.apply_variable_set(temp_vars, "temp-value").await;
    ctx.apply_secret_set(app_secrets, "app-token", "app-password")
        .await;
    ctx.apply_secret_set(temp_secrets, "temp-token", "temp-password")
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
