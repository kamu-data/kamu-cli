// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{ResourceCtx, assert_output_contains_all, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: resource delete semantics (QA scenario 10)
//
// Covers explicit selectors, slash selectors, name patterns, type-scoped
// `--all`, and cross-type `all` deletion. Wired (via
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

    // -- 2. Explicit type/name selector deletes only the selected VariableSet -
    ctx.assert_success(
        ["delete", "vs", app_vars, "--force"],
        Some(&[
            r#"Deleting: VariableSet/app-vars"#,
            r#"Summary 1 item\(s\): 0 deleted, 1 deleting, 0 ignored, 0 failed"#,
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
            r#"Deleting: VariableSet/temp-vars"#,
            r#"Summary 1 item\(s\): 0 deleted, 1 deleting, 0 ignored, 0 failed"#,
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
            r#"Deleting: SecretSet/temp-secrets"#,
            r#"Summary 1 item\(s\): 0 deleted, 1 deleting, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_absent("ss", temp_secrets).await;

    // -- 5. Type-scoped --all dry-run previews only VariableSets --------------
    ctx.apply_variable_set(app_vars, "app-value").await;
    ctx.apply_variable_set(temp_vars, "temp-value").await;

    let variableset_dry_run = ctx
        .stderr(["delete", "variablesets", "--all", "--dry-run"])
        .await;
    assert_output_contains_all(
        &variableset_dry_run,
        &[
            "Deleted (dry-run): VariableSet/app-vars",
            "Deleted (dry-run): VariableSet/temp-vars",
            "Summary 2 item(s): 2 deleted (dry-run), 0 ignored, 0 failed",
        ],
        "delete variablesets --all --dry-run",
    );

    ctx.assert_resource_present("vs", app_vars).await;
    ctx.assert_resource_present("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_absent("ss", temp_secrets).await;

    // -- 6. Type-scoped --all deletes only VariableSets -----------------------
    let variableset_delete = ctx
        .stderr(["delete", "variablesets", "--all", "--force"])
        .await;
    assert_output_contains_all(
        &variableset_delete,
        &[
            "Deleting: VariableSet/app-vars",
            "Deleting: VariableSet/temp-vars",
            "Summary 2 item(s): 0 deleted, 2 deleting, 0 ignored, 0 failed",
        ],
        "delete variablesets --all --force",
    );

    ctx.assert_resource_absent("vs", app_vars).await;
    ctx.assert_resource_absent("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;

    // -- 7. Cross-type all dry-run previews every remaining resource ----------
    ctx.apply_variable_set(app_vars, "app-value").await;
    ctx.apply_variable_set(temp_vars, "temp-value").await;
    ctx.apply_secret_set(temp_secrets, "temp-token", "temp-password")
        .await;

    let all_dry_run = ctx.stderr(["delete", "%/%", "--dry-run"]).await;
    assert_output_contains_all(
        &all_dry_run,
        &[
            "Deleted (dry-run): VariableSet/app-vars",
            "Deleted (dry-run): VariableSet/temp-vars",
            "Deleted (dry-run): SecretSet/app-secrets",
            "Deleted (dry-run): SecretSet/temp-secrets",
            "Summary 4 item(s): 4 deleted (dry-run), 0 ignored, 0 failed",
        ],
        "delete %/% --dry-run",
    );

    ctx.assert_resource_present("vs", app_vars).await;
    ctx.assert_resource_present("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_present("ss", temp_secrets).await;

    // `%` is advertised as the all-types target, so `--all` must apply to it the
    // same way it does to a concrete type.
    let any_type_all_flag = ctx.stderr(["delete", "%", "--all", "--dry-run"]).await;
    assert_output_contains_all(
        &any_type_all_flag,
        &[
            "Deleted (dry-run): VariableSet/app-vars",
            "Deleted (dry-run): VariableSet/temp-vars",
            "Deleted (dry-run): SecretSet/app-secrets",
            "Deleted (dry-run): SecretSet/temp-secrets",
            "Summary 4 item(s): 4 deleted (dry-run), 0 ignored, 0 failed",
        ],
        "delete % --all --dry-run",
    );

    // A bare type carries the same meaning without the flag: `delete %` is
    // `delete %/%`. Previewed only — section 8 reuses this fixture state.
    let bare_any_type = ctx.stderr(["delete", "%", "--dry-run"]).await;
    assert_output_contains_all(
        &bare_any_type,
        &[
            "Deleted (dry-run): VariableSet/app-vars",
            "Deleted (dry-run): VariableSet/temp-vars",
            "Deleted (dry-run): SecretSet/app-secrets",
            "Deleted (dry-run): SecretSet/temp-secrets",
            "Summary 4 item(s): 4 deleted (dry-run), 0 ignored, 0 failed",
        ],
        "delete % --dry-run",
    );

    // The alias widens the name half to `%`, never the type half.
    let bare_type = ctx.stderr(["delete", "vs", "--dry-run"]).await;
    assert_output_contains_all(
        &bare_type,
        &[
            "Deleted (dry-run): VariableSet/app-vars",
            "Deleted (dry-run): VariableSet/temp-vars",
            "Summary 2 item(s): 2 deleted (dry-run), 0 ignored, 0 failed",
        ],
        "delete vs --dry-run",
    );

    ctx.assert_resource_present("vs", app_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;

    // -- 8. Cross-type all deletes everything ---------------------------------
    let all_delete = ctx.stderr(["delete", "%/%", "--force"]).await;
    assert_output_contains_all(
        &all_delete,
        &[
            "Deleting: VariableSet/app-vars",
            "Deleting: VariableSet/temp-vars",
            "Deleting: SecretSet/app-secrets",
            "Deleting: SecretSet/temp-secrets",
            "Summary 4 item(s): 0 deleted, 4 deleting, 0 ignored, 0 failed",
        ],
        "delete %/% --force",
    );

    ctx.assert_resource_absent("vs", app_vars).await;
    ctx.assert_resource_absent("vs", temp_vars).await;
    ctx.assert_resource_absent("ss", app_secrets).await;
    ctx.assert_resource_absent("ss", temp_secrets).await;

    // Summary succeeds and `list all` is back to the empty JSON array.
    ctx.assert_success(["summary"], None).await;
    let list_all = ctx.stdout(["list", "%"]).await;
    assert_eq!(
        list_all.trim(),
        "[]",
        "`list all` after deleting every resource should be empty, got:\n{list_all}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: bare resource ID selector (no type token)
//
// IMPORTANT: this scenario applies a `SecretSet` resource (to prove a bare ID
// resolves across differing resource types, not just the first one tried), so
// it must be wired with
// `Options::with_kamu_config(fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG)`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_bare_id_selector(ctx: ResourceCtx) {
    let vars_name = "bare-id-vars";
    let secrets_name = "bare-id-secrets";
    ctx.apply_variable_set(vars_name, "some-value").await;
    ctx.apply_secret_set(secrets_name, "some-token", "some-password")
        .await;

    let vars_id = ctx.resource_id("vs", vars_name).await;
    let secrets_id = ctx.resource_id("ss", secrets_name).await;

    // A bare ID resolves regardless of which resource type it belongs to —
    // this only holds if the search genuinely spans every type rather than
    // stopping at the first one tried.
    let view = ctx.get_one(["get", &vars_id]).await;
    assert_eq!(view.name(), vars_name);

    let secrets_view = ctx.get_one(["get", &secrets_id]).await;
    assert_eq!(secrets_view.name(), secrets_name);

    // Type given explicitly still works.
    let view_with_type = ctx.get_one(["get", "vs", &vars_id]).await;
    assert_eq!(view_with_type.name(), vars_name);

    let missing_id = "51a420dc-f0a0-4842-a148-4481195a4b34";
    ctx.assert_failure(["get", missing_id], None).await;

    ctx.assert_success(
        ["delete", &vars_id, "--force"],
        Some(&[format!(r#"Deleting: VariableSet/{vars_name}"#).as_str()]),
    )
    .await;
    ctx.assert_success(
        ["delete", &secrets_id, "--force"],
        Some(&[format!(r#"Deleting: SecretSet/{secrets_name}"#).as_str()]),
    )
    .await;

    ctx.assert_resource_absent("vs", vars_name).await;
    ctx.assert_resource_absent("ss", secrets_name).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: `--include-deleting` on `get` and `list`
//
// The flag also shows resources whose deletion waits for their controllers.
// The controllers clean up once the delete command has finished, so a deleted
// resource is gone with or without the flag.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_include_deleting(ctx: ResourceCtx) {
    let kept = "kept-vars";
    let gone = "gone-vars";
    let vs_schema = fixtures::VARIABLE_SET_SCHEMA.to_string();

    ctx.apply_variable_set(kept, "one").await;
    ctx.apply_variable_set(gone, "two").await;

    assert_eq!(
        ctx.get_idents(["get", "vs/%", "--include-deleting"]).await,
        vec![
            (vs_schema.clone(), gone.to_string()),
            (vs_schema.clone(), kept.to_string()),
        ]
    );

    ctx.assert_success(
        ["delete", "vs", gone, "--force"],
        Some(&[r#"Deleting: VariableSet/gone-vars"#]),
    )
    .await;

    ctx.assert_failure(["get", "vs", gone, "--include-deleting"], None)
        .await;
    assert_eq!(
        ctx.get_idents(["get", "vs/%", "--include-deleting"]).await,
        vec![(vs_schema, kept.to_string())]
    );

    let listed = ctx.stdout(["list", "vs", "--include-deleting"]).await;
    assert!(
        listed.contains(kept) && !listed.contains(gone),
        "`list vs --include-deleting` should show only {kept}, got:\n{listed}"
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
