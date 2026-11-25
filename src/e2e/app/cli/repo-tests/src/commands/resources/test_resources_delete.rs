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
    ctx.assert_deleted(
        ["delete", "vs", app_vars, "--force"],
        &["VariableSet/app-vars"],
    )
    .await;

    ctx.assert_resource_absent("vs", app_vars).await;
    ctx.assert_resource_present("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_present("ss", temp_secrets).await;

    // -- 3. Slash selector deletes the other VariableSet ----------------------
    ctx.assert_deleted(
        ["delete", "vs/temp-vars", "--force"],
        &["VariableSet/temp-vars"],
    )
    .await;

    ctx.assert_resource_absent("vs", temp_vars).await;
    ctx.assert_resource_present("ss", app_secrets).await;
    ctx.assert_resource_present("ss", temp_secrets).await;

    // -- 4. Pattern selector deletes matching SecretSets only -----------------
    ctx.assert_deleted(
        ["delete", "ss", "temp-%", "--force"],
        &["SecretSet/temp-secrets"],
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
    ctx.assert_deleted_in_any_order(
        ["delete", "variablesets", "--all", "--force"],
        &["VariableSet/app-vars", "VariableSet/temp-vars"],
    )
    .await;

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
    ctx.assert_deleted_in_any_order(
        ["delete", "%/%", "--force"],
        &[
            "VariableSet/app-vars",
            "VariableSet/temp-vars",
            "SecretSet/app-secrets",
            "SecretSet/temp-secrets",
        ],
    )
    .await;

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

    ctx.assert_deleted(
        ["delete", &vars_id, "--force"],
        &[&format!("VariableSet/{vars_name}")],
    )
    .await;
    ctx.assert_deleted(
        ["delete", &secrets_id, "--force"],
        &[&format!("SecretSet/{secrets_name}")],
    )
    .await;

    ctx.assert_resource_absent("vs", vars_name).await;
    ctx.assert_resource_absent("ss", secrets_name).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: `--include-deleting`, `--include-deleted` and `--deleted`
//
// Deletions are completed before the checks, so `--include-deleting` alone shows
// no deleted resource.
//
// IMPORTANT: this scenario applies a `SecretSet` resource, so it must be wired
// with `Options::with_kamu_config(fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG)`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_deletion_visibility(ctx: ResourceCtx) {
    let live = "live-vars";
    let reused = "reused-vars";
    let gone = "gone-vars";

    ctx.apply_variable_set(live, "one").await;
    ctx.apply_variable_set(reused, "two").await;
    ctx.apply_variable_set(gone, "three").await;
    let live_id = ctx.resource_id("vs", live).await;
    let reused_id = ctx.resource_id("vs", reused).await;
    let gone_id = ctx.resource_id("vs", gone).await;

    ctx.assert_deleted(
        ["delete", "vs", gone, "--force"],
        &["VariableSet/gone-vars"],
    )
    .await;
    ctx.wait_until_deleted(&gone_id).await;
    ctx.assert_deleted(
        ["delete", "vs", reused, "--force"],
        &["VariableSet/reused-vars"],
    )
    .await;
    ctx.wait_until_deleted(&reused_id).await;
    ctx.apply_variable_set(reused, "four").await;
    let new_id = ctx.resource_id("vs", reused).await;

    // -- 1. A name finds deleted resources only when asked for ----------------
    for args in [
        ["get", "vs", gone].as_slice(),
        &["get", "vs", gone, "--include-deleting"],
    ] {
        ctx.assert_failure(
            args.iter().copied(),
            Some(&[&format!(
                "Resource '{gone}' of type 'VariableSet' was not found"
            )]),
        )
        .await;
    }
    assert_eq!(
        got_ids_and_phases(&ctx, &["vs", gone, "--include-deleted"]).await,
        [(gone_id.clone(), "Deleted".to_string())]
    );
    let reused_phases = got_ids_and_phases(&ctx, &["vs", reused, "--include-deleted"]).await;
    assert_eq!(
        reused_phases.iter().map(|(id, _)| id).collect::<Vec<_>>(),
        [&new_id, &reused_id],
        "the live resource comes first"
    );
    assert_eq!(
        got_ids_and_phases(&ctx, &["vs", reused, "--deleted"]).await,
        [(reused_id.clone(), "Deleted".to_string())]
    );

    // -- 2. An id finds a resource in any state -------------------------------
    for args in [vec![gone_id.as_str()], vec!["vs", gone_id.as_str()]] {
        assert_eq!(
            got_ids_and_phases(&ctx, &args).await,
            [(gone_id.clone(), "Deleted".to_string())]
        );
    }
    ctx.assert_failure(
        ["get", &live_id, "--deleted"],
        Some(&[r"Resource with id .+ was not found"]),
    )
    .await;
    ctx.assert_failure(
        ["delete", &gone_id, "--force"],
        Some(&[r"Resource with id .+ was not found"]),
    )
    .await;

    // -- 3. Listings of deleted resources run newest deletion first -----------
    assert_eq!(
        listed_names_and_phases(&ctx, &["vs", "--deleted"]).await,
        [(reused, "Deleted"), (gone, "Deleted")].map(to_owned_pair)
    );
    assert_eq!(
        listed_names_and_phases(&ctx, &["vs", "--deleted", "--max-results", "1"]).await,
        [(reused, "Deleted")].map(to_owned_pair)
    );
    assert_eq!(
        listed_names_and_phases(&ctx, &["%", "--include-deleted"])
            .await
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        [live, reused, reused, gone]
    );
    assert_eq!(
        listed_names_and_phases(&ctx, &["vs", "--include-deleting"])
            .await
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        [live, reused]
    );
    assert_eq!(
        listed_names_and_phases(&ctx, &[&gone_id]).await,
        [(gone, "Deleted")].map(to_owned_pair)
    );

    // -- 4. The summary counts deleted resources ------------------------------
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_SCHEMA).await, 4);
    let summary = ctx.stdout(["summary", "-o", "table"]).await;
    assert_output_contains_all(&summary, &["Deleting", "Deleted"], "summary -o table");

    // -- 5. The options exclude each other ------------------------------------
    for command in ["get", "list"] {
        ctx.assert_failure(
            [command, "vs", "--include-deleted", "--deleted"],
            Some(&["cannot be used with"]),
        )
        .await;
    }

    // -- 6. Datasets do not take them -----------------------------------------
    for option in ["--include-deleting", "--include-deleted", "--deleted"] {
        ctx.assert_failure(
            ["list", "datasets", option],
            Some(&["supported only when listing resources"]),
        )
        .await;
    }

    // -- 7. Deleted secrets are not revealed ----------------------------------
    let secrets = "gone-secrets";
    ctx.apply_secret_set(secrets, "some-token", "some-password")
        .await;
    let secrets_id = ctx.resource_id("ss", secrets).await;
    ctx.assert_deleted(
        ["delete", "ss", secrets, "--force"],
        &["SecretSet/gone-secrets"],
    )
    .await;
    ctx.wait_until_deleted(&secrets_id).await;

    for format in [["-o", "yaml"].as_slice(), &["-o", "yaml", "--spec"]] {
        let encrypted = ctx
            .stdout(["get", &secrets_id].iter().chain(format).copied())
            .await;
        assert!(
            encrypted.contains("jwe") && !encrypted.contains("some-password"),
            "the spec of a deleted secret set stays readable encrypted, got:\n{encrypted}"
        );
        ctx.assert_failure(
            ["get", &secrets_id, "--revealed"]
                .iter()
                .chain(format)
                .copied(),
            Some(&[r"Resource .+ is deleted, its spec cannot be revealed"]),
        )
        .await;
    }
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

/// Runs `get <args>` and returns the id and phase of each resource, in order.
async fn got_ids_and_phases(ctx: &ResourceCtx, args: &[&str]) -> Vec<(String, String)> {
    ctx.get_views(["get"].iter().chain(args).copied())
        .await
        .iter()
        .map(|view| (view.id(), view.phase().to_string()))
        .collect()
}

/// Runs `list <args>` and returns the name and phase of each resource, in
/// order.
async fn listed_names_and_phases(ctx: &ResourceCtx, args: &[&str]) -> Vec<(String, String)> {
    let doc = ctx
        .stdout_json(["list"].iter().chain(args).chain(&["-o", "json"]).copied())
        .await;
    doc.as_array()
        .unwrap()
        .iter()
        .map(|row| {
            to_owned_pair((
                row["Name"].as_str().unwrap(),
                row["Phase"].as_str().unwrap(),
            ))
        })
        .collect()
}

fn to_owned_pair((name, phase): (&str, &str)) -> (String, String) {
    (name.to_string(), phase.to_string())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
