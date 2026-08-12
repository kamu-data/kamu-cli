// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{ResourceCtx, assert_output_contains_all, fixtures};

/// Seeds `VariableSet`s covering single-label and AND-ed label filters.
async fn seed_labeled_variable_sets(ctx: &ResourceCtx) {
    let prod = fixtures::variable_set_manifest_with_environment_label(
        "prod-vars",
        "production",
        "environment",
    );
    ctx.assert_success_with_stdin(["apply", "--stdin"], &prod, None)
        .await;

    let prod_team = fixtures::variable_set_manifest_with_environment_and_team_labels(
        "prod-team",
        "production",
        "core",
    );
    ctx.assert_success_with_stdin(["apply", "--stdin"], &prod_team, None)
        .await;

    let stage = fixtures::variable_set_manifest_with_environment_label(
        "stage-vars",
        "staging",
        "environment",
    );
    ctx.assert_success_with_stdin(["apply", "--stdin"], &stage, None)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_label_filter_read_paths(ctx: ResourceCtx) {
    seed_labeled_variable_sets(&ctx).await;

    assert_eq!(
        ctx.list_names("vs").await,
        ["prod-team", "prod-vars", "stage-vars"],
        "unfiltered list should return every seeded VariableSet"
    );

    assert_eq!(
        ctx.list_names_with_labels("vs", &["environment=production"])
            .await,
        ["prod-team", "prod-vars"],
        "`-l environment=production` should exclude the staging resource"
    );

    // The label is stored under its canonical URI, so filtering by the short
    // name only works if the filter is canonicalized too.
    let by_uri = format!("{}=production", fixtures::ENVIRONMENT_LABEL_SCHEMA);
    assert_eq!(
        ctx.list_names_with_labels("vs", &[by_uri.as_str()]).await,
        ["prod-team", "prod-vars"],
        "canonical URI key must select identically to the short name"
    );

    assert_eq!(
        ctx.list_names_with_labels("vs", &["environment=production", "team=core"])
            .await,
        ["prod-team"],
        "two `-l` flags should intersect, not union"
    );

    // The comma-separated form of one flag must mean the same thing.
    assert_eq!(
        ctx.list_names_with_labels("vs", &["environment=production,team=core"])
            .await,
        ["prod-team"],
        "comma-separated selectors in one flag should AND identically"
    );

    assert_eq!(
        ctx.list_names_with_labels("vs", &["environment=nonexistent"])
            .await,
        Vec::<String>::new(),
        "a value no resource carries should return an empty list, not an error"
    );

    assert_eq!(
        ctx.list_names_with_labels("vs", &["nosuchkey=value"]).await,
        Vec::<String>::new(),
        "an unregistered free-form key should match nothing without erroring"
    );

    assert_eq!(
        ctx.list_names_with_labels("all", &["environment=production"])
            .await,
        ["prod-team", "prod-vars"],
        "`list all -l` should narrow across the multi-type path"
    );

    ctx.assert_failure(
        ["list", "datasets", "-l", "environment=production"],
        Some(&[r#"Label selectors are not supported when listing datasets"#]),
    )
    .await;

    let idents = ctx
        .get_idents(["get", "vs/%", "-l", "environment=production"])
        .await;
    assert_eq!(
        idents,
        [
            (
                fixtures::VARIABLE_SET_SCHEMA.to_string(),
                "prod-team".to_string()
            ),
            (
                fixtures::VARIABLE_SET_SCHEMA.to_string(),
                "prod-vars".to_string()
            ),
        ],
        "`get vs/% -l …` should resolve to only the matching resources"
    );

    // Narrowing to a single match still yields a full resource document, so
    // the filter affects selection only — not the projection.
    let view = ctx
        .get_one([
            "get",
            "vs/%",
            "-l",
            "environment=production",
            "-l",
            "team=core",
        ])
        .await;
    assert_eq!(view.name(), "prod-team");
    assert_eq!(
        view.label_str(fixtures::ENVIRONMENT_LABEL_SCHEMA),
        Some("production"),
        "the filtered `get` must still return the resource's stored labels"
    );

    // An exact (non-pattern) selector combined with a filter that the named
    // resource does not satisfy must yield nothing — the exact path is
    // filtered too, not short-circuited into an unfiltered lookup.
    //
    // A fully-excluded `get` emits empty stdout rather than an empty JSON array.
    let excluded = ctx
        .stdout([
            "get",
            "vs",
            "stage-vars",
            "-l",
            "environment=production",
            "-o",
            "json",
            "--ignore-not-found",
        ])
        .await;
    assert!(
        excluded.trim().is_empty(),
        "an exact selector must still be subject to the label filter, got:\n{excluded}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_label_filter_delete(ctx: ResourceCtx) {
    seed_labeled_variable_sets(&ctx).await;

    let dry_run = ctx
        .stderr([
            "delete",
            "variablesets",
            "--all",
            "-l",
            "environment=staging",
            "--dry-run",
        ])
        .await;
    assert_output_contains_all(
        &dry_run,
        &[
            "Deleted (dry-run): VariableSet/stage-vars",
            "Summary 1 item(s): 1 deleted (dry-run), 0 ignored, 0 failed",
        ],
        "delete variablesets --all -l environment=staging --dry-run",
    );

    ctx.assert_success(
        [
            "delete",
            "variablesets",
            "--all",
            "-l",
            "environment=staging",
            "--force",
        ],
        Some(&[
            r#"Deleted: VariableSet/stage-vars"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    assert_eq!(
        ctx.list_names("vs").await,
        ["prod-team", "prod-vars"],
        "only the staging resource should have been deleted"
    );

    ctx.assert_failure(
        ["delete", "datasets", "--all", "-l", "environment=staging"],
        Some(&[r#"Label selectors are not supported when deleting datasets"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_label_filter_any_type_exact_ref(ctx: ResourceCtx) {
    seed_labeled_variable_sets(&ctx).await;

    let excluded = ctx
        .stdout([
            "get",
            "%/prod-vars",
            "-l",
            "environment=staging",
            "-o",
            "json",
            "--ignore-not-found",
        ])
        .await;
    assert!(
        excluded.trim().is_empty(),
        "an any-type + exact-name selector must still be subject to the label filter, \
         got:\n{excluded}"
    );

    let view = ctx
        .get_one(["get", "%/prod-vars", "-l", "environment=production"])
        .await;
    assert_eq!(view.name(), "prod-vars");

    let id = ctx.resource_id("vs", "prod-vars").await;

    let excluded_by_id = ctx
        .stdout([
            "get",
            &format!("%/{id}"),
            "-l",
            "environment=staging",
            "-o",
            "json",
            "--ignore-not-found",
        ])
        .await;
    assert!(
        excluded_by_id.trim().is_empty(),
        "an any-type + exact-id selector must still be subject to the label filter, \
         got:\n{excluded_by_id}"
    );

    let view_by_id = ctx
        .get_one(["get", &format!("%/{id}"), "-l", "environment=production"])
        .await;
    assert_eq!(view_by_id.name(), "prod-vars");

    // A structured value is deliberately not indexed, so it can never be
    // matched by an equality filter.
    let structured_name = "structured-label-vars";
    let structured_manifest =
        fixtures::variable_set_manifest_with_structured_label(structured_name);
    ctx.assert_success_with_stdin(["apply", "--stdin"], &structured_manifest, None)
        .await;

    assert!(
        ctx.list_names("vs")
            .await
            .contains(&structured_name.to_string()),
        "the structured-label resource must be listed when no filter is applied"
    );

    assert_eq!(
        ctx.list_names_with_labels("vs", &["coordinates=1"]).await,
        Vec::<String>::new(),
        "a structured label value must not be selectable by equality"
    );

    let structured_view = ctx.get_one(["get", "vs", structured_name]).await;
    assert_eq!(
        structured_view.label("coordinates").cloned(),
        Some(serde_json::json!({ "lat": 1, "lon": 2 })),
        "the structured value must still be readable via `get`"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Seeds `VariableSet` and `SecretSet` resources for multi-type filters.
async fn seed_labeled_cross_type_resources(ctx: &ResourceCtx) {
    let prod_vars = fixtures::variable_set_manifest_with_environment_label(
        "prod-vars",
        "production",
        "environment",
    );
    ctx.assert_success_with_stdin(["apply", "--stdin"], &prod_vars, None)
        .await;

    let prod_creds = fixtures::secret_set_manifest_with_environment_label(
        "prod-creds",
        "production",
        "environment",
    );
    ctx.assert_success_with_stdin(["apply", "--stdin"], &prod_creds, None)
        .await;

    let stage_creds = fixtures::secret_set_manifest_with_environment_label(
        "stage-creds",
        "staging",
        "environment",
    );
    ctx.assert_success_with_stdin(["apply", "--stdin"], &stage_creds, None)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_label_filter_any_type_multitype(ctx: ResourceCtx) {
    seed_labeled_cross_type_resources(&ctx).await;

    let idents = ctx
        .get_idents(["get", "%/%-creds", "-l", "environment=production"])
        .await;
    assert_eq!(
        idents,
        [(
            fixtures::SECRET_SET_SCHEMA.to_string(),
            "prod-creds".to_string()
        )],
        "`%/%-creds -l environment=production` should exclude the staging SecretSet"
    );

    let idents = ctx
        .get_idents(["get", "%", "all", "-l", "environment=production"])
        .await;
    assert_eq!(
        idents,
        [
            (
                fixtures::SECRET_SET_SCHEMA.to_string(),
                "prod-creds".to_string()
            ),
            (
                fixtures::VARIABLE_SET_SCHEMA.to_string(),
                "prod-vars".to_string()
            ),
        ],
        "`% all -l environment=production` should exclude the staging SecretSet"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
