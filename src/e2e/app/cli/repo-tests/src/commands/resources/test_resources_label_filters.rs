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
// Scenario: `-l key=value` label filtering across `list`, `get` and `delete`
//
// Exercises the full stack through the real CLI: selector parsing, filter
// resolution (short name -> canonical schema URI), the repository predicate,
// and the GraphQL transport when running against a remote context.
//
// The filter is a *narrowing of the candidate set*, so the assertions are
// always "these and only these" — comparing whole sorted name lists rather
// than probing for presence, which would pass even if the filter were ignored
// entirely.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Seeds three `VariableSet`s spanning the cases the filters must separate:
///
/// | name        | environment | team    |
/// |-------------|-------------|---------|
/// | `prod-vars` | production  | —       |
/// | `prod-team` | production  | core    |
/// | `stage-vars`| staging     | —       |
///
/// `prod-team` sharing `environment=production` with `prod-vars` is what makes
/// the two-predicate AND meaningful: the second predicate has to do real work
/// to isolate it.
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

pub async fn test_resources_label_filter_list(ctx: ResourceCtx) {
    seed_labeled_variable_sets(&ctx).await;

    // ── Unfiltered baseline ──────────────────────────────────────────────────
    // Pins the full candidate set, so every narrowing below is a real
    // subtraction rather than a query that happened to return little.
    assert_eq!(
        ctx.list_names("vs").await,
        ["prod-team", "prod-vars", "stage-vars"],
        "unfiltered list should return every seeded VariableSet"
    );

    // ── Short-name key narrows ───────────────────────────────────────────────
    assert_eq!(
        ctx.list_names_with_labels("vs", &["environment=production"])
            .await,
        ["prod-team", "prod-vars"],
        "`-l environment=production` should exclude the staging resource"
    );

    // ── Canonical URI key is equivalent ──────────────────────────────────────
    // The label is stored under its canonical URI, so filtering by the short
    // name only works if the filter is canonicalized too. Asserting the two
    // forms agree is what proves resolution happens on the filter path.
    let by_uri = format!("{}=production", fixtures::ENVIRONMENT_LABEL_SCHEMA);
    assert_eq!(
        ctx.list_names_with_labels("vs", &[by_uri.as_str()]).await,
        ["prod-team", "prod-vars"],
        "canonical URI key must select identically to the short name"
    );

    // ── Two predicates are ANDed ─────────────────────────────────────────────
    // `team` is free-form and `environment` is schema-registered, so this also
    // covers AND-ing across the canonicalized/opaque boundary.
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

    // ── Non-matching value excludes everything ───────────────────────────────
    assert_eq!(
        ctx.list_names_with_labels("vs", &["environment=nonexistent"])
            .await,
        Vec::<String>::new(),
        "a value no resource carries should return an empty list, not an error"
    );

    // ── An unknown key excludes everything ───────────────────────────────────
    // Free-form keys are unconstrained, so an unregistered key is a valid
    // filter that simply matches nothing — not a rejection.
    assert_eq!(
        ctx.list_names_with_labels("vs", &["nosuchkey=value"]).await,
        Vec::<String>::new(),
        "an unregistered free-form key should match nothing without erroring"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_label_filter_list_all(ctx: ResourceCtx) {
    seed_labeled_variable_sets(&ctx).await;

    // `list all` spans every resource type, so the filter must be applied to
    // the multi-type expansion path (where the filter is resolved once per
    // schema and collapsed), not just the single-type one.
    assert_eq!(
        ctx.list_names_with_labels("all", &["environment=production"])
            .await,
        ["prod-team", "prod-vars"],
        "`list all -l` should narrow across the multi-type path"
    );

    // Datasets are not resources and carry no labels, so filtering them is
    // rejected rather than silently ignored.
    ctx.assert_failure(
        ["list", "datasets", "-l", "environment=production"],
        Some(&[r#"Label selectors are not supported when listing datasets"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_label_filter_get(ctx: ResourceCtx) {
    seed_labeled_variable_sets(&ctx).await;

    // `get` filters during its phase-1 expansion of the name pattern into
    // identifiers, so a pattern plus a filter must intersect the two.
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
    // A fully-excluded `get` emits *empty* stdout rather than an empty JSON
    // array, so this asserts on the raw output (the same convention the
    // deleted-resource lookups use) instead of parsing it into views.
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

    // Dry-run first: the filter must narrow the *plan*, so a mistake here is
    // visible before anything is destroyed.
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
            "Would delete: variablesets/stage-vars",
            "Summary 1 item(s): 1 would delete, 0 ignored, 0 failed",
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
            r#"Deleted: variablesets/stage-vars"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    // The production resources must survive: `--all` was scoped by the filter,
    // which is the whole point of the flag combination.
    assert_eq!(
        ctx.list_names("vs").await,
        ["prod-team", "prod-vars"],
        "only the staging resource should have been deleted"
    );

    // Datasets carry no labels, so a filtered dataset delete is rejected.
    ctx.assert_failure(
        ["delete", "datasets", "--all", "-l", "environment=staging"],
        Some(&[r#"Label selectors are not supported when deleting datasets"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A wildcard *type* pattern combined with an *exact* name/id (`%/prod-vars`)
// resolves via `TypePatternExactName`, which now routes through
// `search_handles` (like `TypePatternAll`/`TypePatternNamePattern`) instead of
// the unfiltered scalar `get_handle` path, so `-l` is respected here too.
pub async fn test_resources_label_filter_type_pattern_exact_name(ctx: ResourceCtx) {
    seed_labeled_variable_sets(&ctx).await;

    // ── Non-matching filter excludes a `ByName` ref ──────────────────────────
    // `prod-vars` carries `environment=production`, not `staging`, so a
    // correctly filtered lookup must exclude it.
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
        "a type-pattern + exact-name selector must still be subject to the label filter, \
         got:\n{excluded}"
    );

    // ── Matching filter still returns the resource ───────────────────────────
    let view = ctx
        .get_one(["get", "%/prod-vars", "-l", "environment=production"])
        .await;
    assert_eq!(view.name(), "prod-vars");

    // ── Same coverage for a `ById` ref ────────────────────────────────────────
    // The type-pattern segment resolves any UUIDv4-shaped selector to `ById`
    // before the exact match, so that ref kind needs its own assertion.
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
        "a type-pattern + exact-id selector must still be subject to the label filter, \
         got:\n{excluded_by_id}"
    );

    let view_by_id = ctx
        .get_one(["get", &format!("%/{id}"), "-l", "environment=production"])
        .await;
    assert_eq!(view_by_id.name(), "prod-vars");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Seeds a `VariableSet` and a `SecretSet` sharing the `environment` label, so
/// a type-pattern selector spanning both schemas (`%sets`) has a genuine
/// multi-type candidate set to narrow. Requires
/// [`fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG`].
///
/// | name         | type        | environment |
/// |--------------|-------------|-------------|
/// | `prod-vars`  | VariableSet | production  |
/// | `prod-creds` | SecretSet   | production  |
/// | `stage-creds`| SecretSet   | staging     |
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

// Coverage gap closed: `TypePatternNamePattern`
// (`<type-pattern>/<name-pattern>`, both segments wildcarded) is resolved via
// `search_handles` as well — same backend call as `TypePatternAll`, different
// selector shape. Must run with
// `Options::with_kamu_config(fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG)`.
pub async fn test_resources_label_filter_type_pattern_name_pattern(ctx: ResourceCtx) {
    seed_labeled_cross_type_resources(&ctx).await;

    // `%sets/%-creds` matches only the two SecretSets by name pattern; the
    // filter must still narrow that set to the production one.
    let idents = ctx
        .get_idents(["get", "%sets/%-creds", "-l", "environment=production"])
        .await;
    assert_eq!(
        idents,
        [(
            fixtures::SECRET_SET_SCHEMA.to_string(),
            "prod-creds".to_string()
        )],
        "`%sets/%-creds -l environment=production` should exclude the staging SecretSet"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Coverage gap closed: `TypePatternAll` (`<type-pattern> all`, e.g. `%sets
// all`) is resolved via `search_handles` with `name_pattern: Some("%")` — not
// testable with a label filter until that selector shape worked at all.
pub async fn test_resources_label_filter_type_pattern_all(ctx: ResourceCtx) {
    seed_labeled_cross_type_resources(&ctx).await;

    // `%sets all` spans every VariableSet and SecretSet; the filter must still
    // narrow that set to the production ones.
    let idents = ctx
        .get_idents(["get", "%sets", "all", "-l", "environment=production"])
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
        "`%sets all -l environment=production` should exclude the staging SecretSet"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_label_filter_structured_not_selectable(ctx: ResourceCtx) {
    // A structured (non-string) label value is stored but deliberately not
    // indexed, so it can never be selected by an equality filter. The resource
    // must remain fully reachable *without* the filter — proving this is an
    // indexing boundary, not data loss.
    let name = "structured-label-vars";
    let manifest = fixtures::variable_set_manifest_with_structured_label(name);
    ctx.assert_success_with_stdin(["apply", "--stdin"], &manifest, None)
        .await;

    assert_eq!(
        ctx.list_names("vs").await,
        [name],
        "the resource must be listed when no filter is applied"
    );

    // The value is a JSON object; the CLI grammar can only express string
    // equality, so no selector spelling can match it.
    assert_eq!(
        ctx.list_names_with_labels("vs", &["coordinates=1"]).await,
        Vec::<String>::new(),
        "a structured label value must not be selectable by equality"
    );

    let view = ctx.get_one(["get", "vs", name]).await;
    assert_eq!(
        view.label("coordinates").cloned(),
        Some(serde_json::json!({ "lat": 1, "lon": 2 })),
        "the structured value must still be readable via `get`"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
