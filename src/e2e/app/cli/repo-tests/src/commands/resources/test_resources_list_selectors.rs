// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use pretty_assertions::assert_eq;

use crate::resources::ResourceCtx;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: `list` name patterns, exact IDs, and multi-type selectors
//
// IMPORTANT: this scenario applies `SecretSet` resources, so it must be wired
// with `Options::with_kamu_config(fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG)`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_list_selectors(ctx: ResourceCtx) {
    ctx.apply_variable_set("app-vars", "app-value").await;
    ctx.apply_variable_set("temp-vars", "temp-value").await;
    ctx.apply_secret_set("app-secrets", "app-token", "app-password")
        .await;

    // -- 1. A name pattern narrows within one type ----------------------------
    assert_eq!(ctx.list_names_of(&["vs/app-%"]).await, vec!["app-vars"]);
    assert_eq!(
        ctx.list_names_of(&["vs/%"]).await,
        vec!["app-vars", "temp-vars"],
        "`vs/%` should be equivalent to `vs`"
    );

    // An exact name is a degenerate pattern, not a `get`.
    assert_eq!(
        ctx.list_names_of(&["vs/temp-vars"]).await,
        vec!["temp-vars"]
    );

    // No match is an empty listing, never a not-found error.
    assert_eq!(
        ctx.list_names_of(&["vs/nomatch-%"]).await,
        Vec::<String>::new()
    );

    // -- 2. A pattern must not change the column shape ------------------------
    assert_eq!(
        ctx.list_columns(&["vs/app-%"]).await,
        ctx.list_columns(&["vs"]).await,
        "a name pattern must not change which columns `list` renders"
    );

    // -- 3. Exact IDs, under the same `UUIDv4` rule as `get` ------------------
    let vars_id = ctx.resource_id("vs", "app-vars").await;
    let secrets_id = ctx.resource_id("ss", "app-secrets").await;

    assert_eq!(
        ctx.list_names_of(&[&format!("vs/{vars_id}")]).await,
        vec!["app-vars"]
    );

    // A bare ID resolves across types — this only holds if the lookup genuinely
    // spans every type rather than stopping at the first one tried.
    assert_eq!(ctx.list_names_of(&[&vars_id]).await, vec!["app-vars"]);
    assert_eq!(ctx.list_names_of(&[&secrets_id]).await, vec!["app-secrets"]);

    // An ID belonging to another type yields nothing rather than erroring.
    assert_eq!(
        ctx.list_names_of(&[&format!("ss/{vars_id}")]).await,
        Vec::<String>::new()
    );

    // -- 4. Multi-type selectors ----------------------------------------------
    assert_eq!(
        ctx.list_names_of(&["vs/app-%", "ss/app-%"]).await,
        vec!["app-secrets", "app-vars"]
    );

    // Each type keeps its own pattern.
    assert_eq!(
        ctx.list_names_of(&["vs/temp-%", "ss/app-%"]).await,
        vec!["app-secrets", "temp-vars"]
    );

    // A repeated type is accepted and results are deduplicated.
    assert_eq!(
        ctx.list_names_of(&["vs/app-%", "vs/%"]).await,
        vec!["app-vars", "temp-vars"]
    );

    // -- 5. Arity decides the column shape, not the results -------------------
    let multi_type_columns = ctx.list_columns(&["vs/app-%", "ss/app-%"]).await;
    assert!(
        multi_type_columns.iter().any(|column| column == "Type"),
        "a multi-type listing should carry a `Type` column, got: {multi_type_columns:?}"
    );

    // The rule itself is unit-tested in `list_command`; asserted here on the
    // *rendered* columns, which only the real binary produces: naming two types
    // keeps the generic shape even when only one of them matches.
    assert_eq!(
        ctx.list_columns(&["vs/app-%", "ss/nomatch-%"]).await,
        multi_type_columns,
        "column shape must follow the request's arity, not its results"
    );

    // -- 6. Machine output stays a single JSON document -----------------------
    let doc = ctx.list_json(&["vs/app-%", "ss/app-%"]).await;
    assert!(
        doc.is_array(),
        "`list` with several targets must still emit one JSON array, got:\n{doc}"
    );

    // -- 7. Patterns compose with label filters -------------------------------
    assert_eq!(
        ctx.list_names_with_labels("vs", &[]).await,
        vec!["app-vars", "temp-vars"]
    );

    // -- 8. `list` truncates where `get` would error --------------------------
    // Unlike `get`, overflowing the limit is not an error, so a narrowed
    // listing stays usable without `--unbounded`.
    let truncated = ctx
        .stdout(["list", "vs/%", "--max-results", "1", "-o", "json"])
        .await;
    let truncated: serde_json::Value = serde_json::from_str(&truncated)
        .unwrap_or_else(|e| panic!("`list vs/% --max-results 1` should emit JSON: {e}"));
    assert_eq!(
        truncated.as_array().map(Vec::len),
        Some(1),
        "`--max-results` should truncate rather than error, got:\n{truncated}"
    );

    // -- 9. Rejected forms -----------------------------------------------------
    for args in [
        vec!["list", "datasets", "vs"],
        vec!["list", "datasets/app-%"],
        vec!["list", "%/app-%", "vs/app-%"],
        vec!["list", "vs/a/b"],
        // The bare same-type form belongs to `get`; accepting it here would
        // start turning `list` into a second selection command.
        vec!["list", "vs", "app-vars"],
    ] {
        ctx.assert_failure(args.clone(), None).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
