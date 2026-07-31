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
// Scenario: `get` selector grammar (QA scenario 7)
//
// Seeds four resources:
//   vs/app-vars    VariableSet  MESSAGE=hello
//   vs/db-creds    VariableSet  MESSAGE=dbpass
//   ss/db-creds    SecretSet    (requires SECRETS_ENCRYPTION_KAMU_CONFIG)
//   ss/app-secrets SecretSet
//
// This is a *selector* test: each case asserts which resources a selector
// resolves to (the sorted `(schema, name)` identity set) and — where it adds
// signal — the one spec field the form is about. It deliberately does NOT
// compare whole `get -o json` documents: the reconciler status block, volatile
// headers (id/account/timestamps), and encrypted secret blobs are not what
// the selector grammar is about, and re-asserting them here would break these
// tests on unrelated changes. The full document shape is pinned once, per type,
// by `test_resources_golden_view`.
//
// Covers:
//   - Three equivalent selector forms for one resource
//   - Multi-name same-type, mixed ref-form
//   - Name pattern, type pattern, type+name pattern, `%sets`
//   - `--max-results` truncation and `--unbounded`
//
// `test_resources_get_selectors_type_pattern_all_defect` below is a separate,
// deliberately failing scenario for a known defect in one selector shape — kept
// out of the main function so its failure doesn't hide whether the other nine
// cases still pass.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Identity constants — kept terse so assertions read as "which resources came
// back". `vs(..)`/`ss(..)` build the `(schema, name)` pairs `get_idents`
// returns.
fn vs(name: &str) -> (String, String) {
    (fixtures::VARIABLE_SET_SCHEMA.to_string(), name.to_string())
}
fn ss(name: &str) -> (String, String) {
    (fixtures::SECRET_SET_SCHEMA.to_string(), name.to_string())
}

pub async fn test_resources_get_selectors(ctx: ResourceCtx) {
    // ── Seed ──────────────────────────────────────────────────────────────────

    let app_vars_value = "hello";
    let db_creds_value = "dbpass";

    ctx.apply_variable_set("app-vars", app_vars_value).await;
    ctx.apply_variable_set("db-creds", db_creds_value).await;
    ctx.apply_secret_set("db-creds", "tok1", "pw1").await;
    ctx.apply_secret_set("app-secrets", "tok2", "pw2").await;

    // ── 1. Three equivalent single-resource selector forms ────────────────────
    //
    // `variablesets app-vars`, `vs app-vars`, and `vs/app-vars` must all resolve
    // to the same single VariableSet. `get_one` also asserts cardinality == 1
    // (no `items` wrapping for a single result).

    for form in [
        vec!["get", "variablesets", "app-vars"],
        vec!["get", "vs", "app-vars"],
        vec!["get", "vs/app-vars"],
    ] {
        let view = ctx.get_one(form.clone()).await;
        assert_eq!(
            view.ident(),
            (fixtures::VARIABLE_SET_SCHEMA, "app-vars"),
            "`{}` resolved to the wrong resource",
            form.join(" ")
        );
        assert_eq!(
            view.variable("MESSAGE"),
            Some(app_vars_value),
            "`{}` should carry MESSAGE={app_vars_value}",
            form.join(" ")
        );
        assert_eq!(view.description(), Some(fixtures::DEFAULT_DESCRIPTION));
    }

    // ── 2. Multi-name same-type: `get vs app-vars db-creds` ───────────────────
    //
    // Two names → both VariableSets, nothing else.

    let idents = ctx.get_idents(["get", "vs", "app-vars", "db-creds"]).await;
    assert_eq!(idents, [vs("app-vars"), vs("db-creds")]);

    // ── 3. Mixed ref-form: `get vs/app-vars ss/db-creds` ──────────────────────
    //
    // Two different types, one name each.

    let idents = ctx.get_idents(["get", "vs/app-vars", "ss/db-creds"]).await;
    assert_eq!(idents, [ss("db-creds"), vs("app-vars")]);

    // ── 4. Name pattern: `get vs app-%` ──────────────────────────────────────
    //
    // Only vs/app-vars matches `app-%` (vs/db-creds does not). Single result.

    let view = ctx.get_one(["get", "vs", "app-%"]).await;
    assert_eq!(view.ident(), (fixtures::VARIABLE_SET_SCHEMA, "app-vars"));
    assert_eq!(view.variable("MESSAGE"), Some(app_vars_value));

    // ── 5. Type pattern + exact name: `get s%/db-creds` ───────────────────────
    //
    // Case-insensitive type prefix `s%` matches SecretSet only; `db-creds`
    // selects exactly the SecretSet (not the VariableSet of the same name).

    let view = ctx.get_one(["get", "s%/db-creds"]).await;
    assert_eq!(view.ident(), (fixtures::SECRET_SET_SCHEMA, "db-creds"));
    assert!(
        view.has_secret("API_TOKEN"),
        "ss/db-creds should expose its API_TOKEN secret key:\n{}",
        view.as_json()
    );

    // ── 6. Type + name pattern: `get s%/db-%` ─────────────────────────────────
    //
    // Type starts `s`, name starts `db-` → only ss/db-creds.

    let view = ctx.get_one(["get", "s%/db-%"]).await;
    assert_eq!(view.ident(), (fixtures::SECRET_SET_SCHEMA, "db-creds"));

    // ── 7. Type pattern spanning types: `get %sets db-creds` ──────────────────
    //
    // `%sets` matches both variablesets and secretsets; name `db-creds` exists
    // in both → exactly those two.

    let idents = ctx.get_idents(["get", "%sets", "db-creds"]).await;
    assert_eq!(idents, [ss("db-creds"), vs("db-creds")]);

    // ── 8. `--max-results 1`: wildcard expansion truncated ────────────────────
    //
    // `vs %-creds` is a pattern selector, so `--max-results` applies. Only
    // vs/db-creds matches `%-creds`, so the cap of 1 returns that single
    // resource (and, being one result, no `items` wrapping).

    let view = ctx
        .get_one(["get", "vs", "%-creds", "--max-results", "1"])
        .await;
    assert_eq!(view.ident(), (fixtures::VARIABLE_SET_SCHEMA, "db-creds"));
    assert_eq!(view.variable("MESSAGE"), Some(db_creds_value));

    // ── 9. `--unbounded`: every resource ──────────────────────────────────────
    //
    // `get all --unbounded` returns all four seeded resources and only those.

    let idents = ctx.get_idents(["get", "all", "--unbounded"]).await;
    assert_eq!(
        idents,
        [
            ss("app-secrets"),
            ss("db-creds"),
            vs("app-vars"),
            vs("db-creds"),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// KNOWN DEFECT (not yet fixed): type pattern + `all` name selector.
//
// `%sets all` (equivalently `%sets/%`) resolves to `TypePatternAll`, which is
// *always* resolved via `search_handles` with both `exact_names` and
// `name_pattern` left `None` (`process_type_pattern_all_item` in
// `resource_selection_resolution_service_impl.rs`). The local facade's
// `search_handles` unconditionally rejects that combination with
// `InvalidResourceSearchQueryError` ("Resource handle search requires exact
// names or a name pattern"), so this selector shape fails for every
// multi-type-pattern-plus-`all` selection — nothing to do with labels or any
// other flag. The mock-facade unit test for this path
// (`resolves_type_pattern_all_via_search_across_matched_types`) never catches
// it because the mock does not enforce the same validation the real facade
// does. `%sets db-creds` (case 7 in `test_resources_get_selectors` above, an
// exact name) is the sibling shape that *does* work — this is the one where
// the name segment is the broad `all`/`%` selector instead.
//
// Asserts the *desired* behavior (a real match set), so this test fails until
// `TypePatternAll` is rewired to pass a query the search backend accepts —
// deliberately, so the defect stays visible in CI rather than being pinned as
// a passing "error message stays the same" check.
pub async fn test_resources_get_selectors_type_pattern_all_defect(ctx: ResourceCtx) {
    ctx.apply_variable_set("app-vars", "hello").await;
    ctx.apply_secret_set("app-secrets", "tok1", "pw1").await;

    let idents = ctx.get_idents(["get", "%sets", "all"]).await;
    assert_eq!(
        idents,
        [ss("app-secrets"), vs("app-vars")],
        "`%sets all` should resolve to every VariableSet and SecretSet, matching `%sets/%`"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
