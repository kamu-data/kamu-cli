// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::ResourceCtx;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: empty context baseline (QA scenario 1)
//
// Proves resource commands behave predictably when no user resources exist.
// Wired (via `kamu_cli_resource_e2e_test!`) to run against both the local and
// remote contexts.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_empty_context_baseline(ctx: ResourceCtx) {
    // `context` reports the effective context (default output is JSON).
    let context_out = ctx.stdout(["context"]).await;
    assert!(
        context_out.contains("\"Name\""),
        "`context` should report a context row, got:\n{context_out}"
    );

    // `context api-resources` lists the supported resource kinds. This is also
    // where we exercise the per-command `--context` override path: every
    // resource command (incl. `context api-resources`) accepts `--context`, so
    // appending it must target the same context as the active switch. For the
    // local permutation `context_override_arg()` is empty (a no-op).
    let mut api_resources_args = vec!["context".to_string(), "api-resources".to_string()];
    api_resources_args.extend(ctx.context_override_arg());
    let api_resources = ctx.stdout(api_resources_args).await;
    for kind in ["variablesets", "secretsets"] {
        assert!(
            api_resources.contains(kind),
            "`context api-resources` should list '{kind}', got:\n{api_resources}"
        );
    }

    // `list all` succeeds and reports no user-created resources (empty JSON array).
    let list_all = ctx.stdout(["list", "all"]).await;
    assert_eq!(
        list_all.trim(),
        "[]",
        "`list all` on an empty context should be an empty array, got:\n{list_all}"
    );

    // `summary` succeeds (zero-count table). We only smoke-test success here;
    // detailed per-phase counts are covered by the lifecycle scenarios.
    ctx.assert_success(["summary"], None).await;

    // `get vs <missing>` fails with a not-found error...
    ctx.assert_failure(
        ["get", "vs", "missing"],
        Some(&[r#"Resource 'missing' of kind 'VariableSet' was not found"#]),
    )
    .await;

    // ...but succeeds (no error) with `--ignore-not-found`.
    ctx.assert_success(["get", "vs", "missing", "--ignore-not-found"], None)
        .await;

    // `delete vs <missing> --ignore-not-found --force` succeeds and reports the
    // missing resource as ignored.
    ctx.assert_success(
        ["delete", "vs", "missing", "--ignore-not-found", "--force"],
        Some(&[
            r#"Ignored: missing"#,
            r#"Summary 1 item\(s\): 0 deleted, 1 ignored, 0 failed"#,
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
