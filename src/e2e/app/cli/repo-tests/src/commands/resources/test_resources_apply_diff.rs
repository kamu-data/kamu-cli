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
// Scenario: apply diff rendering
//
// The unit tests in `resource_manifest_diff.rs` pin the renderer against
// synthetic documents. This proves the same behavior survives the real
// pipeline: canonicalization in the facade, transport (local *and* remote),
// and terminal rendering.
//
// Colors are absent here because output is not a tty, which is itself worth
// covering — a diff must stay readable when piped or captured in CI.
//
// Assertions match sequentially, in output order.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_apply_diff_rendering(ctx: ResourceCtx) {
    let name = "diff-render-vars";

    let baseline = fixtures::variable_set_manifest_yaml_rich(
        name,
        "platform",
        "gold",
        &[("ALPHA", "1"), ("BRAVO", "2"), ("CHARLIE", "3")],
    );

    // ── 1. Create renders as pure additions ──────────────────────────────────
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &baseline,
        Some(&[
            r#"Created: STDIN -> VariableSet/diff-render-vars"#,
            // A create has no `before`, so the whole manifest renders as
            // additions.
            r#"\+ \$schema:"#,
            r#"\+ +team: platform"#,
            r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed, 1 warning\(s\)"#,
        ]),
    )
    .await;

    // ── 2. A re-apply of the same manifest renders no diff at all ────────────
    // Byte-identical canonical documents are the reason this stays quiet, and
    // it is the property that used to require timestamp normalization.
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &baseline,
        Some(&[
            r#"Unchanged: STDIN -> VariableSet/diff-render-vars"#,
            r#"Summary 1 item\(s\): 0 created, 0 updated, 1 unchanged, 0 rejected, 0 failed, 1 warning\(s\)"#,
        ]),
    )
    .await;

    let stderr = ctx.stderr_with_stdin(["apply", "--stdin"], &baseline).await;
    assert!(
        !stderr.contains("spec.variables."),
        "an unchanged apply must render no diff regions, got:\n{stderr}"
    );

    // ── 3. Two independent changes render as two anchored regions ────────────
    // One label and one variable change, in separate parts of the document.
    // Each must be reported at its own narrowest path — this is what the old
    // whole-map/whole-spec granularity could not express.
    let two_changes = fixtures::variable_set_manifest_yaml_rich(
        name,
        "data",
        "gold",
        &[("ALPHA", "1"), ("BRAVO", "CHANGED"), ("CHARLIE", "3")],
    );

    ctx.assert_success_with_stdin(
        ["apply", "--stdin", "--dry-run"],
        &two_changes,
        Some(&[
            r#"Updated \(dry-run\): STDIN -> VariableSet/diff-render-vars"#,
            r#"headers\.labels\.team:"#,
            r#"- platform"#,
            r#"\+ data"#,
            r#"spec\.variables\.BRAVO\.value:"#,
            r#"- '2'"#,
            r#"\+ CHANGED"#,
            r#"Summary 1 item\(s\): 0 created, 1 updated, 0 unchanged, 0 rejected, 0 failed, 1 warning\(s\)"#,
        ]),
    )
    .await;

    // Untouched siblings must not appear *in the diff*: it is proportional to
    // the change, not to the size of the document.
    //
    // Scoped to the diff lines because these tests run with `-v`, which also
    // dumps the full resulting resource — that dump legitimately contains every
    // variable and label.
    let stderr = ctx
        .stderr_with_stdin(["apply", "--stdin", "--dry-run"], &two_changes)
        .await;
    let diff_body: String = stderr
        .lines()
        .filter(|line| {
            let t = line.trim_start();
            t.starts_with("- ") || t.starts_with("+ ")
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        !diff_body.is_empty(),
        "expected a rendered diff, got:\n{stderr}"
    );
    for untouched in ["ALPHA", "CHARLIE", "tier", "env"] {
        assert!(
            !diff_body.contains(untouched),
            "unchanged `{untouched}` must not appear in the diff, got:\n{diff_body}"
        );
    }

    // ── 4. Added and removed keys are reported separately ────────────────────
    let added_and_removed = fixtures::variable_set_manifest_yaml_rich(
        name,
        "platform",
        "gold",
        &[("ALPHA", "1"), ("BRAVO", "2"), ("DELTA", "4")],
    );

    ctx.assert_success_with_stdin(
        ["apply", "--stdin", "--dry-run"],
        &added_and_removed,
        Some(&[
            r#"Updated \(dry-run\): STDIN -> VariableSet/diff-render-vars"#,
            // CHARLIE disappears, DELTA appears; each gets its own region
            // rather than one opaque `spec.variables` replacement.
            r#"spec\.variables\.CHARLIE:"#,
            r#"spec\.variables\.DELTA:"#,
            r#"Summary 1 item\(s\): 0 created, 1 updated, 0 unchanged, 0 rejected, 0 failed, 1 warning\(s\)"#,
        ]),
    )
    .await;

    // ── 5. A live apply shows the diff too, not just a dry run ───────────────
    // This is a behavior change: previously only `--dry-run` rendered changes.
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &two_changes,
        Some(&[
            r#"Updated: STDIN -> VariableSet/diff-render-vars"#,
            r#"headers\.labels\.team:"#,
            r#"\+ data"#,
            r#"Summary 1 item\(s\): 0 created, 1 updated, 0 unchanged, 0 rejected, 0 failed, 1 warning\(s\)"#,
        ]),
    )
    .await;

    // The live apply actually persisted what its diff claimed.
    let view = ctx.get_one(["get", "vs", name]).await;
    assert_eq!(view.variable("BRAVO"), Some("CHANGED"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
