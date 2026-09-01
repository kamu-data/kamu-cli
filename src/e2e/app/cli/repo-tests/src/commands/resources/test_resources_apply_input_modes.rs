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
// Scenario: apply input modes (QA scenario 4)
//
// `apply` accepts manifests from several sources, all of which must land the
// same resource state:
//   - a YAML file path,
//   - a JSON file path,
//   - stdin (`--stdin`),
//   - an extensionless file with an explicit `--format` (and the negative: the
//     same file *without* `--format` is rejected as an unsupported extension),
//   - a directory (top-level files only by default; `--recursive` also walks
//     nested files).
//
// VariableSet only — no encryption config needed, so the harness wiring stays
// minimal. Each sub-case uses a distinct resource name so the present/absent
// assertions remain independent of one another.
//
// Manifest files are written under the workspace via
// `ResourceCtx::write_manifest`. Commands run with `current_dir` set to the
// workspace, so the relative path is passed to `apply` directly (it also
// appears verbatim in the `Created:` status line, making that assertion
// deterministic).
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_apply_input_modes(ctx: ResourceCtx) {
    // ── 1. YAML file path ─────────────────────────────────────────────────────
    {
        let name = "yaml-path-vars";
        let rel = "manifests/yaml-path.yaml";
        ctx.write_manifest(
            rel,
            &fixtures::variable_set_manifest_yaml(name, "from-yaml"),
        );

        ctx.assert_resource_absent("vs", name).await;
        ctx.assert_success(
            ["apply", rel],
            Some(&[
                &format!(r#"Created: {rel} -> VariableSet/{name}"#),
                r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;
        ctx.assert_resource_present("vs", name).await;
    }

    // ── 2. JSON file path ─────────────────────────────────────────────────────
    {
        let name = "json-path-vars";
        let rel = "manifests/json-path.json";
        ctx.write_manifest(
            rel,
            &fixtures::variable_set_manifest_json(name, "from-json"),
        );

        ctx.assert_resource_absent("vs", name).await;
        ctx.assert_success(
            ["apply", rel],
            Some(&[
                &format!(r#"Created: {rel} -> VariableSet/{name}"#),
                r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;
        ctx.assert_resource_present("vs", name).await;
    }

    // ── 3. stdin ──────────────────────────────────────────────────────────────
    {
        let name = "stdin-vars";
        let manifest = fixtures::variable_set_manifest_yaml(name, "from-stdin");

        ctx.assert_resource_absent("vs", name).await;
        ctx.assert_success_with_stdin(
            ["apply", "--stdin"],
            &manifest,
            Some(&[
                &format!(r#"Created: STDIN -> VariableSet/{name}"#),
                r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;
        ctx.assert_resource_present("vs", name).await;
    }

    // ── 4. Extensionless file ─────────────────────────────────────────────────
    //
    // Negative first: without `--format`, the extensionless file is rejected as
    // an unsupported extension and nothing is applied. Then, with `--format
    // json`, the same JSON body applies cleanly.
    {
        let name = "extless-vars";
        let rel = "manifests/extless";
        ctx.write_manifest(
            rel,
            &fixtures::variable_set_manifest_json(name, "from-extless"),
        );

        ctx.assert_resource_absent("vs", name).await;

        // No `--format`: unsupported extension → command fails, resource absent.
        ctx.assert_failure(
            ["apply", rel],
            Some(&[
                r#"Unsupported manifest extension; expected \.yaml, \.yml, or \.json, or pass --format"#,
            ]),
        )
        .await;
        ctx.assert_resource_absent("vs", name).await;

        // With `--format json`: applies successfully.
        ctx.assert_success(
            ["apply", rel, "--format", "json"],
            Some(&[
                &format!(r#"Created: {rel} -> VariableSet/{name}"#),
                r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;
        ctx.assert_resource_present("vs", name).await;
    }

    // ── 5. Directory — non-recursive (default) ────────────────────────────────
    //
    // A directory with a top-level manifest and a nested one. Without
    // `--recursive`, only the top-level file is discovered; the nested resource
    // stays absent.
    {
        let top_name = "dir-top-vars";
        let nested_name = "dir-nested-vars";
        let dir = "bundle";
        ctx.write_manifest(
            &format!("{dir}/top.yaml"),
            &fixtures::variable_set_manifest_yaml(top_name, "from-dir-top"),
        );
        ctx.write_manifest(
            &format!("{dir}/nested/deep.yaml"),
            &fixtures::variable_set_manifest_yaml(nested_name, "from-dir-deep"),
        );

        ctx.assert_resource_absent("vs", top_name).await;
        ctx.assert_resource_absent("vs", nested_name).await;

        ctx.assert_success(
            ["apply", dir],
            Some(&[
                r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;
        ctx.assert_resource_present("vs", top_name).await;
        ctx.assert_resource_absent("vs", nested_name).await;

        // ── 6. Directory — recursive ──────────────────────────────────────────
        //
        // Re-applying the same directory with `--recursive` now also picks up
        // the nested manifest. The top-level one is unchanged.
        ctx.assert_success(
            ["apply", dir, "--recursive"],
            Some(&[
                r#"Summary 2 item\(s\): 1 created, 0 updated, 1 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;
        ctx.assert_resource_present("vs", top_name).await;
        ctx.assert_resource_present("vs", nested_name).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
