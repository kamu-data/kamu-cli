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
// Scenario: batch apply — success & failure semantics (QA scenario 6)
//
// `apply` accepts multiple manifest paths in one invocation. This covers:
//   - ordered multi-manifest success across kinds (VariableSet + SecretSet),
//   - later-wins when the same resource appears twice (last write applies),
//   - stop-on-error (default): a rejected manifest aborts the batch, so items
//     listed *after* it are never applied,
//   - --continue-on-error: the batch processes every manifest, applying the
//     valid ones and still reporting overall failure for the rejected one.
//
// Includes a SecretSet, so the harness MUST wire
// `fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG`. Manifests are written to disk via
// `ResourceCtx::write_manifest` and applied by relative path (so the path also
// appears verbatim in the per-item status lines). State is asserted via
// `assert_resource_present`/`assert_resource_absent`; the summary counters
// (which encode created/updated/unchanged/rejected/failed exactly) carry the
// stop-vs-continue distinction.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_apply_batch(ctx: ResourceCtx) {
    // ── 1. Ordered multi-manifest success across kinds ────────────────────────
    {
        let vs_name = "batch-vars";
        let ss_name = "batch-secrets";
        ctx.write_manifest(
            "batch/vars.yaml",
            &fixtures::variable_set_manifest_yaml(vs_name, "from-batch"),
        );
        ctx.write_manifest(
            "batch/secrets.yaml",
            &fixtures::secret_set_manifest_yaml(ss_name, "tok", "pw"),
        );

        ctx.assert_resource_absent("vs", vs_name).await;
        ctx.assert_resource_absent("ss", ss_name).await;

        ctx.assert_success(
            ["apply", "batch/vars.yaml", "batch/secrets.yaml"],
            Some(&[
                r#"Summary 2 item\(s\): 2 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;

        ctx.assert_resource_present("vs", vs_name).await;
        ctx.assert_resource_present("ss", ss_name).await;
    }

    // ── 2. Later-wins when the same resource appears twice ────────────────────
    //
    // Two manifests for the same VariableSet are passed in one invocation; the
    // last one applied determines the final value (created, then updated).
    {
        let name = "later-wins-vars";
        ctx.write_manifest(
            "later/v1.yaml",
            &fixtures::variable_set_manifest_yaml(name, "old-value"),
        );
        ctx.write_manifest(
            "later/v2.yaml",
            &fixtures::variable_set_manifest_yaml(name, "new-value"),
        );

        ctx.assert_resource_absent("vs", name).await;

        ctx.assert_success(
            ["apply", "later/v1.yaml", "later/v2.yaml"],
            Some(&[
                r#"Summary 2 item\(s\): 1 created, 1 updated, 0 unchanged, 0 rejected, 0 failed"#,
            ]),
        )
        .await;

        let get_out = ctx.stdout(["get", "vs", name]).await;
        assert!(
            get_out.contains("new-value"),
            "later-wins: `get vs {name}` should show 'new-value', got:\n{get_out}"
        );
        assert!(
            !get_out.contains("old-value"),
            "later-wins: `get vs {name}` should not show 'old-value', got:\n{get_out}"
        );
    }

    // ── 3. Stop-on-error (default) ────────────────────────────────────────────
    //
    // Order: [valid-a, invalid, valid-b]. The invalid manifest (empty
    // `variables`) is rejected; without --continue-on-error the batch aborts on
    // it, so valid-b (listed after) is never applied. valid-a (before) is.
    {
        let a_name = "stop-valid-a";
        let invalid_name = "stop-invalid";
        let b_name = "stop-valid-b";
        ctx.write_manifest(
            "stop/a.yaml",
            &fixtures::variable_set_manifest_yaml(a_name, "a"),
        );
        ctx.write_manifest(
            "stop/invalid.yaml",
            &fixtures::variable_set_manifest_business_invalid(invalid_name),
        );
        ctx.write_manifest(
            "stop/b.yaml",
            &fixtures::variable_set_manifest_yaml(b_name, "b"),
        );

        ctx.assert_resource_absent("vs", a_name).await;
        ctx.assert_resource_absent("vs", b_name).await;

        // total_items counts only the items processed before the abort: a
        // (created) + invalid (rejected). valid-b is never reached.
        ctx.assert_failure(
            ["apply", "stop/a.yaml", "stop/invalid.yaml", "stop/b.yaml"],
            Some(&[
                r#"Summary 2 item\(s\): 1 created, 0 updated, 0 unchanged, 1 rejected, 0 failed"#,
                r#"Failed to apply 1 item\(s\)"#,
            ]),
        )
        .await;

        ctx.assert_resource_present("vs", a_name).await;
        ctx.assert_resource_absent("vs", b_name).await;
        ctx.assert_resource_absent("vs", invalid_name).await;
    }

    // ── 4. --continue-on-error
    // ────────────────────────────────────────────────
    //
    // Same trio, but now the batch processes all three: valid-a and valid-b are
    // applied, the invalid one is rejected, and the command still reports
    // overall failure.
    {
        let a_name = "cont-valid-a";
        let invalid_name = "cont-invalid";
        let b_name = "cont-valid-b";
        ctx.write_manifest(
            "cont/a.yaml",
            &fixtures::variable_set_manifest_yaml(a_name, "a"),
        );
        ctx.write_manifest(
            "cont/invalid.yaml",
            &fixtures::variable_set_manifest_business_invalid(invalid_name),
        );
        ctx.write_manifest(
            "cont/b.yaml",
            &fixtures::variable_set_manifest_yaml(b_name, "b"),
        );

        ctx.assert_resource_absent("vs", a_name).await;
        ctx.assert_resource_absent("vs", b_name).await;

        // All three processed: a + b created, invalid rejected.
        ctx.assert_failure(
            [
                "apply",
                "cont/a.yaml",
                "cont/invalid.yaml",
                "cont/b.yaml",
                "--continue-on-error",
            ],
            Some(&[
                r#"Summary 3 item\(s\): 2 created, 0 updated, 0 unchanged, 1 rejected, 0 failed"#,
                r#"Failed to apply 1 item\(s\)"#,
            ]),
        )
        .await;

        ctx.assert_resource_present("vs", a_name).await;
        ctx.assert_resource_present("vs", b_name).await;
        ctx.assert_resource_absent("vs", invalid_name).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
