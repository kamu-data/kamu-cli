// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::resources::{ResourceCtx, assert_resource_absent, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: SecretSet lifecycle + reveal safety (QA scenario 3)
//
// apply (2 secrets) → list shows name + count but no plaintext → get hides
// plaintext (shows only encrypted blobs) → get --revealed reveals plaintext →
// get --spec is apply-compatible (encrypted form) → get -o name → get -o name
// --revealed warns that --revealed has no effect but still succeeds → delete →
// gone.
//
// IMPORTANT: applying a `SecretSet` requires secrets encryption to be enabled,
// so this fixture must be wired with `Options::with_kamu_config(
// fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG)`. The safety crux is that the
// secret *plaintext* must never appear unless `--revealed` is passed.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_secretset_lifecycle(ctx: ResourceCtx) {
    let resource_name = "app-secrets";
    let api_token = "super-secret-token";
    let db_password = "hunter2pass";

    // ── 1. Precondition: resource is absent ──────────────────────────────────
    assert_resource_absent(&ctx, "ss", resource_name).await;

    // ── 2. Apply a SecretSet (two secret entries) via stdin ──────────────────
    let manifest = fixtures::secret_set_manifest_yaml(resource_name, api_token, db_password);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest,
        Some(&[
            r#"Created: STDIN -> SecretSet/app-secrets"#,
            r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;

    // ── 3. list secretsets — full kind name + alias; name + count visible, no
    //       plaintext ────────────────────────────────────────────────────────
    for list_args in [["list", "secretsets"], ["list", "ss"]] {
        let list_out = ctx.stdout(list_args).await;
        assert!(
            list_out.contains(resource_name),
            "`{list_args:?}` should contain '{resource_name}', got:\n{list_out}"
        );
        assert_no_plaintext(
            &list_out,
            [api_token, db_password],
            &format!("{list_args:?}"),
        );
    }

    // ── 4. get ss <name> (default) — encrypted blobs, NO plaintext ───────────
    let get_out = ctx.stdout(["get", "ss", resource_name]).await;
    assert!(
        get_out.contains(resource_name),
        "`get ss` should contain the resource name, got:\n{get_out}"
    );
    assert!(
        get_out.contains("encrypted"),
        "`get ss` (default) should show the encrypted form, got:\n{get_out}"
    );
    assert_no_plaintext(&get_out, [api_token, db_password], "get ss <name>");

    // ── 5. get ss <name> --revealed — plaintext IS shown ─────────────────────
    let revealed_out = ctx.stdout(["get", "ss", resource_name, "--revealed"]).await;
    assert!(
        revealed_out.contains(api_token),
        "`get ss --revealed` should reveal the API token, got:\n{revealed_out}"
    );
    assert!(
        revealed_out.contains(db_password),
        "`get ss --revealed` should reveal the DB password, got:\n{revealed_out}"
    );

    // ── 6. get ss <name> --spec — apply-compatible manifest (encrypted form,
    //       still no plaintext) ────────────────────────────────────────────────
    let spec_out = ctx.stdout(["get", "ss", resource_name, "--spec"]).await;
    assert!(
        spec_out.contains("apiVersion") && spec_out.contains("SecretSet"),
        "`get ss --spec` should emit a SecretSet manifest, got:\n{spec_out}"
    );
    assert!(
        spec_out.contains(resource_name),
        "`get ss --spec` should contain the resource name, got:\n{spec_out}"
    );
    assert_no_plaintext(&spec_out, [api_token, db_password], "get ss --spec");

    // ── 7. get ss <name> -o name — selector-style output ─────────────────────
    let name_out = ctx.stdout(["get", "ss", resource_name, "-o", "name"]).await;
    assert!(
        name_out.contains(resource_name),
        "`get ss -o name` should print the resource name, got:\n{name_out}"
    );

    // ── 8. get ss <name> -o name --revealed — `--revealed` has no effect with
    //       `-o name`; the CLI warns but still succeeds (not an error) ─────────
    ctx.assert_success(
        ["get", "ss", resource_name, "-o", "name", "--revealed"],
        Some(&[r#"Warning: `--revealed` has no effect with `-o name`"#]),
    )
    .await;

    // ── 9. Delete the resource ───────────────────────────────────────────────
    ctx.assert_success(
        ["delete", "ss", resource_name, "--force"],
        Some(&[
            r#"Deleted: secretsets/app-secrets"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    // ── 10. Gone: list empty, get fails ──────────────────────────────────────
    let list_after_delete = ctx.stdout(["list", "ss"]).await;
    assert!(
        !list_after_delete.contains(resource_name),
        "`list ss` after delete should not contain '{resource_name}', got:\n{list_after_delete}"
    );
    ctx.assert_failure(
        ["get", "ss", resource_name],
        Some(&[r#"Resource 'app-secrets' of kind 'SecretSet' was not found"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: apply a manifest whose secret is ALREADY encrypted (QA scenario 3)
//
// Exercises the `SecretSpec::Encrypted` apply path — the GitOps case where a
// committed manifest carries `encrypted`/`nonce` instead of a plaintext value.
// The sanitizer must accept the encrypted value as-is (not re-encrypt it), and
// `--revealed` must decrypt it back to the original plaintext.
//
// The ciphertext is tied to the configured sample encryption key; see
// `fixtures::PRE_ENCRYPTED_API_TOKEN` for how it was produced. Wire with
// `Options::with_kamu_config(fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG)`.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_secretset_apply_pre_encrypted(ctx: ResourceCtx) {
    let resource_name = "gitops-secrets";

    assert_resource_absent(&ctx, "ss", resource_name).await;

    // Apply a manifest whose secret is supplied in already-encrypted form.
    let manifest = fixtures::secret_set_manifest_pre_encrypted_yaml(resource_name);
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest,
        Some(&[
            r#"Created: STDIN -> SecretSet/gitops-secrets"#,
            r#"Summary 1 item\(s\): 1 created, 0 updated, 0 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;

    // Re-applying the same encrypted manifest is idempotent: the sanitizer
    // passes encrypted secrets through untouched, so nothing changes.
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &manifest,
        Some(&[
            r#"Unchanged: STDIN -> SecretSet/gitops-secrets"#,
            r#"Summary 1 item\(s\): 0 created, 0 updated, 1 unchanged, 0 rejected, 0 failed"#,
        ]),
    )
    .await;

    // `--revealed` decrypts the stored ciphertext back to the original
    // plaintext, proving the encrypted value was accepted and stored verbatim.
    let revealed_out = ctx.stdout(["get", "ss", resource_name, "--revealed"]).await;
    assert!(
        revealed_out.contains(fixtures::PRE_ENCRYPTED_API_TOKEN_PLAINTEXT),
        "`get ss --revealed` should decrypt the pre-encrypted secret to '{}', got:\n{revealed_out}",
        fixtures::PRE_ENCRYPTED_API_TOKEN_PLAINTEXT
    );

    // Default view must still not leak the plaintext.
    let get_out = ctx.stdout(["get", "ss", resource_name]).await;
    assert_no_plaintext(
        &get_out,
        [fixtures::PRE_ENCRYPTED_API_TOKEN_PLAINTEXT],
        "get ss <name> (pre-encrypted)",
    );

    ctx.assert_success(["delete", "ss", resource_name, "--force"], None)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Assert that none of the given secret plaintext values appear in the output —
/// the secret-safety crux of this scenario.
fn assert_no_plaintext<'a>(output: &str, plaintexts: impl IntoIterator<Item = &'a str>, ctx: &str) {
    for plaintext in plaintexts {
        assert!(
            !output.contains(plaintext),
            "`{ctx}` leaked a secret plaintext ('{plaintext}'); output:\n{output}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
