// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: `context add` authenticates in one command
//
// The harness pipes stdio, so `is_tty` is false and the interactive auto-login
// never fires here — coverage runs through the credential flags instead.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_context_add_with_access_token(mut client: KamuApiServerClient) {
    let token = client.auth().login_as_e2e_user().await;
    let server_url = client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    kamu.assert_success_command_execution(
        [
            "context",
            "add",
            "prod",
            "--url",
            server_url.as_str(),
            "--access-token",
            token.as_str(),
        ],
        None,
        Some(["Added prod in workspace"]),
    )
    .await;

    assert_context_healthy(&kamu, "prod").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_context_add_with_password(client: KamuApiServerClient) {
    let server_url = client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    kamu.assert_success_command_execution(
        [
            "context",
            "add",
            "prod",
            "--url",
            server_url.as_str(),
            "--password-login",
            "e2e-user",
            "--password",
            "kamu.dev:e2e-user",
        ],
        None,
        Some(["Added prod in workspace", "Login successful"]),
    )
    .await;

    assert_context_healthy(&kamu, "prod").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_context_add_with_oauth(mut client: KamuApiServerClient) {
    let token = client.auth().login_as_e2e_user().await;
    let server_url = client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    kamu.assert_success_command_execution(
        [
            "context",
            "add",
            "prod",
            "--url",
            server_url.as_str(),
            "--oauth-provider",
            "github",
            "--oauth-token",
            token.as_str(),
        ],
        None,
        Some(["Added prod in workspace", "Login successful"]),
    )
    .await;

    assert_context_healthy(&kamu, "prod").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Without credentials and without a TTY the command must not block on a
/// browser: it registers the context and falls back to the warning.
pub async fn test_resources_context_add_without_login_warns(client: KamuApiServerClient) {
    let server_url = client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    kamu.assert_success_command_execution(
        ["context", "add", "prod", "--url", server_url.as_str()],
        None,
        Some([
            "Added prod in workspace",
            "Warning: Context 'prod' is reachable, but no access token was found",
            "Run `kamu login ",
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `--no-login` suppresses the login attempt without disturbing a session that
/// is already established.
pub async fn test_resources_context_add_no_login_flag(mut client: KamuApiServerClient) {
    let token = client.auth().login_as_e2e_user().await;
    let server_url = client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    // Without a token: registers and warns, same as the no-credentials case
    kamu.assert_success_command_execution(
        [
            "context",
            "add",
            "staging",
            "--url",
            server_url.as_str(),
            "--no-login",
        ],
        None,
        Some([
            "Added staging in workspace",
            "Warning: Context 'staging' is reachable, but no access token was found",
        ]),
    )
    .await;

    // With a token already stored: no login is performed, but the existing
    // session is left intact
    kamu.execute([
        "login",
        server_url.as_str(),
        "--access-token",
        token.as_str(),
    ])
    .await
    .success();

    let result = kamu
        .execute([
            "context",
            "add",
            "prod",
            "--url",
            server_url.as_str(),
            "--no-login",
        ])
        .await
        .success();
    let stderr = String::from_utf8_lossy(&result.get_output().stderr).into_owned();

    assert!(
        !stderr.contains("Login successful"),
        "`--no-login` must not perform a login, got:\n{stderr}"
    );

    assert_context_healthy(&kamu, "prod").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A failed login is reported and exits non-zero, but the context stays
/// registered so the user can retry against it.
pub async fn test_resources_context_add_bad_password_fails(client: KamuApiServerClient) {
    let server_url = client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    kamu.assert_failure_command_execution(
        [
            "context",
            "add",
            "prod",
            "--url",
            server_url.as_str(),
            "--password-login",
            "e2e-user",
            "--password",
            "wrong-password",
        ],
        None,
        Some([
            "Added prod in workspace",
            "Warning: Context 'prod' was registered, but login did not complete",
            "Error: ",
        ]),
    )
    .await;

    let contexts = context_list(&kamu).await;
    assert!(
        contexts
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry.get("Name").and_then(serde_json::Value::as_str) == Some("prod")),
        "context should stay registered after a failed login, got:\n{contexts}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Credential flags are mutually exclusive with `--no-login`.
pub async fn test_resources_context_add_conflicting_flags(client: KamuApiServerClient) {
    let server_url = client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    kamu.assert_failure_command_execution(
        [
            "context",
            "add",
            "prod",
            "--url",
            server_url.as_str(),
            "--no-login",
            "--access-token",
            "some-token",
        ],
        None,
        Some(["cannot be used with"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_context_healthy(kamu: &KamuCliPuppet, name: &str) {
    kamu.assert_success_command_execution(
        ["context", "check", name],
        None,
        Some([format!("Context '{name}' is reachable and access token is valid").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn context_list(kamu: &KamuCliPuppet) -> serde_json::Value {
    let result = kamu
        .execute(["context", "list", "-o", "json"])
        .await
        .success();
    let stdout = std::str::from_utf8(&result.get_output().stdout).unwrap();

    serde_json::from_str(stdout)
        .unwrap_or_else(|e| panic!("`context list -o json` did not return JSON: {e}\n{stdout}"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
