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
// Scenario: context management smoke, remote-only (QA scenario 12)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_context_management(mut client: KamuApiServerClient) {
    let context_name = "prod";
    let token = client.auth().login_as_e2e_user().await;
    let server_url = client.get_base_url().clone();

    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    kamu.execute([
        "login",
        server_url.as_str(),
        "--access-token",
        token.as_str(),
    ])
    .await
    .success();

    assert_context_present(&context_list(&kamu).await, "local", "Local");
    assert_active_context(&kamu, "local").await;

    kamu.assert_failure_command_execution(
        ["context", "delete", "local"],
        None,
        Some(["Context name 'local' is reserved"]),
    )
    .await;
    kamu.assert_failure_command_execution(
        ["context", "add", "local", "--url", server_url.as_str()],
        None,
        Some(["Context name 'local' is reserved"]),
    )
    .await;

    kamu.execute(["context", "add", context_name, "--url", server_url.as_str()])
        .await
        .success();

    assert_context_present(&context_list(&kamu).await, context_name, "Remote");

    kamu.execute(["context", "check", context_name])
        .await
        .success();

    kamu.execute(["context", "use", context_name])
        .await
        .success();

    // Exercise the bare `context` command after switching, but assert the
    // active marker through the stable JSON list format.
    kamu.execute(["context"]).await.success();
    assert_active_context(&kamu, context_name).await;

    kamu.execute(["context", "local"]).await.success();
    assert_active_context(&kamu, "local").await;

    kamu.execute(["context", "use", context_name])
        .await
        .success();
    assert_active_context(&kamu, context_name).await;

    kamu.execute(["--yes", "context", "delete", context_name])
        .await
        .success();

    assert_context_absent(&context_list(&kamu).await, context_name);
    assert_active_context(&kamu, "local").await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
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

async fn assert_active_context(kamu: &KamuCliPuppet, expected_name: &str) {
    let contexts = context_list(kamu).await;
    let active_names: Vec<_> = context_rows(&contexts, "context list -o json")
        .iter()
        .filter(|entry| entry.get("Current").and_then(serde_json::Value::as_str) == Some("*"))
        .filter_map(|entry| entry.get("Name").and_then(serde_json::Value::as_str))
        .collect();

    assert_eq!(
        active_names,
        [expected_name],
        "exactly one active context should be {expected_name}, got:\n{contexts}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_context_present(contexts: &serde_json::Value, name: &str, kind: &str) {
    assert!(
        context_rows(contexts, "context list -o json")
            .iter()
            .any(|entry| {
                entry.get("Name").and_then(serde_json::Value::as_str) == Some(name)
                    && entry.get("Kind").and_then(serde_json::Value::as_str) == Some(kind)
            }),
        "`context list -o json` should contain {kind} context {name}, got:\n{contexts}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_context_absent(contexts: &serde_json::Value, name: &str) {
    assert!(
        context_rows(contexts, "context list -o json")
            .iter()
            .all(|entry| { entry.get("Name").and_then(serde_json::Value::as_str) != Some(name) }),
        "`context list -o json` should not contain context {name}, got:\n{contexts}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn context_rows<'a>(doc: &'a serde_json::Value, label: &str) -> &'a [serde_json::Value] {
    doc.as_array()
        .unwrap_or_else(|| panic!("`{label}` should be a JSON array, got:\n{doc}"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
