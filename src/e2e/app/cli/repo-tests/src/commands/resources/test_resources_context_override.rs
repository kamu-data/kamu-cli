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

use crate::resources::{ResourceCtx, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: context override isolation, local↔remote (QA scenario 11)
//
// This is remote-only by nature: one CLI workspace keeps `local` as the active
// context while also registering a remote `prod` context backed by the e2e API
// server. Resource commands without `--context` must hit local state; commands
// with `--context prod` must hit remote state and must not change the active
// context.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_context_override_isolation(mut client: KamuApiServerClient) {
    let prod_context = "prod";
    let resource_name = "shared-name";

    let ctx = create_local_workspace_with_remote_context(&mut client, prod_context).await;

    ctx.assert_active_context_is_local().await;

    // Same resource name exists in both contexts with different values.
    ctx.apply_variable_set(resource_name, "local-value").await;

    let remote_manifest = fixtures::variable_set_manifest_yaml(resource_name, "remote-value");
    ctx.assert_success_with_stdin(
        ctx.args_with_context(["apply", "--stdin"], prod_context),
        &remote_manifest,
        None,
    )
    .await;

    // Plain get reads the local context; `--context prod` reads the remote.
    // Asserting the exact MESSAGE value (not a substring) proves context
    // isolation: each get sees its own context's value and not the other's.
    let local_view = ctx.get_one(["get", "vs", resource_name]).await;
    assert_eq!(local_view.variable("MESSAGE"), Some("local-value"));

    let remote_view = ctx
        .get_one(ctx.args_with_context(["get", "vs", resource_name], prod_context))
        .await;
    assert_eq!(remote_view.variable("MESSAGE"), Some("remote-value"));

    ctx.assert_active_context_is_local().await;

    // Exercise `--context prod` across the other resource command families.
    let remote_list = ctx
        .stdout(ctx.args_with_context(["list", "vs"], prod_context))
        .await;
    assert!(
        remote_list.contains(resource_name),
        "`list vs --context prod` should show the remote resource, got:\n{remote_list}"
    );

    ctx.assert_success(ctx.args_with_context(["summary"], prod_context), None)
        .await;

    let remote_api_resources = ctx
        .stdout(ctx.args_with_context(["context", "api-resources"], prod_context))
        .await;
    for kind in ["variablesets", "secretsets"] {
        assert!(
            remote_api_resources.contains(kind),
            "`context api-resources --context prod` should list '{kind}', \
             got:\n{remote_api_resources}"
        );
    }

    // Deleting via `--context prod` removes only the remote copy.
    ctx.assert_success(
        ctx.args_with_context(["delete", "vs", resource_name, "--force"], prod_context),
        Some(&[
            r#"Deleted: variablesets/shared-name"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    let remote_after_delete = ctx
        .stdout(ctx.args_with_context(
            ["get", "vs", resource_name, "--ignore-not-found"],
            prod_context,
        ))
        .await;
    assert!(
        !remote_after_delete.contains(resource_name),
        "remote resource should be gone after delete, got:\n{remote_after_delete}"
    );

    // The local copy is untouched by the remote delete: still present, still
    // carrying the local value.
    let local_after_delete = ctx.get_one(["get", "vs", resource_name]).await;
    assert_eq!(local_after_delete.variable("MESSAGE"), Some("local-value"));

    ctx.assert_active_context_is_local().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_local_workspace_with_remote_context(
    client: &mut KamuApiServerClient,
    context_name: &str,
) -> ResourceCtx {
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

    kamu.execute(["context", "add", context_name, "--url", server_url.as_str()])
        .await
        .success();

    ResourceCtx::Local(kamu)
}
