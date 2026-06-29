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
// Scenario: ID lookup for resources (QA scenario 8)
//
// A resource ID is a stable selector for the same resource across updates, and
// stops resolving after the resource is deleted. Wired with
// `kamu_cli_resource_e2e_test!` to run against both local and remote contexts.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_id_lookup(ctx: ResourceCtx) {
    let resource_name = "id-vars";
    let initial_value = "initial-value";
    let updated_value = "updated-value";

    ctx.assert_resource_absent("vs", resource_name).await;

    ctx.apply_variable_set(resource_name, initial_value).await;

    let resource_id = ctx.resource_id("vs", resource_name).await;

    let view_by_id = ctx.get_one(["get", "variablesets", &resource_id]).await;
    assert_eq!(
        view_by_id.ident(),
        (fixtures::VARIABLE_SET_KIND, resource_name)
    );
    assert_eq!(view_by_id.variable("MESSAGE"), Some(initial_value));

    ctx.apply_variable_set(resource_name, updated_value).await;

    let updated_view_by_id = ctx.get_one(["get", "variablesets", &resource_id]).await;
    assert_eq!(
        updated_view_by_id.ident(),
        (fixtures::VARIABLE_SET_KIND, resource_name)
    );
    assert_eq!(updated_view_by_id.variable("MESSAGE"), Some(updated_value));

    ctx.assert_success(["delete", "vs", resource_name, "--force"], None)
        .await;

    ctx.assert_failure(
        ["get", "variablesets", &resource_id],
        Some(&[r"Resource with id .+ was not found"]),
    )
    .await;

    let ignored_missing = ctx
        .stdout(["get", "variablesets", &resource_id, "--ignore-not-found"])
        .await;
    assert!(
        ignored_missing.trim().is_empty(),
        "`get variablesets <id> --ignore-not-found` should emit no resources after delete, \
         got:\n{ignored_missing}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
