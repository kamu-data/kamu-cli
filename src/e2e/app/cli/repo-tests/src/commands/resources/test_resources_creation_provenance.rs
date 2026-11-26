// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde_json::json;

use crate::resources::{ResourceCtx, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: creation provenance as `get -o yaml` reports it
//
// A created resource carries a `CreationProvenance` condition naming the
// acting user and the boundary the apply came through: `Cli` for a local
// workspace, the GraphQL API for a remote one. An update leaves it as it was.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_creation_provenance(ctx: ResourceCtx) {
    let name = "provenance-vars";

    ctx.apply_variable_set(name, "one").await;
    let (created, account) = provenance_and_account(&ctx, name).await;

    // The caller applies into their own account
    assert_eq!(
        created["principal"]["type"],
        json!("https://opendatafabric.org/schemas/auth/v1alpha1/User")
    );
    assert!(
        account["name"].is_string() && account["did"].is_string(),
        "{account:#}"
    );
    assert_eq!(created["principal"]["name"], account["name"]);
    assert_eq!(created["principal"]["did"], account["did"]);
    let expected_origin = match ctx.context_name() {
        None => json!({ "kind": "Cli" }),
        Some(_) => json!({ "kind": "Api", "api": "GraphQl" }),
    };
    assert_eq!(created["origin"], expected_origin);

    ctx.apply_variable_set(name, "two").await;
    assert_eq!(provenance_and_account(&ctx, name).await.0, created);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The provenance of the resource and the account that owns it
async fn provenance_and_account(
    ctx: &ResourceCtx,
    name: &str,
) -> (serde_json::Value, serde_json::Value) {
    let view = ctx
        .stdout_yaml_as_json(["get", "vs", name, "-o", "yaml"])
        .await;
    let provenance = view["status"]["conditions"]
        .get(fixtures::CREATION_PROVENANCE_CONDITION_SCHEMA)
        .unwrap_or_else(|| panic!("no creation provenance in {view:#}"))
        .clone();
    (provenance, view["headers"]["account"].clone())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
