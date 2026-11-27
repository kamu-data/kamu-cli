// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::formatdoc;
use pretty_assertions::assert_eq;

use crate::resources::ResourceCtx;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_owner_references(ctx: ResourceCtx) {
    ctx.apply_variable_set("owner", "value").await;
    let owner_id = ctx.resource_id("vs", "owner").await;

    apply_owned(&ctx, "by-name", "VariableSet:owner").await;
    apply_owned(&ctx, "by-id", &format!("{{ id: {owner_id} }}")).await;
    ctx.apply_variable_set("unowned", "value").await;

    // -- 1. Every form of the owner finds the same resources ------------------
    for owner in [
        "vs/owner",
        "variablesets/OWNER",
        &format!("vs/{owner_id}"),
        &owner_id,
    ] {
        assert_eq!(
            ctx.list_names_of(&["%", "--owner", owner]).await,
            ["by-id", "by-name"],
            "{owner}"
        );
    }
    assert_eq!(
        ctx.list_names_of(&["vs/by-n%", "--owner", "vs/owner"])
            .await,
        ["by-name"]
    );

    // -- 2. `get` narrows exact names and patterns alike ----------------------
    for (selector, expected) in [
        ("vs/%", &["VariableSet/by-id", "VariableSet/by-name"][..]),
        ("vs/by-id", &["VariableSet/by-id"][..]),
    ] {
        let mut names = ctx
            .stdout(["get", selector, "--owner", "vs/owner", "-o", "name"])
            .await
            .lines()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, expected, "{selector}");
    }
    ctx.assert_failure(
        ["get", "vs/unowned", "--owner", "vs/owner"],
        Some(&["unowned"]),
    )
    .await;

    // -- 3. The rendered manifest keeps the owner when applied again ----------
    let manifest = ctx.stdout(["get", "vs/by-name", "--spec"]).await;
    assert!(manifest.contains("ownerReferences:"), "{manifest}");
    ctx.assert_success_with_stdin(["apply", "--stdin"], &manifest, Some(&["Unchanged"]))
        .await;

    for format in ["yaml", "json"] {
        let resource = ctx.stdout(["get", "vs/by-name", "-o", format]).await;
        assert!(resource.contains("ownerReferences"), "{resource}");
        assert!(resource.contains(&owner_id), "{resource}");
    }

    // -- 4. A wrong owner is refused ------------------------------------------
    ctx.assert_failure_with_stdin(
        ["apply", "--stdin"],
        &owned_manifest("misowned", "VariableSet:missing"),
        Some(&["Owner 'VariableSet:missing' is not found"]),
    )
    .await;

    // -- 5. The owner's ID outlives it, its name does not ---------------------
    ctx.assert_deleted(["delete", "vs/owner", "--force"], &["VariableSet/owner"])
        .await;
    assert_eq!(
        ctx.list_names_of(&["vs", "--owner", &owner_id]).await,
        ["by-id", "by-name"]
    );
    ctx.assert_failure(
        ["list", "vs", "--owner", "vs/owner"],
        Some(&["Cannot search by owner"]),
    )
    .await;

    // -- 6. Usage errors ------------------------------------------------------
    for owner in ["vs/own%", "%/owner", "owner", "vs", "%"] {
        ctx.assert_failure(
            ["list", "vs", "--owner", owner],
            Some(&["--owner must name one resource"]),
        )
        .await;
    }
    ctx.assert_failure(
        ["list", "vs", "--owner", "unknown/owner"],
        Some(&["Unsupported owner type 'unknown'"]),
    )
    .await;
    ctx.assert_failure(
        ["list", "datasets", "--owner", "vs/owner"],
        Some(&["--owner is supported only when listing resources"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn owned_manifest(name: &str, owner_reference: &str) -> String {
    formatdoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSet
        headers:
          name: {name}
          ownerReferences:
            - {owner_reference}
        spec:
          variables:
            KEY:
              value: value
        "#
    )
}

async fn apply_owned(ctx: &ResourceCtx, name: &str, owner_reference: &str) {
    ctx.assert_success_with_stdin(
        ["apply", "--stdin"],
        &owned_manifest(name, owner_reference),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
