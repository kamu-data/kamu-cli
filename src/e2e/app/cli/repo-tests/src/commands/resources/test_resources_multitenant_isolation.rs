// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;

use crate::resources::{ResourceCtx, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: multi-tenant resource isolation through the CLI `--account` flag,
// followed by `headers.account` manifest targeting (QA scenario: `--account`
// isolation + manifest-level account targeting). Both need the same
// alice/bob multi-tenant workspace, so they share one fixture and one
// `kamu_cli_execute_command_e2e_test!` instantiation instead of two.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_multitenant_isolation(kamu: KamuCliPuppet) {
    let mut ctx = ResourceCtx::Local(kamu);
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    ctx.create_account(&alice).await;
    ctx.create_account(&bob).await;

    ctx.set_account(Some(alice.clone()));
    ctx.apply_variable_set("shared", "alice-value").await;
    ctx.apply_variable_set("alice-only", "alice-only-value")
        .await;
    ctx.apply_variable_set("app-a", "alice-app-a").await;
    ctx.apply_variable_set("app-b", "alice-app-b").await;

    ctx.set_account(Some(bob.clone()));
    ctx.apply_variable_set("shared", "bob-value").await;
    ctx.apply_variable_set("bob-only", "bob-only-value").await;
    ctx.apply_variable_set("app-c", "bob-app-c").await;

    ctx.set_account(Some(alice.clone()));
    let alice_shared = ctx.get_one(["get", "vs", "shared"]).await;
    assert_eq!(
        alice_shared.ident(),
        (fixtures::VARIABLE_SET_SCHEMA, "shared")
    );
    assert_eq!(alice_shared.variable("MESSAGE"), Some("alice-value"));
    let alice_shared_id = alice_shared.id();

    ctx.set_account(Some(bob.clone()));
    let bob_shared = ctx.get_one(["get", "vs", "shared"]).await;
    assert_eq!(
        bob_shared.ident(),
        (fixtures::VARIABLE_SET_SCHEMA, "shared")
    );
    assert_eq!(bob_shared.variable("MESSAGE"), Some("bob-value"));
    let bob_shared_id = bob_shared.id();

    assert_ne!(
        alice_shared_id, bob_shared_id,
        "same resource name in different accounts must resolve to distinct UIDs"
    );

    ctx.set_account(Some(alice.clone()));
    pretty_assertions::assert_eq!(
        ctx.list_names("vs").await,
        vec!["alice-only", "app-a", "app-b", "shared"]
    );
    pretty_assertions::assert_eq!(
        ctx.get_idents(["get", "vs", "app-%"]).await,
        ["app-a", "app-b"].map(vs_ident),
    );
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_SCHEMA).await, 4);

    ctx.set_account(Some(bob.clone()));
    pretty_assertions::assert_eq!(
        ctx.list_names("vs").await,
        vec!["app-c", "bob-only", "shared"]
    );
    pretty_assertions::assert_eq!(
        ctx.get_idents(["get", "vs", "app-%"]).await,
        ["app-c"].map(vs_ident),
    );
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_SCHEMA).await, 3);

    ctx.set_account(Some(alice.clone()));
    ctx.assert_success(
        ["delete", "vs", "shared", "--force"],
        Some(&[
            r#"Deleted: VariableSet/shared"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    ctx.set_account(Some(bob.clone()));
    let bob_shared_after_alice_delete = ctx.get_one(["get", "vs", "shared"]).await;
    assert_eq!(
        bob_shared_after_alice_delete.variable("MESSAGE"),
        Some("bob-value")
    );
    assert_eq!(
        bob_shared_after_alice_delete.id(),
        bob_shared_id,
        "deleting alice's `shared` resource must not replace or remove bob's copy"
    );

    // ── `headers.account` manifest targeting ─────────────────────────────────
    //
    // Reuses the alice/bob accounts already created above. The isolation
    // scenario's own assertions are done at this point, so its leftover
    // VariableSets are cleared first: this scenario asserts exact `list_names`
    // equality against only the resources it creates.
    ctx.set_account(Some(alice));
    ctx.assert_success(["delete", "variablesets", "--all", "--force"], None)
        .await;
    ctx.set_account(Some(bob));
    ctx.assert_success(["delete", "variablesets", "--all", "--force"], None)
        .await;

    test_resources_manifest_account(&mut ctx).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_resources_manifest_account(ctx: &mut ResourceCtx) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    ctx.set_account(Some(alice.clone()));
    let alice_manifest =
        fixtures::variable_set_manifest_yaml_for_account("from-manifest", "alice-value", "alice");

    ctx.assert_success_with_stdin(["apply", "--stdin"], &alice_manifest, None)
        .await;

    let alice_resource = ctx.get_one(["get", "vs", "from-manifest"]).await;
    assert_eq!(
        alice_resource.ident(),
        (fixtures::VARIABLE_SET_SCHEMA, "from-manifest")
    );
    assert_eq!(alice_resource.variable("MESSAGE"), Some("alice-value"));

    let bob_manifest =
        fixtures::variable_set_manifest_yaml_for_account("bob-target", "bob-value", "bob");
    ctx.assert_failure_with_stdin(
        ["apply", "--stdin"],
        &bob_manifest,
        Some(&["Current subject is not allowed to use resources of account 'bob'"]),
    )
    .await;

    let unknown_manifest =
        fixtures::variable_set_manifest_yaml_for_account("unknown-target", "ghost-value", "ghost");
    ctx.assert_failure_with_stdin(
        ["apply", "--stdin"],
        &unknown_manifest,
        Some(&["Account not found by name: 'ghost'"]),
    )
    .await;

    pretty_assertions::assert_eq!(ctx.list_names("vs").await, vec!["from-manifest"]);
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_SCHEMA).await, 1);

    ctx.set_account(Some(bob));
    pretty_assertions::assert_eq!(ctx.list_names("vs").await, Vec::<String>::new());
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_SCHEMA).await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn vs_ident(name: &str) -> (String, String) {
    (fixtures::VARIABLE_SET_SCHEMA.to_string(), name.to_string())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
