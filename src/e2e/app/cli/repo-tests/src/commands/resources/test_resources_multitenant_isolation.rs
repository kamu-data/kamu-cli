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
// Scenario: multi-tenant resource isolation through the CLI `--account` flag
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
        (fixtures::VARIABLE_SET_KIND, "shared")
    );
    assert_eq!(alice_shared.variable("MESSAGE"), Some("alice-value"));
    let alice_shared_uid = alice_shared.uid();

    ctx.set_account(Some(bob.clone()));
    let bob_shared = ctx.get_one(["get", "vs", "shared"]).await;
    assert_eq!(bob_shared.ident(), (fixtures::VARIABLE_SET_KIND, "shared"));
    assert_eq!(bob_shared.variable("MESSAGE"), Some("bob-value"));
    let bob_shared_uid = bob_shared.uid();

    assert_ne!(
        alice_shared_uid, bob_shared_uid,
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
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_KIND).await, 4);

    ctx.set_account(Some(bob.clone()));
    pretty_assertions::assert_eq!(
        ctx.list_names("vs").await,
        vec!["app-c", "bob-only", "shared"]
    );
    pretty_assertions::assert_eq!(
        ctx.get_idents(["get", "vs", "app-%"]).await,
        ["app-c"].map(vs_ident),
    );
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_KIND).await, 3);

    ctx.set_account(Some(alice));
    ctx.assert_success(
        ["delete", "vs", "shared", "--force"],
        Some(&[
            r#"Deleted: variablesets/shared"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    ctx.set_account(Some(bob));
    let bob_shared_after_alice_delete = ctx.get_one(["get", "vs", "shared"]).await;
    assert_eq!(
        bob_shared_after_alice_delete.variable("MESSAGE"),
        Some("bob-value")
    );
    assert_eq!(
        bob_shared_after_alice_delete.uid(),
        bob_shared_uid,
        "deleting alice's `shared` resource must not replace or remove bob's copy"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn vs_ident(name: &str) -> (String, String) {
    (fixtures::VARIABLE_SET_KIND.to_string(), name.to_string())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
