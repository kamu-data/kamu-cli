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

pub async fn test_resources_manifest_account(kamu: KamuCliPuppet) {
    let mut ctx = ResourceCtx::Local(kamu);
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    ctx.create_account(&alice).await;
    ctx.create_account(&bob).await;

    ctx.set_account(Some(alice.clone()));
    let alice_manifest =
        fixtures::variable_set_manifest_yaml_for_account("from-manifest", "alice-value", "alice");

    ctx.assert_success_with_stdin(["apply", "--stdin"], &alice_manifest, None)
        .await;

    let alice_resource = ctx.get_one(["get", "vs", "from-manifest"]).await;
    assert_eq!(
        alice_resource.ident(),
        (fixtures::VARIABLE_SET_KIND, "from-manifest")
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
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_KIND).await, 1);

    ctx.set_account(Some(bob));
    pretty_assertions::assert_eq!(ctx.list_names("vs").await, Vec::<String>::new());
    assert_eq!(ctx.summary_count(fixtures::VARIABLE_SET_KIND).await, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
