// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_set_value(kamu: KamuCliPuppet) {
    // 0. CI sets container runtime to podman for some targets, so we simulate this
    //    behavior for all others.
    kamu.assert_success_command_execution(
        ["config", "set", "engine.runtime", "podman"],
        None,
        Some(["Set engine.runtime to podman in workspace scope"]),
    )
    .await;

    // 1. Set flow for the "engine.networkNs" key
    kamu.assert_success_command_execution(
        ["config", "list"],
        Some(indoc::indoc!(
            r#"
            engine:
              runtime: podman

            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["config", "set", "engine.networkNs", "host"],
        None,
        Some(["Set engine.networkNs to host in workspace scope"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["config", "get", "engine.networkNs"],
        Some(indoc::indoc!(
            r#"
            host

            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["config", "list"],
        Some(indoc::indoc!(
            r#"
            engine:
              runtime: podman
              networkNs: host

            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    // 2. Set flow for the "uploads.maxFileSizeInMb" key
    kamu.assert_success_command_execution(
        ["config", "set", "uploads.maxFileSizeInMb", "42"],
        None,
        Some(["Set uploads.maxFileSizeInMb to 42 in workspace scope"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["config", "get", "uploads.maxFileSizeInMb"],
        Some(indoc::indoc!(
            r#"
            42

            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["config", "list"],
        Some(indoc::indoc!(
            r#"
            engine:
              runtime: podman
              networkNs: host
            uploads:
              maxFileSizeInMb: 42

            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_reset_key(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        ["config", "set", "engine.networkNs", "host"],
        None,
        Some(["Set engine.networkNs to host in workspace scope"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["config", "set", "engine.networkNs"],
        None,
        Some(["Removed engine.networkNs from workspace scope"]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["config", "get", "engine.networkNs"],
        None,
        Some(["Error: Key engine.networkNs not found"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_get_with_default(kamu: KamuCliPuppet) {
    kamu.assert_failure_command_execution(
        ["config", "get", "engine.networkNs"],
        None,
        Some(["Error: Key engine.networkNs not found"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["config", "get", "engine.networkNs", "--with-defaults"],
        Some(indoc::indoc!(
            r#"
            private

            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_config_get_from_config(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        ["config", "list"],
        Some(indoc::indoc!(
            r#"
            engine:
              runtime: podman
            uploads:
              maxFileSizeInMb: 42

            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
