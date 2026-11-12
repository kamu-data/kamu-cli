// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use container_runtime::TerminateStatus;
use kamu_cli_e2e_common::KamuApiServerProcess;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A multi-tenant workspace, for the `e2e-user` account, with a shutdown
/// timeout short enough to wait for in a test
pub const KAMU_CONFIG_WITH_SHORT_SHUTDOWN_TIMEOUT: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      auth:
        users:
          predefined:
            - accountName: kamu
              password: kamu.dev
              email: kamu@example.com
              properties: [ admin ]
      backgroundAgents:
        shutdownTimeout: 2s
    "#
);

/// The shutdown timeout above, plus time for the process to start exiting
const EXIT_BOUND: Duration = Duration::from_secs(5);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_api_server_exits_on_sigterm(kamu: KamuCliPuppet) {
    let mut server = KamuApiServerProcess::start(&kamu).await;

    let (status, _) = server.terminate(EXIT_BOUND).await;

    assert_exited_successfully(&status);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_api_server_exits_on_sigterm_during_a_transfer(kamu: KamuCliPuppet) {
    let mut server = KamuApiServerProcess::start(&kamu).await;
    let _session = server
        .open_idle_push_session(&odf::DatasetAlias::new(
            Some(odf::AccountName::new_unchecked("e2e-user")),
            odf::DatasetName::new_unchecked("pushed"),
        ))
        .await;

    let (status, _) = server.terminate(EXIT_BOUND).await;

    assert_exited_successfully(&status);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The server exited on its own within the bound; a server it had to kill
/// ignored the signal or outlived its shutdown timeout
fn assert_exited_successfully(status: &TerminateStatus) {
    match status {
        TerminateStatus::Exited(exit_status) => {
            assert!(exit_status.success(), "{exit_status}");
        }
        TerminateStatus::Killed => panic!("the server did not exit within {EXIT_BOUND:?}"),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
