// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::TenancyConfig;

use super::{
    ClientSideHarness,
    ClientSideHarnessOptions,
    ServerSideHarnessOptions,
    ServerSideLocalFsHarness,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A client and a local-FS server for one transfer test, each with its
/// harness defaults otherwise
pub(crate) struct LocalFsTransferOptions {
    pub client_tenancy: TenancyConfig,
    pub authenticated_remotely: bool,
    pub server_tenancy: TenancyConfig,
    pub authorized_writes: bool,
}

impl LocalFsTransferOptions {
    pub async fn build(self) -> (ClientSideHarness, ServerSideLocalFsHarness) {
        let client_harness = ClientSideHarness::new(ClientSideHarnessOptions {
            tenancy_config: self.client_tenancy,
            authenticated_remotely: self.authenticated_remotely,
        })
        .await;
        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: self.server_tenancy,
            authorized_writes: self.authorized_writes,
            ..ServerSideHarnessOptions::default()
        })
        .await;
        (client_harness, server_harness)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
