// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod common_harness;
pub(crate) use common_harness::*;

mod client_side_harness;
pub(crate) use client_side_harness::*;

mod server_side_harness;
pub(crate) use server_side_harness::*;

mod server_side_s3_harness;
pub(crate) use server_side_s3_harness::*;

mod server_side_local_fs_harness;
pub(crate) use server_side_local_fs_harness::*;

mod test_api_server;
pub(crate) use test_api_server::*;

macro_rules! await_client_server_flow {
    ($api_server_handle: expr, $client_handle: expr) => {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => panic!("test timeout!"),
            _ = $api_server_handle => panic!("server-side aborted"),
            _ = $client_handle => {} // Pass, do nothing
        }
    };
}

pub(crate) use await_client_server_flow;
