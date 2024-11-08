// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, TransferProtocol};

use crate::transfer_protocol::test_transfer_protocol_shared::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_pull_sequence_simple(kamu_api_server_client: KamuApiServerClient) {
    test_push_pull_sequence(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_force_push_pull_simple(kamu_api_server_client: KamuApiServerClient) {
    test_force_push_pull(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_pull_add_alias_simple(kamu_api_server_client: KamuApiServerClient) {
    test_push_pull_add_alias(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_as_simple(kamu_api_server_client: KamuApiServerClient) {
    test_pull_as(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_pull_all_simple(kamu_api_server_client: KamuApiServerClient) {
    test_push_pull_all(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_pull_recursive_simple(kamu_api_server_client: KamuApiServerClient) {
    test_push_pull_recursive(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_visibility_simple(kamu_api_server_client: KamuApiServerClient) {
    test_push_visibility(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_from_registered_repo_simple(kamu_api_server_client: KamuApiServerClient) {
    test_push_from_registered_repo(kamu_api_server_client, TransferProtocol::Simple).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
