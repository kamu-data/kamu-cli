// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};
use kamu_cli_e2e_common::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_accounts_me_kamu_user(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    assert_matches!(
        kamu_api_server_client.account().me().await,
        Ok(response)
            if response.id == *DEFAULT_ACCOUNT_ID
                && response.account_name == *DEFAULT_ACCOUNT_NAME
    );
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_accounts_me_e2e_user(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_e2e_user().await;

    assert_matches!(
        kamu_api_server_client.account().me().await,
        Ok(response)
            if response.account_name == *E2E_USER_ACCOUNT_NAME
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_accounts_me_unauthorized(kamu_api_server_client: KamuApiServerClient) {
    assert_matches!(
        kamu_api_server_client.account().me().await,
        Err(AccountMeError::Unauthorized)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
