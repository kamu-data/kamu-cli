// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    KamuApiServerClient,
    KamuApiServerClientExt,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_password(kamu_node_api_client: KamuApiServerClient) {
    let kamu_node_url = kamu_node_api_client.get_base_url().as_str();
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    {
        let assert = kamu.execute(["logout", kamu_node_url]).await.success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Not logged in to {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["login", kamu_node_url, "--check"])
            .await
            .failure();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Error: No access token found for: {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }

    {
        let assert = kamu
            .execute(["login", "password", "kamu", "kamu", kamu_node_url])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Login successful: {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["login", kamu_node_url, "--check"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Access token valid: {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    // Token validation, via an API call that requires authorization
    {
        let assert = kamu
            .execute([
                "push",
                "player-scores",
                "--to",
                &format!("odf+{kamu_node_url}player-scores"),
            ])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("1 dataset(s) pushed"),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu.execute(["logout", kamu_node_url]).await.success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Logged out of {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_oauth(kamu_node_api_client: KamuApiServerClient) {
    let kamu_node_url = kamu_node_api_client.get_base_url().as_str();
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    {
        let assert = kamu.execute(["logout", kamu_node_url]).await.success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Not logged in to {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["login", kamu_node_url, "--check"])
            .await
            .failure();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Error: No access token found for: {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }

    let oauth_token = kamu_node_api_client.login_as_e2e_user().await;

    {
        let assert = kamu
            .execute(["login", "oauth", "github", &oauth_token, kamu_node_url])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Login successful: {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let assert = kamu
            .execute(["login", kamu_node_url, "--check"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Access token valid: {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }

    // Token validation, via an API call that requires authorization
    kamu_node_api_client
        .create_player_scores_dataset(&oauth_token)
        .await;

    {
        let assert = kamu.execute(["logout", kamu_node_url]).await.success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains(format!("Logged out of {kamu_node_url}").as_str()),
            "Unexpected output:\n{stderr}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
