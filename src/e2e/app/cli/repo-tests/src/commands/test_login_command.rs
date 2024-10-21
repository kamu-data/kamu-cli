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
use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoRecord};
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::RepoName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_password(kamu_node_api_client: KamuApiServerClient) {
    let kamu_node_url = kamu_node_api_client.get_base_url().as_str();
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    kamu.assert_success_command_execution(
        ["logout", kamu_node_url],
        None,
        Some([format!("Not logged in to {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["login", kamu_node_url, "--check"],
        None,
        Some([format!("Error: No access token found for: {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["login", "password", "kamu", "kamu", kamu_node_url],
        None,
        Some([format!("Login successful: {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["login", kamu_node_url, "--check"],
        None,
        Some([format!("Access token valid: {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    // Token validation, via an API call that requires authorization
    kamu.assert_success_command_execution(
        [
            "push",
            "player-scores",
            "--to",
            &format!("odf+{kamu_node_url}player-scores"),
        ],
        None,
        Some(["1 dataset(s) pushed"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["logout", kamu_node_url],
        None,
        Some([format!("Logged out of {kamu_node_url}").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_oauth(kamu_node_api_client: KamuApiServerClient) {
    let kamu_node_url = kamu_node_api_client.get_base_url().as_str();
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    kamu.assert_success_command_execution(
        ["logout", kamu_node_url],
        None,
        Some([format!("Not logged in to {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["login", kamu_node_url, "--check"],
        None,
        Some([format!("Error: No access token found for: {kamu_node_url}").as_str()]),
    )
    .await;

    let oauth_token = kamu_node_api_client.login_as_e2e_user().await;

    kamu.assert_success_command_execution(
        ["login", "oauth", "github", &oauth_token, kamu_node_url],
        None,
        Some([format!("Login successful: {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["login", kamu_node_url, "--check"],
        None,
        Some([format!("Access token valid: {kamu_node_url}").as_str()]),
    )
    .await;

    // Token validation, via an API call that requires authorization
    kamu_node_api_client
        .create_player_scores_dataset(&oauth_token)
        .await;

    kamu.assert_success_command_execution(
        ["logout", kamu_node_url],
        None,
        Some([format!("Logged out of {kamu_node_url}").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_add_repo(kamu: KamuCliPuppet) {
    let dummy_access_token = "dummy-access-token";
    let dummy_url = "http://example.com";

    // Login with skipping adding repo
    kamu.assert_success_command_execution(
        [
            "login",
            dummy_url,
            "--access-token",
            dummy_access_token,
            "--skip-add-repo",
        ],
        None,
        Some([format!("Login successful: {dummy_url}").as_str()]),
    )
    .await;

    let repo_list = kamu.get_list_of_repos().await;
    assert!(repo_list.is_empty());

    // Login with adding repo with default naming
    kamu.assert_success_command_execution(
        ["login", dummy_url, "--access-token", dummy_access_token],
        None,
        Some([format!("Login successful: {dummy_url}").as_str()]),
    )
    .await;

    let repo_list = kamu.get_list_of_repos().await;

    let expected_repo_list = vec![RepoRecord {
        name: RepoName::new_unchecked(url::Url::try_from(dummy_url).unwrap().host_str().unwrap()),
        url: url::Url::try_from(dummy_url).unwrap(),
    }];

    pretty_assertions::assert_eq!(expected_repo_list, repo_list);

    // Login with adding repo with provided name
    let new_dummy_url = "http://example-new.com";

    let expected_repo_name = "foo";
    kamu.assert_success_command_execution(
        [
            "login",
            new_dummy_url,
            "--access-token",
            dummy_access_token,
            "--repo-name",
            expected_repo_name,
        ],
        None,
        Some([format!("Login successful: {new_dummy_url}").as_str()]),
    )
    .await;

    let repo_list = kamu.get_list_of_repos().await;

    let expected_repo_list = vec![
        RepoRecord {
            name: RepoName::new_unchecked(
                url::Url::try_from(dummy_url).unwrap().host_str().unwrap(),
            ),
            url: url::Url::try_from(dummy_url).unwrap(),
        },
        RepoRecord {
            name: RepoName::new_unchecked(expected_repo_name),
            url: url::Url::try_from(new_dummy_url).unwrap(),
        },
    ];

    pretty_assertions::assert_eq!(expected_repo_list, repo_list);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
