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
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoRecord};
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_logout_password
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_password_st(kamu_node_api_client: KamuApiServerClient) {
    test_login_logout_password(kamu_node_api_client, false).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_password_mt(kamu_node_api_client: KamuApiServerClient) {
    test_login_logout_password(kamu_node_api_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_logout_oauth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_oauth_st(kamu_node_api_client: KamuApiServerClient) {
    test_login_logout_oauth(kamu_node_api_client, false).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_logout_oauth_mt(kamu_node_api_client: KamuApiServerClient) {
    test_login_logout_oauth(kamu_node_api_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_add_repo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_add_repo_st(kamu: KamuCliPuppet) {
    test_login_add_repo(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_add_repo_mt(kamu: KamuCliPuppet) {
    test_login_add_repo(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_login_logout_password(
    kamu_node_api_client: KamuApiServerClient,
    is_workspace_multi_tenant: bool,
) {
    let kamu_node_url = kamu_node_api_client.get_base_url().as_str();
    let dataset_alias = odf::DatasetAlias::new(None, DATASET_ROOT_PLAYER_NAME.clone());
    let kamu_api_server_dataset_endpoint = kamu_node_api_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    let kamu = KamuCliPuppet::new_workspace_tmp(is_workspace_multi_tenant).await;

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
            kamu_api_server_dataset_endpoint.as_str(),
        ],
        None,
        Some([r#"1 dataset\(s\) pushed"#]),
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

async fn test_login_logout_oauth(
    mut kamu_node_api_client: KamuApiServerClient,
    is_workspace_multi_tenant: bool,
) {
    let kamu_node_url = kamu_node_api_client.get_base_url().clone();
    let kamu = KamuCliPuppet::new_workspace_tmp(is_workspace_multi_tenant).await;

    kamu.assert_success_command_execution(
        ["logout", kamu_node_url.as_str()],
        None,
        Some([format!("Not logged in to {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["login", kamu_node_url.as_str(), "--check"],
        None,
        Some([format!("Error: No access token found for: {kamu_node_url}").as_str()]),
    )
    .await;

    let (oauth_token, _) = kamu_node_api_client.auth().login_as_e2e_user().await;

    kamu.assert_success_command_execution(
        [
            "login",
            "oauth",
            "github",
            &oauth_token,
            kamu_node_url.as_str(),
        ],
        None,
        Some([format!("Login successful: {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["login", kamu_node_url.as_str(), "--check"],
        None,
        Some([format!("Access token valid: {kamu_node_url}").as_str()]),
    )
    .await;

    // Token validation, via an API call that requires authorization
    kamu_node_api_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    kamu.assert_success_command_execution(
        ["logout", kamu_node_url.as_str()],
        None,
        Some([format!("Logged out of {kamu_node_url}").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_login_add_repo(kamu: KamuCliPuppet) {
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
    pretty_assertions::assert_eq!(Vec::<RepoRecord>::new(), repo_list);

    // Login with adding repo with default naming
    kamu.assert_success_command_execution(
        ["login", dummy_url, "--access-token", dummy_access_token],
        None,
        Some([format!("Login successful: {dummy_url}").as_str()]),
    )
    .await;

    let repo_list = kamu.get_list_of_repos().await;

    let expected_url_1 = "odf+http://example.com/";

    let expected_repo_list = vec![RepoRecord {
        name: odf::RepoName::new_unchecked(
            url::Url::try_from(dummy_url).unwrap().host_str().unwrap(),
        ),
        url: url::Url::try_from(expected_url_1).unwrap(),
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

    let expected_url_2 = "odf+http://example-new.com/";

    let expected_repo_list = vec![
        RepoRecord {
            name: odf::RepoName::new_unchecked(
                url::Url::try_from(dummy_url).unwrap().host_str().unwrap(),
            ),
            url: url::Url::try_from(expected_url_1).unwrap(),
        },
        RepoRecord {
            name: odf::RepoName::new_unchecked(expected_repo_name),
            url: url::Url::try_from(expected_url_2).unwrap(),
        },
    ];

    pretty_assertions::assert_eq!(expected_repo_list, repo_list);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
