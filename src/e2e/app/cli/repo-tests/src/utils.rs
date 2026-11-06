// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    KamuApiServerClient,
    KamuApiServerClientExt,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn make_logged_client(
    anonymous: &KamuApiServerClient,
    account_name: &str,
) -> KamuApiServerClient {
    let mut new_client = anonymous.clone();
    let password = generate_password(&account_name);

    new_client
        .auth()
        .login_with_password(account_name, &password)
        .await;

    new_client
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn make_logged_clients<const N: usize>(
    anonymous: &KamuApiServerClient,
    account_names: [&str; N],
) -> [KamuApiServerClient; N] {
    use std::mem::MaybeUninit;

    let mut logged_clients: [MaybeUninit<KamuApiServerClient>; N] =
        [const { MaybeUninit::uninit() }; N];

    for (i, account_name) in account_names.iter().enumerate() {
        let logged_client = make_logged_client(anonymous, account_name).await;
        logged_clients[i].write(logged_client);
    }

    unsafe { logged_clients.map(|item| item.assume_init()) }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn generate_password(account_name: &impl AsRef<str>) -> String {
    format!("test#{}", account_name.as_ref())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn dataset_id(
    kamu: &KamuCliPuppet,
    dataset_name: &odf::DatasetName,
) -> odf::DatasetID {
    kamu.list_datasets()
        .await
        .into_iter()
        .find(|dataset| &dataset.name == dataset_name)
        .unwrap()
        .id
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits a metadata event through the workspace's GraphQL API
pub(crate) async fn commit_event(
    kamu: &KamuCliPuppet,
    dataset_name: &odf::DatasetName,
    event_yaml: &str,
) {
    let dataset_id = dataset_id(kamu, dataset_name).await;
    let response = kamu
        .gql_query(&format!(
            r#"mutation {{ datasets {{ byId(datasetId: "{}") {{ metadata {{ chain {{ commitEvent(event: "{}", eventFormat: YAML) {{ __typename }} }} }} }} }} }}"#,
            dataset_id.as_did_str(),
            event_yaml.escape_default(),
        ))
        .await;

    assert!(response.contains("CommitResultSuccess"), "{response}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Makes a dataset of the current account private through the workspace's
/// GraphQL API
pub(crate) async fn set_private(kamu: &KamuCliPuppet, dataset_name: &odf::DatasetName) {
    let dataset_id = dataset_id(kamu, dataset_name).await;
    let response = kamu
        .gql_query(&format!(
            r#"mutation {{ datasets {{ byId(datasetId: "{}") {{ setVisibility(visibility: {{ private: {{}} }}) {{ __typename }} }} }} }}"#,
            dataset_id.as_did_str(),
        ))
        .await;

    assert!(
        response.contains("SetDatasetVisibilityResultSuccess"),
        "{response}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs a command that must fail and checks that its stderr contains each of
/// `expected_stderr` in that order, naming `case` in any assertion failure.
/// Returns the stderr for further checks
pub(crate) async fn assert_case_fails(
    kamu: &KamuCliPuppet,
    case: &str,
    cmd: &[&str],
    stdin: Option<&str>,
    expected_stderr: &[&str],
) -> String {
    let result = match stdin {
        Some(input) => kamu.execute_with_input(cmd, input).await,
        None => kamu.execute(cmd).await,
    };
    let output = result.get_output();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

    assert!(
        !output.status.success(),
        "Case '{case}': the command succeeded\n{stderr}"
    );
    let mut search_from = 0;
    for expected in expected_stderr {
        let Some(found_at) = stderr[search_from..].find(expected) else {
            panic!("Case '{case}': stderr has no '{expected}' after the earlier matches\n{stderr}");
        };
        search_from += found_at + expected.len();
    }

    stderr
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Adds the `player-scores` root with its first chunk of data, and the
/// `leaderboard` derivative of it, not yet pulled
pub(crate) async fn add_player_scores_with_derivative(kamu: &KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();
    kamu.ingest_data(
        &DATASET_ROOT_PLAYER_NAME,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Creates the `alice` and `bob` accounts in a multi-tenant workspace
pub(crate) async fn create_alice_and_bob(
    kamu: &KamuCliPuppet,
) -> (odf::AccountName, odf::AccountName) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");
    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;
    (alice, bob)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
