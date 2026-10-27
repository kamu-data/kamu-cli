// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};
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
