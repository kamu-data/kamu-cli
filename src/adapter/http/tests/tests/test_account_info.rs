// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{Account, MockAuthenticationService, DEFAULT_ACCOUNT_ID, DUMMY_ACCESS_TOKEN};
use kamu_core::RunInfoDir;
use serde_json::json;

use crate::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_account_info_with_wrong_token() {
    let harness = AccountInfoHarness::new(false).await;

    let client = async move {
        let cl = reqwest::Client::new();

        let res = cl
            .get(&format!("{}me", harness.root_url))
            .send()
            .await
            .unwrap();
        assert_eq!(401, res.status());
        assert_eq!("Unauthorized", res.text().await.unwrap());
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

#[test_log::test(tokio::test)]
async fn test_get_account_info() {
    let harness = AccountInfoHarness::new(false).await;

    let client = async move {
        let cl = reqwest::Client::new();

        let res = cl
            .get(&format!("{}me", harness.root_url))
            .header("Authorization", format!("Bearer {DUMMY_ACCESS_TOKEN}"))
            .send()
            .await
            .unwrap();

        let dummy_account = Account::dummy();
        pretty_assertions::assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({
                "accountName": dummy_account.account_name,
                "accountType": dummy_account.account_type,
                "id": dummy_account.id,
                "avatarUrl": dummy_account.avatar_url,
                "displayName": dummy_account.display_name,
                "email": dummy_account.email,
                "isAdmin": dummy_account.is_admin,
                "provider": dummy_account.provider,
                "providerIdentityKey": dummy_account.provider_identity_key,
                "registeredAt": dummy_account.registered_at
            })
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AccountInfoHarness {
    #[allow(dead_code)]
    run_info_dir: tempfile::TempDir,
    root_url: url::Url,
    server_harness: ServerSideLocalFsHarness,
}

impl AccountInfoHarness {
    async fn new(is_multi_tenant: bool) -> Self {
        // TODO: Need access to these from harness level
        let run_info_dir = tempfile::tempdir().unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add_value(RunInfoDir::new(run_info_dir.path()))
            .build();

        let server_harness = ServerSideLocalFsHarness::new(
            ServerSideHarnessOptions {
                multi_tenant: is_multi_tenant,
                authorized_writes: true,
                base_catalog: Some(catalog),
            },
            ServerSideHarnessOverrides {
                mock_authentication_service: Some(MockAuthenticationService::resolving_by_id(
                    &DEFAULT_ACCOUNT_ID,
                    DUMMY_ACCESS_TOKEN,
                    Account::dummy(),
                )),
            },
        )
        .await;

        let root_url = url::Url::parse(
            format!("http://{}", server_harness.api_server_addr()).trim_end_matches('/'),
        )
        .unwrap();

        Self {
            run_info_dir,
            root_url,
            server_harness,
        }
    }
}
