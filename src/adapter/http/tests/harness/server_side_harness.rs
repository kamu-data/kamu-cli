// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use std::sync::Arc;

use kamu::domain::auth::{AccountType, DEFAULT_AVATAR_URL};
use kamu::domain::{auth, CurrentAccountSubject, DatasetRepository, InternalError};
use kamu::testing::{MockAuthenticationService, MockDatasetActionAuthorizer};
use kamu::DatasetLayout;
use opendatafabric::{AccountName, DatasetAlias, DatasetHandle, FAKE_ACCOUNT_ID};
use reqwest::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const SERVER_ACCOUNT_NAME: &str = "kamu-server";

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub(crate) trait ServerSideHarness {
    fn operating_account_name(&self) -> Option<AccountName>;

    fn cli_dataset_repository(&self) -> Arc<dyn DatasetRepository>;

    fn dataset_layout(&self, dataset_handle: &DatasetHandle) -> DatasetLayout;

    fn dataset_url(&self, dataset_alias: &DatasetAlias) -> Url;

    async fn api_server_run(self) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ServerSideHarnessOptions {
    pub multi_tenant: bool,
    pub authorized_writes: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn server_authentication_mock() -> MockAuthenticationService {
    MockAuthenticationService::resolving_token(
        kamu::domain::auth::DUMMY_ACCESS_TOKEN,
        kamu::domain::auth::AccountInfo {
            account_id: FAKE_ACCOUNT_ID.to_string(),
            account_name: AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
            account_type: AccountType::User,
            display_name: SERVER_ACCOUNT_NAME.to_string(),
            avatar_url: Some(DEFAULT_AVATAR_URL.to_string()),
        },
    )
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_cli_user_catalog(base_catalog: &dill::Catalog) -> dill::Catalog {
    dill::CatalogBuilder::new_chained(base_catalog)
        .add_value(CurrentAccountSubject::logged(AccountName::new_unchecked(
            SERVER_ACCOUNT_NAME,
        )))
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .bind::<dyn auth::DatasetActionAuthorizer, auth::AlwaysHappyDatasetActionAuthorizer>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_web_user_catalog(
    base_catalog: &dill::Catalog,
    options: &ServerSideHarnessOptions,
) -> dill::Catalog {
    let mut web_catalog_builder = dill::CatalogBuilder::new_chained(&base_catalog);
    if options.authorized_writes {
        web_catalog_builder
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .bind::<dyn auth::DatasetActionAuthorizer, auth::AlwaysHappyDatasetActionAuthorizer>();
    } else {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(|dataset_handle, action| {
                if action == auth::DatasetAction::Write {
                    Err(MockDatasetActionAuthorizer::denying_error(
                        dataset_handle,
                        action,
                    ))
                } else {
                    Ok(())
                }
            });
        web_catalog_builder
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
    }

    web_catalog_builder.build()
}

/////////////////////////////////////////////////////////////////////////////////////////
