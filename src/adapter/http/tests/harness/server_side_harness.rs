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

use chrono::Utc;
use kamu::domain::auth::{
    AlwaysHappyDatasetActionAuthorizer,
    DatasetAction,
    DatasetActionAuthorizer,
};
use kamu::domain::{CompactingService, DatasetRepository, InternalError, SystemTimeSourceStub};
use kamu::testing::MockDatasetActionAuthorizer;
use kamu::DatasetLayout;
use kamu_accounts::{
    Account,
    AccountType,
    CurrentAccountSubject,
    MockAuthenticationService,
    PROVIDER_PASSWORD,
};
use opendatafabric::{AccountID, AccountName, DatasetAlias, DatasetHandle};
use reqwest::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const SERVER_ACCOUNT_NAME: &str = "kamu-server";

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub(crate) trait ServerSideHarness {
    fn operating_account_name(&self) -> Option<AccountName>;

    fn cli_dataset_repository(&self) -> Arc<dyn DatasetRepository>;

    fn cli_compacting_service(&self) -> Arc<dyn CompactingService>;

    fn dataset_layout(&self, dataset_handle: &DatasetHandle) -> DatasetLayout;

    fn dataset_url_with_scheme(&self, dataset_alias: &DatasetAlias, scheme: &str) -> Url;

    fn dataset_url(&self, dataset_alias: &DatasetAlias) -> Url {
        self.dataset_url_with_scheme(dataset_alias, "odf+http")
    }

    fn system_time_source(&self) -> &SystemTimeSourceStub;

    async fn api_server_run(self) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ServerSideHarnessOptions {
    pub multi_tenant: bool,
    pub authorized_writes: bool,
    pub base_catalog: Option<dill::Catalog>,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn server_authentication_mock() -> MockAuthenticationService {
    MockAuthenticationService::resolving_token(
        kamu_accounts::DUMMY_ACCESS_TOKEN,
        Account {
            id: AccountID::new_seeded_ed25519(SERVER_ACCOUNT_NAME.as_bytes()),
            account_name: AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
            account_type: AccountType::User,
            display_name: SERVER_ACCOUNT_NAME.to_string(),
            email: None,
            avatar_url: None,
            registered_at: Utc::now(),
            is_admin: false,
            provider: String::from(PROVIDER_PASSWORD),
            provider_identity_key: String::from(SERVER_ACCOUNT_NAME),
        },
    )
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_cli_user_catalog(base_catalog: &dill::Catalog) -> dill::Catalog {
    let is_admin = false;

    dill::CatalogBuilder::new_chained(base_catalog)
        .add_value(CurrentAccountSubject::logged(
            AccountID::new_seeded_ed25519(SERVER_ACCOUNT_NAME.as_bytes()),
            AccountName::new_unchecked(SERVER_ACCOUNT_NAME),
            is_admin,
        ))
        .add::<AlwaysHappyDatasetActionAuthorizer>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn create_web_user_catalog(
    base_catalog: &dill::Catalog,
    options: &ServerSideHarnessOptions,
) -> dill::Catalog {
    let mut web_catalog_builder = dill::CatalogBuilder::new_chained(base_catalog);
    if options.authorized_writes {
        web_catalog_builder.add::<AlwaysHappyDatasetActionAuthorizer>();
    } else {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .returning(|dataset_handle, action| {
                if action == DatasetAction::Write {
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
            .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
    }

    web_catalog_builder.build()
}

/////////////////////////////////////////////////////////////////////////////////////////
