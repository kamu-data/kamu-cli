// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::*;
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{
    AccountServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_adapter_graphql::ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE;
use kamu_auth_rebac::AccountPropertyName;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacDatasetLifecycleMessageConsumer,
    RebacServiceImpl,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn authentication_catalogs(
    base_catalog: &dill::Catalog,
    predefined_account_opts: PredefinedAccountOpts,
) -> (dill::Catalog, dill::Catalog) {
    authentication_catalogs_ext(base_catalog, None, predefined_account_opts).await
}

pub async fn authentication_catalogs_ext(
    base_catalog: &dill::Catalog,
    subject: Option<CurrentAccountSubject>,
    predefined_account_opts: PredefinedAccountOpts,
) -> (dill::Catalog, dill::Catalog) {
    let current_account_subject = subject.unwrap_or_else(CurrentAccountSubject::new_test);
    let mut predefined_accounts_config = PredefinedAccountsConfig::new();

    let CurrentAccountSubject::Logged(logged_account) = &current_account_subject else {
        unreachable!();
    };

    predefined_accounts_config.predefined.push(
        AccountConfig::test_config_from_subject(logged_account.clone())
            .set_properties(predefined_account_opts.into()),
    );

    let base_auth_catalog = dill::CatalogBuilder::new_chained(base_catalog)
        .add::<LoginPasswordAuthProvider>()
        .add::<PredefinedAccountsRegistrator>()
        .add::<RebacServiceImpl>()
        .add::<RebacDatasetLifecycleMessageConsumer>()
        .add::<InMemoryRebacRepository>()
        .add_value(DefaultAccountProperties::default())
        .add_value(DefaultDatasetProperties::default())
        .add::<InMemoryAccountRepository>()
        .add::<AccountServiceImpl>()
        .add_value(DidSecretEncryptionConfig::sample())
        .add_value(predefined_accounts_config)
        .build();

    let catalog_anonymous = dill::CatalogBuilder::new_chained(&base_auth_catalog)
        .add_value(CurrentAccountSubject::anonymous(
            AnonymousAccountReason::NoAuthenticationProvided,
        ))
        .build();
    let catalog_authorized = dill::CatalogBuilder::new_chained(&base_auth_catalog)
        .add_value(current_account_subject)
        .build();

    init_on_startup::run_startup_jobs(&catalog_authorized)
        .await
        .unwrap();

    (catalog_anonymous, catalog_authorized)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn expect_anonymous_access_error(response: async_graphql::Response) {
    assert!(response.is_err(), "{response:#?}");

    pretty_assertions::assert_eq!(
        vec![ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE.to_string()],
        response
            .errors
            .into_iter()
            .map(|e| e.message)
            .collect::<Vec<_>>(),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Default)]
pub struct PredefinedAccountOpts {
    pub is_admin: bool,
    pub can_provision_accounts: bool,
}

impl From<PredefinedAccountOpts> for Vec<AccountPropertyName> {
    fn from(value: PredefinedAccountOpts) -> Self {
        let mut result = Vec::new();
        if value.is_admin {
            result.push(AccountPropertyName::IsAdmin);
        }
        if value.can_provision_accounts {
            result.push(AccountPropertyName::CanProvisionAccounts);
        }
        result
    }
}
