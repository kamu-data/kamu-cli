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
use kamu_accounts_services::{LoginPasswordAuthProvider, PredefinedAccountsRegistrator};
use kamu_adapter_graphql::ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn authentication_catalogs(
    base_catalog: &dill::Catalog,
) -> (dill::Catalog, dill::Catalog) {
    let current_account_subject = CurrentAccountSubject::new_test();
    let mut predefined_accounts_config = PredefinedAccountsConfig::new();

    if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
        predefined_accounts_config
            .predefined
            .push(AccountConfig::test_config_from_name(
                logged_account.account_name.clone(),
            ));
    } else {
        unreachable!();
    }

    let base_auth_catalog = dill::CatalogBuilder::new_chained(base_catalog)
        .add::<LoginPasswordAuthProvider>()
        .add::<PredefinedAccountsRegistrator>()
        .add::<InMemoryAccountRepository>()
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
