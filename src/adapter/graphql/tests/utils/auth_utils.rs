// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::DatabaseTransactionRunner;
use kamu_accounts::*;
use kamu_accounts_inmem::AccountRepositoryInMemory;
use kamu_accounts_services::{LoginPasswordAuthProvider, PredefinedAccountsRegistrator};
use kamu_adapter_graphql::ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE;

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn authentication_catalogs(
    base_catalog: &dill::Catalog,
) -> (dill::Catalog, dill::Catalog) {
    let catalog_anonymous = dill::CatalogBuilder::new_chained(base_catalog)
        .add_value(CurrentAccountSubject::anonymous(
            AnonymousAccountReason::NoAuthenticationProvided,
        ))
        .build();

    let current_account_subject = CurrentAccountSubject::new_test();
    let mut predefined_accounts_config = PredefinedAccountsConfig::new();

    if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
        predefined_accounts_config
            .predefined
            .push(AccountConfig::from_name(
                logged_account.account_name.clone(),
            ));
    } else {
        panic!()
    }

    let catalog_authorized = dill::CatalogBuilder::new_chained(base_catalog)
        .add::<LoginPasswordAuthProvider>()
        .add::<PredefinedAccountsRegistrator>()
        .add::<AccountRepositoryInMemory>()
        .add_value(current_account_subject)
        .add_value(predefined_accounts_config)
        .build();

    DatabaseTransactionRunner::new(catalog_authorized.clone())
        .transactional(|transactional_catalog| async move {
            let registrator = transactional_catalog
                .get_one::<PredefinedAccountsRegistrator>()
                .unwrap();

            registrator
                .ensure_predefined_accounts_are_registered()
                .await
        })
        .await
        .unwrap();

    (catalog_anonymous, catalog_authorized)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub fn expect_anonymous_access_error(response: async_graphql::Response) {
    assert!(response.is_err());
    assert_eq!(
        response
            .errors
            .into_iter()
            .map(|e| e.message)
            .collect::<Vec<_>>(),
        vec![ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE.to_string()]
    );
}

/////////////////////////////////////////////////////////////////////////////////////////
