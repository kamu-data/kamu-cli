// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, CatalogBuilder};
use kamu_accounts::{
    Account,
    AccountService,
    CreateAccountUseCase,
    CurrentAccountSubject,
    DeleteAccountUseCase,
    DidSecretEncryptionConfig,
    PredefinedAccountsConfig,
    TEST_PASSWORD,
    UpdateAccountUseCase,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::utils::AccountAuthorizationHelperTestProvider;
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    DeleteAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use time_source::{SystemTimeSource, SystemTimeSourceProvider};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountBaseUseCaseHarness {
    intermediate_catalog: Catalog,
    system_time_source: Arc<dyn SystemTimeSource>,
    account_service: Arc<dyn AccountService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AccountBaseUseCaseHarness {
    pub fn new(opts: AccountBaseUseCaseHarnessOpts<'_>) -> Self {
        let intermediate_catalog = {
            let mut b = if let Some(base_catalog) = opts.maybe_base_catalog {
                CatalogBuilder::new_chained(base_catalog)
            } else {
                CatalogBuilder::new()
            };

            b.add::<InMemoryAccountRepository>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add_value(DidSecretEncryptionConfig::sample())
                .add::<AccountServiceImpl>()
                .add::<CreateAccountUseCaseImpl>()
                .add::<UpdateAccountUseCaseImpl>()
                .add::<DeleteAccountUseCaseImpl>()
                .add::<kamu_auth_rebac_services::RebacServiceImpl>()
                .add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>()
                .add_value(kamu_auth_rebac_services::DefaultAccountProperties::default())
                .add_value(kamu_auth_rebac_services::DefaultDatasetProperties::default());

            if let Some(predefined_account_config) = opts.maybe_predefined_accounts_config {
                b.add::<PredefinedAccountsRegistrator>();
                b.add::<LoginPasswordAuthProvider>();
                b.add_value(predefined_account_config);
            }

            opts.account_authorization_helper_provider
                .embed_into_catalog(&mut b);

            opts.system_time_source_provider.embed_into_catalog(&mut b);

            b.build()
        };

        Self {
            system_time_source: intermediate_catalog.get_one().unwrap(),
            account_service: intermediate_catalog.get_one().unwrap(),
            intermediate_catalog,
        }
    }

    #[inline]
    pub fn intermediate_catalog(&self) -> &Catalog {
        &self.intermediate_catalog
    }

    #[inline]
    pub fn account_service(&self) -> &dyn AccountService {
        self.account_service.as_ref()
    }

    pub async fn create_account(&self, catalog: &Catalog, account_name: &str) -> Account {
        let account = Account {
            registered_at: self.system_time_source.now(),
            ..Account::test(Self::account_id_from_name(account_name), account_name)
        };

        let create_account_uc = catalog.get_one::<dyn CreateAccountUseCase>().unwrap();
        create_account_uc
            .execute(&account, &TEST_PASSWORD, false /* quiet */)
            .await
            .unwrap()
    }

    pub async fn rename_account(&self, catalog: &Catalog, old_name: &str, new_name: &str) {
        // Locate account
        let account_svc = catalog.get_one::<dyn AccountService>().unwrap();
        let account = account_svc
            .account_by_name(&odf::AccountName::new_unchecked(old_name))
            .await
            .unwrap()
            .unwrap();

        // Prepare updated account
        let mut updated_account = account.clone();
        updated_account.account_name = odf::AccountName::new_unchecked(new_name);
        updated_account.display_name = new_name.to_string();

        // Execute update on user's behalf in authenticated context
        {
            let mut b = CatalogBuilder::new_chained(catalog);
            b.add_value(CurrentAccountSubject::logged(
                account.id.clone(),
                account.account_name.clone(),
            ));
            let authenticated_catalog = b.build();

            let update_account_uc = authenticated_catalog
                .get_one::<dyn UpdateAccountUseCase>()
                .unwrap();
            update_account_uc.execute(&updated_account).await.unwrap();
        }
    }

    pub async fn delete_account(&self, catalog: &Catalog, account_name: &str) {
        // Locate account
        let account_svc = catalog.get_one::<dyn AccountService>().unwrap();
        let account = account_svc
            .account_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .unwrap();

        // Execute delete on user's behalf in authenticated context
        {
            let mut b = dill::CatalogBuilder::new_chained(catalog);
            b.add_value(CurrentAccountSubject::logged(
                account.id.clone(),
                account.account_name.clone(),
            ));
            let authenticated_catalog = b.build();

            let delete_account_uc = authenticated_catalog
                .get_one::<dyn DeleteAccountUseCase>()
                .unwrap();
            delete_account_uc.execute(&account).await.unwrap();
        }
    }

    pub fn account_id_from_name(account_name: &str) -> odf::AccountID {
        odf::AccountID::new_seeded_ed25519(account_name.as_bytes())
    }

    pub async fn get_account_by_id(&self, account_id: &odf::AccountID) -> Account {
        self.account_service
            .get_account_by_id(account_id)
            .await
            .unwrap()
    }

    pub async fn find_account_by_name(&self, account_name: &impl AsRef<str>) -> Option<Account> {
        self.account_service
            .account_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
    }

    pub async fn get_account_by_name(&self, account_name: &impl AsRef<str>) -> Account {
        self.account_service
            .account_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .unwrap()
    }

    pub async fn account_exists(&self, account_name: &impl AsRef<str>) -> bool {
        self.account_service
            .find_account_id_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .is_some()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct AccountBaseUseCaseHarnessOpts<'a> {
    pub maybe_base_catalog: Option<&'a dill::Catalog>,
    pub system_time_source_provider: SystemTimeSourceProvider,
    pub account_authorization_helper_provider: AccountAuthorizationHelperTestProvider,
    pub maybe_predefined_accounts_config: Option<PredefinedAccountsConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
