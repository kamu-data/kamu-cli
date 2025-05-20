// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use database_common::NoOpDatabasePlugin;
use kamu_accounts::*;
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{
    AccountServiceImpl,
    DeleteAccountUseCaseImpl,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac::AccountPropertyName;
use kamu_auth_rebac_services::{DefaultAccountProperties, DefaultDatasetProperties};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ADMIN: &str = "admin";
const REGULAR_USER: &str = "regular_user";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_own_account() {
    let harness =
        DeleteAccountUseCaseImplHarness::new(CurrentAccountSubject::new_test_with(&REGULAR_USER))
            .await;

    let regular_user_account = harness.get_account_by_name(&REGULAR_USER).await;

    assert_matches!(
        harness.use_case.execute(&regular_user_account).await,
        Ok(())
    );

    assert!(!harness.account_exists(&REGULAR_USER).await);

    // Idempotence
    assert_matches!(
        harness.use_case.execute(&regular_user_account).await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_delete_other_account() {
    let harness =
        DeleteAccountUseCaseImplHarness::new(CurrentAccountSubject::new_test_with(&ADMIN)).await;

    let regular_user_account = harness.get_account_by_name(&REGULAR_USER).await;

    assert_matches!(
        harness.use_case.execute(&regular_user_account).await,
        Ok(())
    );

    assert!(!harness.account_exists(&REGULAR_USER).await);

    // Idempotence
    assert_matches!(
        harness.use_case.execute(&regular_user_account).await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_delete_non_existent_account() {
    let harness =
        DeleteAccountUseCaseImplHarness::new(CurrentAccountSubject::new_test_with(&ADMIN)).await;

    let non_existent_account: Account = (&AccountConfig::test_config_from_name(
        odf::AccountName::new_unchecked("non_existent_account"),
    ))
        .into();

    assert_matches!(
        harness.use_case.execute(&non_existent_account).await,
        Err(DeleteAccountByNameError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_try_to_delete_account() {
    let harness = DeleteAccountUseCaseImplHarness::new(CurrentAccountSubject::anonymous(
        AnonymousAccountReason::NoAuthenticationProvided,
    ))
    .await;

    let regular_user_account = harness.get_account_by_name(&REGULAR_USER).await;

    assert_matches!(
        harness.use_case.execute(&regular_user_account).await,
        Err(DeleteAccountByNameError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == String::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_admin_try_to_delete_other_account() {
    let harness =
        DeleteAccountUseCaseImplHarness::new(CurrentAccountSubject::new_test_with(&REGULAR_USER))
            .await;

    let admin_user_account = harness.get_account_by_name(&ADMIN).await;

    assert_matches!(
        harness.use_case.execute(&admin_user_account).await,
        Err(DeleteAccountByNameError::Access(odf::AccessError::Unauthenticated(e)))
            if e.to_string() == String::new()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteAccountUseCaseImplHarness {
    use_case: Arc<dyn DeleteAccountUseCase>,
    account_service: Arc<dyn AccountService>,
}

impl DeleteAccountUseCaseImplHarness {
    async fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let mut b = dill::CatalogBuilder::new();

        let predefined_account_config = {
            let mut p = PredefinedAccountsConfig::new();
            p.predefined = vec![
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(ADMIN))
                    .set_properties(vec![AccountPropertyName::IsAdmin]),
                AccountConfig::test_config_from_name(odf::AccountName::new_unchecked(REGULAR_USER)),
            ];
            p
        };

        b.add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add_value(predefined_account_config)
            .add_value(current_account_subject.clone())
            // .add::<SystemTimeSourceDefault>()
            // .add::<LoginPasswordAuthProvider>()
            // .add::<RebacServiceImpl>()
            // .add::<InMemoryRebacRepository>()
            // .add_value(DidSecretEncryptionConfig::sample())
            // .add::<InMemoryDidSecretKeyRepository>()
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default())
            // .add::<DummyOutboxImpl>()
            .add::<DeleteAccountUseCaseImpl>()
            .add::<PredefinedAccountsRegistrator>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            use_case: catalog.get_one().unwrap(),
            account_service: catalog.get_one().unwrap(),
        }
    }

    async fn get_account_by_name(&self, account_name: &impl AsRef<str>) -> Account {
        self.account_service
            .account_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .unwrap()
    }

    async fn account_exists(&self, account_name: &impl AsRef<str>) -> bool {
        self.account_service
            .find_account_id_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .is_some()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
