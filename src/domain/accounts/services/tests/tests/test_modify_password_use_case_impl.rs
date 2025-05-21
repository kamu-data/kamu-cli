// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::Argon2Hasher;
use database_common::NoOpDatabasePlugin;
use kamu_accounts::{
    Account,
    ModifyAccountPasswordUseCase,
    Password,
    PasswordHashRepository,
    DEFAULT_ACCOUNT_NAME,
};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{LoginPasswordAuthProvider, ModifyAccountPasswordUseCaseImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_account_password_use_case() {
    let harness = ModifyAccountPasswordUseCaseImplHarness::new().await;

    let password = "foo_password".to_string();

    let account = Account::dummy();

    assert!(harness
        .login_password_auth_provider
        .save_password(&account, password)
        .await
        .is_ok());

    let new_password = Password::try_new("new_foo_password").unwrap();
    assert!(harness
        .use_case
        .execute(&account, new_password.clone())
        .await
        .is_ok());

    let password_hash = harness
        .password_hash_repository
        .find_password_hash_by_account_name(&DEFAULT_ACCOUNT_NAME)
        .await
        .unwrap()
        .unwrap();

    assert!(Argon2Hasher::verify_async(
        new_password.as_bytes(),
        password_hash.as_str(),
        crypto_utils::PasswordHashingMode::Default
    )
    .await
    .unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ModifyAccountPasswordUseCaseImplHarness {
    use_case: Arc<dyn ModifyAccountPasswordUseCase>,
    login_password_auth_provider: Arc<LoginPasswordAuthProvider>,
    password_hash_repository: Arc<dyn PasswordHashRepository>,
}

impl ModifyAccountPasswordUseCaseImplHarness {
    async fn new() -> Self {
        let mut b = dill::CatalogBuilder::new();

        b.add::<InMemoryAccountRepository>()
            .add::<ModifyAccountPasswordUseCaseImpl>()
            .add::<LoginPasswordAuthProvider>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            use_case: catalog.get_one().unwrap(),
            login_password_auth_provider: catalog.get_one().unwrap(),
            password_hash_repository: catalog.get_one().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
