// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::{Argon2Hasher, Hasher};
use database_common::NoOpDatabasePlugin;
use kamu_accounts::{
    ModifyPasswordUseCase,
    Password,
    PasswordHashRepository,
    DEFAULT_ACCOUNT_NAME,
};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{LoginPasswordAuthProvider, ModifyPasswordUseCaseImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_password_use_case() {
    let harness = ModifyPasswordUseCaseImplHarness::new().await;

    let password = "foo_password".to_string();

    assert!(harness
        .login_password_auth_provider
        .save_password(&DEFAULT_ACCOUNT_NAME, password)
        .await
        .is_ok());

    let new_password = Password::try_new("new_foo_password").unwrap();
    assert!(harness
        .use_case
        .execute(&DEFAULT_ACCOUNT_NAME, new_password.clone())
        .await
        .is_ok());

    let password_hash = harness
        .password_hash_repository
        .find_password_hash_by_account_name(&DEFAULT_ACCOUNT_NAME)
        .await
        .unwrap()
        .unwrap();

    tokio::task::spawn_blocking(move || {
        let argon2_hasher: Argon2Hasher<'_> =
            Argon2Hasher::new(crypto_utils::PasswordHashingMode::Default);
        assert!(argon2_hasher.verify(new_password.as_bytes(), &password_hash));
    })
    .await
    .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ModifyPasswordUseCaseImplHarness {
    _catalog: dill::Catalog,
    use_case: Arc<dyn ModifyPasswordUseCase>,
    login_password_auth_provider: Arc<LoginPasswordAuthProvider>,
    password_hash_repository: Arc<dyn PasswordHashRepository>,
}

impl ModifyPasswordUseCaseImplHarness {
    async fn new() -> Self {
        let mut b = dill::CatalogBuilder::new();

        b.add::<InMemoryAccountRepository>()
            .add::<ModifyPasswordUseCaseImpl>()
            .add::<LoginPasswordAuthProvider>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        Self {
            use_case: catalog.get_one().unwrap(),
            login_password_auth_provider: catalog.get_one().unwrap(),
            password_hash_repository: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
