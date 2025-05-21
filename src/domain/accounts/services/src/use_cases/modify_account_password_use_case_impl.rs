// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::{Argon2Hasher, PasswordHashingMode};
use internal_error::ResultIntoInternal;
use kamu_accounts::{
    Account,
    ModifyAccountPasswordError,
    ModifyAccountPasswordUseCase,
    Password,
    PasswordHashRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ModifyAccountPasswordUseCaseImpl {
    password_hash_repository: Arc<dyn PasswordHashRepository>,
    password_hashing_mode: PasswordHashingMode,
}

#[dill::component(pub)]
#[dill::interface(dyn ModifyAccountPasswordUseCase)]
impl ModifyAccountPasswordUseCaseImpl {
    #[allow(clippy::needless_pass_by_value)]
    fn new(
        password_hash_repository: Arc<dyn PasswordHashRepository>,
        password_hashing_mode: Option<Arc<PasswordHashingMode>>,
    ) -> Self {
        Self {
            password_hash_repository,
            password_hashing_mode: password_hashing_mode
                .map_or(PasswordHashingMode::Default, |mode| *mode),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ModifyAccountPasswordUseCase for ModifyAccountPasswordUseCaseImpl {
    async fn execute(
        &self,
        account: &Account,
        password: Password,
    ) -> Result<(), ModifyAccountPasswordError> {
        let password_hash =
            Argon2Hasher::hash_async(password.as_bytes(), self.password_hashing_mode)
                .await
                .int_err()?;

        self.password_hash_repository
            .modify_password_hash(&account.account_name, password_hash)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
