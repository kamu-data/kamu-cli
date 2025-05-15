// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::{Argon2Hasher, Hasher, PasswordHashingMode};
use internal_error::ResultIntoInternal;
use kamu_accounts::{ModifyPasswordError, ModifyPasswordUseCase, Password, PasswordHashRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ModifyPasswordUseCaseImpl {
    password_hash_repository: Arc<dyn PasswordHashRepository>,
    password_hashing_mode: PasswordHashingMode,
}

#[dill::component(pub)]
#[dill::interface(dyn ModifyPasswordUseCase)]
impl ModifyPasswordUseCaseImpl {
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
impl ModifyPasswordUseCase for ModifyPasswordUseCaseImpl {
    async fn execute(
        &self,
        account_name: &odf::AccountName,
        password: Password,
    ) -> Result<(), ModifyPasswordError> {
        let hashing_mode = self.password_hashing_mode;
        let password_hash = tokio::task::spawn_blocking(move || {
            let argon2_hasher = Argon2Hasher::new(hashing_mode);
            argon2_hasher.hash(password.as_bytes())
        })
        .await
        .int_err()?;

        self.password_hash_repository
            .modify_password_hash(account_name, password_hash)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
