// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::{
    Account,
    AccountService,
    ModifyAccountPasswordError,
    ModifyAccountPasswordUseCase,
    ModifyAccountPasswordWithConfirmationError,
    Password,
};

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ModifyAccountPasswordUseCase)]
pub struct ModifyAccountPasswordUseCaseImpl {
    account_authorization_helper: Arc<utils::AccountAuthorizationHelper>,
    account_service: Arc<dyn AccountService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ModifyAccountPasswordUseCase for ModifyAccountPasswordUseCaseImpl {
    async fn execute(
        &self,
        account: &Account,
        password: Password,
    ) -> Result<(), ModifyAccountPasswordError> {
        self.account_authorization_helper.is_admin().await?;

        self.account_service
            .modify_account_password(&account.account_name, &password)
            .await?;

        Ok(())
    }

    async fn execute_with_confirmation(
        &self,
        account: &Account,
        old_password: Password,
        new_password: Password,
    ) -> Result<(), ModifyAccountPasswordWithConfirmationError> {
        self.account_authorization_helper
            .can_modify_account(&account.account_name)
            .await?;

        self.account_service
            .verify_account_password(&account.account_name, &old_password)
            .await?;

        self.account_service
            .modify_account_password(&account.account_name, &new_password)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
