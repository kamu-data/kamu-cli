// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_accounts::{
    Account,
    AccountLifecycleMessage,
    AccountService,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ModifyAccountPasswordError,
    ModifyAccountPasswordUseCase,
    ModifyAccountPasswordWithConfirmationError,
    Password,
};
use messaging_outbox::Outbox;

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ModifyAccountPasswordUseCase)]
pub struct ModifyAccountPasswordUseCaseImpl {
    account_authorization_helper: Arc<dyn utils::AccountAuthorizationHelper>,
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn Outbox>,
}

impl ModifyAccountPasswordUseCaseImpl {
    async fn notify_password_changed(&self, account: &Account) -> Result<(), InternalError> {
        use messaging_outbox::OutboxExt;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                AccountLifecycleMessage::password_changed(
                    account.id.clone(),
                    account.email.clone(),
                    account.display_name.clone(),
                ),
            )
            .await?;

        Ok(())
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
        self.account_authorization_helper
            .ensure_account_password_can_be_modified(&account.account_name)
            .await?;

        self.account_service
            .modify_account_password(&account.account_name, &password)
            .await?;

        self.notify_password_changed(account).await?;

        Ok(())
    }

    async fn execute_with_confirmation(
        &self,
        account: &Account,
        old_password: Password,
        new_password: Password,
    ) -> Result<(), ModifyAccountPasswordWithConfirmationError> {
        self.account_authorization_helper
            .ensure_account_password_with_confirmation_can_be_modified(&account.account_name)
            .await?;

        self.account_service
            .verify_account_password(&account.account_name, &old_password)
            .await?;

        self.account_service
            .modify_account_password(&account.account_name, &new_password)
            .await?;

        self.notify_password_changed(account).await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
