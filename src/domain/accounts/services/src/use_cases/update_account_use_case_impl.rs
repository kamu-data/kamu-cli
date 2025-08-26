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
    AccountNotFoundByIdError,
    AccountProvider,
    AccountService,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    UpdateAccountUseCase,
    UpdateAccountUseCaseError,
};
use messaging_outbox::Outbox;

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
pub struct UpdateInnerAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn Outbox>,
}

impl UpdateInnerAccountUseCaseImpl {
    async fn notify_account_updated(
        &self,
        account: &Account,
        original_account: &Account,
    ) -> Result<(), InternalError> {
        use messaging_outbox::OutboxExt;

        if original_account.account_name != account.account_name {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                    AccountLifecycleMessage::renamed(
                        account.id.clone(),
                        account.email.clone(),
                        original_account.account_name.clone(),
                        account.account_name.clone(),
                        account.display_name.clone(),
                    ),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn execute_inner(
        &self,
        account: &Account,
        original_account: &Account,
    ) -> Result<(), UpdateAccountUseCaseError> {
        let mut account_to_update = account.clone();
        if original_account.account_name != account.account_name
            && account.provider == AccountProvider::Password.to_string()
        {
            account_to_update.provider_identity_key = account.account_name.to_string();
        }

        self.account_service
            .update_account(&account_to_update)
            .await?;

        self.notify_account_updated(original_account, &account_to_update)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn UpdateAccountUseCase)]
pub struct UpdateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    account_authorization_helper: Arc<dyn utils::AccountAuthorizationHelper>,
    update_inner_use_case: Arc<UpdateInnerAccountUseCaseImpl>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl UpdateAccountUseCase for UpdateAccountUseCaseImpl {
    async fn execute(&self, account: &Account) -> Result<(), UpdateAccountUseCaseError> {
        let Ok(original_account) = self.account_service.get_account_by_id(&account.id).await else {
            return Err(UpdateAccountUseCaseError::NotFound(
                AccountNotFoundByIdError {
                    account_id: account.id.clone(),
                },
            ));
        };

        if !self
            .account_authorization_helper
            .can_modify_account(&original_account.account_name)
            .await?
        {
            return Err(UpdateAccountUseCaseError::Access(
                odf::AccessError::Unauthorized(
                    format!("Cannot update account {}", account.account_name).into(),
                ),
            ));
        }

        self.update_inner_use_case
            .execute_inner(account, &original_account)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
