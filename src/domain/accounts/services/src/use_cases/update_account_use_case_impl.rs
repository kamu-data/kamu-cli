// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    Account,
    AccountLifecycleMessage,
    AccountNotFoundByIdError,
    AccountProvider,
    AccountService,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    UpdateAccountError,
    UpdateAccountUseCase,
};
use messaging_outbox::Outbox;
use time_source::SystemTimeSource;

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn UpdateAccountUseCase)]
pub struct UpdateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn Outbox>,
    time_source: Arc<dyn SystemTimeSource>,
    account_authorization_helper: dill::Lazy<Arc<dyn utils::AccountAuthorizationHelper>>,
}

impl UpdateAccountUseCaseImpl {
    async fn notify_account_updated(
        &self,
        updated_account: &Account,
        original_account: &Account,
    ) -> Result<(), InternalError> {
        use messaging_outbox::OutboxExt;

        if original_account.account_name != updated_account.account_name
            || original_account.display_name != updated_account.display_name
            || original_account.email != updated_account.email
        {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                    AccountLifecycleMessage::updated(
                        self.time_source.now(),
                        updated_account.id.clone(),
                        original_account.email.clone(),
                        updated_account.email.clone(),
                        original_account.account_name.clone(),
                        updated_account.account_name.clone(),
                        original_account.display_name.clone(),
                        updated_account.display_name.clone(),
                    ),
                )
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl UpdateAccountUseCase for UpdateAccountUseCaseImpl {
    async fn execute(&self, account: &Account) -> Result<(), UpdateAccountError> {
        let Ok(original_account) = self.account_service.get_account_by_id(&account.id).await else {
            return Err(UpdateAccountError::NotFound(AccountNotFoundByIdError {
                account_id: account.id.clone(),
            }));
        };

        let account_authorization_helper = self.account_authorization_helper.get().int_err()?;
        if !account_authorization_helper
            .can_modify_account(&original_account.account_name)
            .await?
        {
            return Err(UpdateAccountError::Access(odf::AccessError::Unauthorized(
                format!("Cannot update account {}", account.account_name).into(),
            )));
        }

        self.execute_internal(account, &original_account).await
    }

    async fn execute_internal(
        &self,
        account: &Account,
        original_account: &Account,
    ) -> Result<(), UpdateAccountError> {
        let mut account_to_update = account.clone();
        if original_account.account_name != account.account_name
            && account.provider == AccountProvider::Password.to_string()
        {
            account_to_update.provider_identity_key = account.account_name.to_string();
        }

        self.account_service
            .update_account(&account_to_update)
            .await?;

        self.notify_account_updated(&account_to_update, original_account)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
