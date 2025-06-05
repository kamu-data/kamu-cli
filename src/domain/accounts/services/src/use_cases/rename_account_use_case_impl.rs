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
    AccountLifecycleMessage,
    AccountService,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    RenameAccountError,
    RenameAccountUseCase,
};
use messaging_outbox::OutboxExt;

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn RenameAccountUseCase)]
pub struct RenameAccountUseCaseImpl {
    account_authorization_helper: Arc<utils::AccountAuthorizationHelper>,
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn messaging_outbox::Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl RenameAccountUseCase for RenameAccountUseCaseImpl {
    async fn execute(
        &self,
        account: &Account,
        new_name: odf::AccountName,
    ) -> Result<(), RenameAccountError> {
        self.account_authorization_helper
            .ensure_account_can_be_renamed(&account.account_name)
            .await?;

        if account.account_name == new_name {
            return Ok(());
        }

        self.account_service
            .rename_account(account, new_name.clone())
            .await?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                AccountLifecycleMessage::renamed(
                    account.id.clone(),
                    account.email.clone(),
                    account.account_name.clone(),
                    new_name,
                    account.display_name.clone(),
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
