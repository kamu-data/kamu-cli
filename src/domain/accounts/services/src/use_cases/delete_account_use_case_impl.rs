// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::*;

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DeleteAccountUseCase)]
pub struct DeleteAccountUseCaseImpl {
    account_authorization_helper: Arc<utils::AccountAuthorizationHelper>,
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn messaging_outbox::Outbox>,
}

#[async_trait::async_trait]
impl DeleteAccountUseCase for DeleteAccountUseCaseImpl {
    async fn execute(&self, account: &Account) -> Result<(), DeleteAccountByNameError> {
        self.account_authorization_helper
            .ensure_account_can_be_deleted(&account.account_name)
            .await?;

        self.account_service
            .delete_account_by_name(&account.account_name)
            .await?;

        use messaging_outbox::OutboxExt;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                AccountLifecycleMessage::deleted(
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
