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
    AccountNotFoundByIdError,
    AccountService,
    Principal,
    UpdateAccountError,
    UpdateAccountUseCase,
};
use messaging_outbox::Outbox;
use time_source::SystemTimeSource;

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct UpdateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    update_account_helper: utils::UpdateAccountUseCaseHelper,
    account_authorization_helper: Arc<dyn utils::AccountAuthorizationHelper>,
    principal: Arc<Principal>,
}

#[dill::component(pub)]
#[dill::interface(dyn UpdateAccountUseCase)]
impl UpdateAccountUseCaseImpl {
    pub fn new(
        account_service: Arc<dyn AccountService>,
        outbox: Arc<dyn Outbox>,
        time_source: Arc<dyn SystemTimeSource>,
        account_authorization_helper: Arc<dyn utils::AccountAuthorizationHelper>,
        principal: Arc<Principal>,
    ) -> Self {
        Self {
            update_account_helper: utils::UpdateAccountUseCaseHelper::new(
                account_service.clone(),
                outbox,
                time_source,
            ),
            account_service,
            account_authorization_helper,
            principal,
        }
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

        if !self
            .account_authorization_helper
            .can_modify_account(&self.principal, &original_account.account_name)
            .await?
        {
            return Err(UpdateAccountError::Access(odf::AccessError::Unauthorized(
                format!("Cannot update account {}", account.account_name).into(),
            )));
        }

        self.update_account_helper
            .update(account, &original_account)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
