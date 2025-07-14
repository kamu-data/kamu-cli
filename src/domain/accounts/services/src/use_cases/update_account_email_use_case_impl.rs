// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use email_utils::Email;
use kamu_accounts::{
    Account,
    AccountRepository,
    UpdateAccountEmailError,
    UpdateAccountEmailUseCase,
};

use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn UpdateAccountEmailUseCase)]
pub struct UpdateAccountEmailUseCaseImpl {
    account_authorization_helper: Arc<dyn utils::AccountAuthorizationHelper>,
    account_repo: Arc<dyn AccountRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl UpdateAccountEmailUseCase for UpdateAccountEmailUseCaseImpl {
    async fn execute(
        &self,
        account: &Account,
        new_email: Email,
    ) -> Result<(), UpdateAccountEmailError> {
        if !self
            .account_authorization_helper
            .can_modify_account(&account.account_name)
            .await?
        {
            return Err(UpdateAccountEmailError::Access(
                odf::AccessError::Unauthorized(
                    format!("Cannot update email in account {}", account.account_name).into(),
                ),
            ));
        }

        self.account_repo
            .update_account_email(&account.id, new_email.clone())
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
