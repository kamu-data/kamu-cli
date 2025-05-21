// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::*;

use super::AccountFlowsMut;
use crate::prelude::*;
use crate::{utils, AdminGuard};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct AccountMut {
    account: Account,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl AccountMut {
    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    /// Update account email
    #[tracing::instrument(level = "info", name = AccountMut_update_email, skip_all)]
    pub async fn update_email(
        &self,
        ctx: &Context<'_>,
        new_email: Email<'_>,
    ) -> Result<UpdateEmailResult> {
        utils::check_logged_account_name_match(ctx, &self.account.account_name)?;

        let account_repo = from_catalog_n!(ctx, dyn AccountRepository);
        match account_repo
            .update_account_email(&self.account.id, new_email.clone().into())
            .await
        {
            Ok(_) => Ok(UpdateEmailResult::Success(UpdateEmailSuccess {
                new_email: new_email.as_ref().to_string(),
            })),
            Err(UpdateAccountError::Duplicate(_)) => Ok(UpdateEmailResult::NonUniqueEmail(
                UpdateEmailNonUnique::default(),
            )),
            Err(e @ (UpdateAccountError::NotFound(_) | UpdateAccountError::Internal(_))) => {
                Err(e.int_err().into())
            }
        }
    }

    /// Reset password for a selected account. Allowed only for admin users
    #[tracing::instrument(level = "info", name = AccountMut_modify_password, skip_all)]
    #[graphql(guard = "AdminGuard")]
    async fn modify_password(
        &self,
        ctx: &Context<'_>,
        password: AccountPassword<'_>,
    ) -> Result<ModifyPasswordResult> {
        let modify_password_use_case = from_catalog_n!(ctx, dyn ModifyPasswordUseCase);

        use ModifyPasswordError as E;

        match modify_password_use_case
            .execute(&self.account.account_name, password.into())
            .await
        {
            Ok(_) => Ok(ModifyPasswordResult::Success(
                ModifyPasswordSuccess::default(),
            )),
            Err(e @ (E::Internal(_) | E::AccountNotFound(_))) => Err(e.int_err().into()),
        }
    }

    /// Delete a selected account. Allowed only for admin users
    #[tracing::instrument(level = "info", name = AccountMut_delete, skip_all)]
    async fn delete(&self, ctx: &Context<'_>) -> Result<DeleteAccountResult> {
        // NOTE: DeleteAccountUseCase handles access verification

        let delete_account_use_case = from_catalog_n!(ctx, dyn DeleteAccountUseCase);

        use DeleteAccountByNameError as E;

        match delete_account_use_case.execute(&self.account).await {
            Ok(_) => Ok(DeleteAccountResult::Success(Default::default())),
            Err(E::NotFound(e)) => unreachable!("Account should exist: {e}"),
            Err(E::Access(access_error)) => Err(access_error.into()),
            Err(e @ E::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Access to the mutable flow configurations of this account
    #[expect(clippy::unused_async)]
    async fn flows(&self, ctx: &Context<'_>) -> Result<AccountFlowsMut> {
        utils::check_logged_account_name_match(ctx, &self.account.account_name)?;

        Ok(AccountFlowsMut::new(&self.account))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// UpdateEmailResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UpdateEmailResult {
    Success(UpdateEmailSuccess),
    NonUniqueEmail(UpdateEmailNonUnique),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct UpdateEmailSuccess {
    pub new_email: String,
}

#[ComplexObject]
impl UpdateEmailSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct UpdateEmailNonUnique {
    message: String,
}

impl Default for UpdateEmailNonUnique {
    fn default() -> Self {
        Self {
            message: "Non-unique email".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ModifyPasswordResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "&String"))]
pub enum ModifyPasswordResult {
    Success(ModifyPasswordSuccess),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct ModifyPasswordSuccess {
    pub message: String,
}

impl Default for ModifyPasswordSuccess {
    fn default() -> Self {
        Self {
            message: "Password modified".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DeleteAccountResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteAccountResult {
    Success(DeleteAccountSuccess),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct DeleteAccountSuccess {
    message: String,
}

impl Default for DeleteAccountSuccess {
    fn default() -> Self {
        Self {
            message: "Account deleted".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
