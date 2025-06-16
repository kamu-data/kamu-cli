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
use crate::utils;

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

    /// Update account name
    #[tracing::instrument(level = "info", name = AccountMut_rename, skip_all)]
    pub async fn rename(
        &self,
        ctx: &Context<'_>,
        new_name: AccountName<'_>,
    ) -> Result<RenameAccountResult> {
        // This operation is not allowed in single-tenant mode
        utils::check_multi_tenant_config(ctx)?;

        let rename_account_use_case = from_catalog_n!(ctx, dyn RenameAccountUseCase);

        match rename_account_use_case
            .execute(&self.account, new_name.clone().into())
            .await
        {
            Ok(_) => Ok(RenameAccountResult::Success(RenameAccountSuccess {
                new_name: new_name.as_ref().to_string(),
            })),
            Err(RenameAccountError::Duplicate(_)) => Ok(RenameAccountResult::NonUniqueName(
                RenameAccountNameNotUnique::default(),
            )),
            Err(RenameAccountError::Access(access_error)) => Err(access_error.into()),
            Err(e @ RenameAccountError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Update account email
    #[tracing::instrument(level = "info", name = AccountMut_update_email, skip_all)]
    pub async fn update_email(
        &self,
        ctx: &Context<'_>,
        new_email: Email<'_>,
    ) -> Result<UpdateEmailResult> {
        // TODO: encapsulate in a service or a use case, don't use repository in GQL!
        // If decided to have a new use case, the security check should be moved there
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
    async fn modify_password(
        &self,
        ctx: &Context<'_>,
        password: AccountPassword<'_>,
    ) -> Result<ModifyPasswordResult> {
        // NOTE: Access verification is handled by the use-case

        let modify_account_password_use_case =
            from_catalog_n!(ctx, dyn ModifyAccountPasswordUseCase);

        use ModifyAccountPasswordError as E;

        match modify_account_password_use_case
            .execute(&self.account, password.into())
            .await
        {
            Ok(_) => Ok(ModifyPasswordResult::Success(
                ModifyPasswordSuccess::default(),
            )),
            Err(E::Access(e)) => Err(e.into()),
            Err(e @ E::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Change password with confirmation
    #[tracing::instrument(level = "info", name = AccountMut_modify_password_with_confirmation, skip_all)]
    async fn modify_password_with_confirmation(
        &self,
        ctx: &Context<'_>,
        old_password: AccountPassword<'_>,
        new_password: AccountPassword<'_>,
    ) -> Result<ModifyPasswordResult> {
        // NOTE: Access verification is handled by the use-case

        let modify_account_password_use_case =
            from_catalog_n!(ctx, dyn ModifyAccountPasswordUseCase);

        use ModifyAccountPasswordWithConfirmationError as E;

        match modify_account_password_use_case
            .execute_with_confirmation(&self.account, old_password.into(), new_password.into())
            .await
        {
            Ok(_) => Ok(ModifyPasswordResult::Success(
                ModifyPasswordSuccess::default(),
            )),
            Err(E::Access(e)) => Err(e.into()),
            Err(E::WrongOldPassword(_)) => Ok(ModifyPasswordResult::WrongOldPassword(
                ModifyPasswordWrongOldPassword::default(),
            )),
            Err(e @ E::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Delete a selected account. Allowed only for admin users
    #[tracing::instrument(level = "info", name = AccountMut_delete, skip_all)]
    async fn delete(&self, ctx: &Context<'_>) -> Result<DeleteAccountResult> {
        // This operation is not allowed in single-tenant mode
        utils::check_multi_tenant_config(ctx)?;

        let delete_account_use_case = from_catalog_n!(ctx, dyn DeleteAccountUseCase);

        use DeleteAccountError as E;

        match delete_account_use_case.execute(&self.account).await {
            Ok(_) => Ok(DeleteAccountResult::Success(Default::default())),
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
// RenameAccountResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RenameAccountResult {
    Success(RenameAccountSuccess),
    NonUniqueName(RenameAccountNameNotUnique),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct RenameAccountSuccess {
    pub new_name: String,
}

#[ComplexObject]
impl RenameAccountSuccess {
    pub async fn message(&self) -> String {
        "Account renamed".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct RenameAccountNameNotUnique {
    message: String,
}

impl Default for RenameAccountNameNotUnique {
    fn default() -> Self {
        Self {
            message: "Non-unique account name".to_string(),
        }
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
    WrongOldPassword(ModifyPasswordWrongOldPassword),
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

#[derive(SimpleObject)]
pub struct ModifyPasswordWrongOldPassword {
    pub message: String,
}

impl Default for ModifyPasswordWrongOldPassword {
    fn default() -> Self {
        Self {
            message: "Wrong old password".to_string(),
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
