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
use crate::queries::Account as AccountView;
use crate::utils::ensure_account_can_provision_accounts;
use crate::AdminGuard;

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

    /// Create a new account
    async fn create_account(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
        email: Option<Email<'_>>,
    ) -> Result<CreateAccountResult> {
        ensure_account_can_provision_accounts(ctx, &self.account.id).await?;

        let create_account_use_case = from_catalog_n!(ctx, dyn CreateAccountUseCase);

        match create_account_use_case
            .execute(&self.account, account_name.as_ref(), email.map(Into::into))
            .await
        {
            Ok(created_account) => Ok(CreateAccountResult::Success(CreateAccountSuccess {
                account: AccountView::from_account(created_account),
            })),
            Err(CreateAccountError::Duplicate(err)) => Ok(
                CreateAccountResult::NonUniqueAccountField(AccountFieldNonUnique {
                    field: err.account_field.to_string(),
                }),
            ),
            Err(e @ CreateAccountError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Reset password for a selected account. Allowed only for admin users
    #[graphql(guard = "AdminGuard::new()")]
    async fn modify_password(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
        password: String,
    ) -> Result<ModifyPasswordResult> {
        let account_service = from_catalog_n!(ctx, dyn AccountService);
        let password = match Password::try_new(password.as_str()) {
            Ok(pass) => pass,
            Err(err) => {
                return Ok(ModifyPasswordResult::InvalidPassword(
                    ModifyPasswordInvalidPassword {
                        reason: err.to_string(),
                    },
                ))
            }
        };

        match account_service
            .modify_password(&account_name, password)
            .await
        {
            Ok(_) => Ok(ModifyPasswordResult::Success(
                ModifyPasswordSuccess::default(),
            )),
            Err(ModifyPasswordError::AccountNotFound(_)) => Ok(
                ModifyPasswordResult::AccountNotFound(AccountNotFound::default()),
            ),
            Err(e @ ModifyPasswordError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Deleting an account by name
    async fn delete_account_by_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
    ) -> Result<DeleteAccountResult> {
        ensure_account_can_provision_accounts(ctx, &self.account.id).await?;

        let delete_account_use_case = from_catalog_n!(ctx, dyn DeleteAccountUseCase);

        use DeleteAccountError as E;

        match delete_account_use_case.execute(account_name.as_ref()).await {
            Ok(_) => Ok(DeleteAccountResult::Success(Default::default())),
            Err(E::NotFound(_)) => Ok(DeleteAccountResult::AccountNotFound(Default::default())),
            Err(e @ E::Internal(_)) => Err(e.int_err().into()),
        }
    }

    /// Access to the mutable flow configurations of this account
    async fn flows(&self) -> AccountFlowsMut {
        AccountFlowsMut::new(self.account.clone())
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
// CreateAccountResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateAccountResult {
    Success(CreateAccountSuccess),
    InvalidEmail(CreateAccountEmailInvalid),
    NonUniqueAccountField(AccountFieldNonUnique),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAccountSuccess {
    pub account: AccountView,
}

#[ComplexObject]
impl CreateAccountSuccess {
    pub async fn message(&self) -> String {
        "Account created".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct AccountFieldNonUnique {
    field: String,
}

#[ComplexObject]
impl AccountFieldNonUnique {
    pub async fn message(&self) -> String {
        format!("Non-unique account field '{}'", self.field)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct CreateAccountEmailInvalid {
    message: String,
}

impl Default for CreateAccountEmailInvalid {
    fn default() -> Self {
        Self {
            message: "Invalid email".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ModifyPasswordResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ModifyPasswordResult {
    Success(ModifyPasswordSuccess),
    AccountNotFound(AccountNotFound),
    InvalidPassword(ModifyPasswordInvalidPassword),
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

#[derive(SimpleObject)]
pub struct AccountNotFound {
    pub message: String,
}

impl Default for AccountNotFound {
    fn default() -> Self {
        Self {
            message: "Account not found".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct ModifyPasswordInvalidPassword {
    pub reason: String,
}

#[ComplexObject]
impl ModifyPasswordInvalidPassword {
    pub async fn message(&self) -> &String {
        &self.reason
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DeleteAccountResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteAccountResult {
    Success(DeleteAccountSuccess),
    AccountNotFound(AccountNotFound),
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
