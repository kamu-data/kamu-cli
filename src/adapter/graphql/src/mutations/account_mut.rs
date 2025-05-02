// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use email_utils::Email;
use kamu_accounts::{
    Account,
    AccountRepository,
    AccountService,
    CreateAccountError,
    ModifyPasswordError,
    UpdateAccountError,
};
use random_strings::AllowedSymbols;

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
        new_email: String,
    ) -> Result<UpdateEmailResult> {
        let Ok(new_email) = Email::parse(&new_email) else {
            return Ok(UpdateEmailResult::InvalidEmail(
                UpdateEmailInvalid::default(),
            ));
        };

        let account_repo = from_catalog_n!(ctx, dyn AccountRepository);
        match account_repo
            .update_account_email(&self.account.id, new_email.clone())
            .await
        {
            Ok(_) => Ok(UpdateEmailResult::Success(UpdateEmailSuccess {
                new_email: new_email.as_ref().to_string(),
            })),
            Err(UpdateAccountError::Duplicate(_)) => Ok(UpdateEmailResult::NonUniqueEmail(
                UpdateEmailNonUnique::default(),
            )),
            Err(UpdateAccountError::NotFound(e)) => Err(e.int_err().into()),
            Err(UpdateAccountError::Internal(e)) => Err(e.into()),
        }
    }

    // Create a new account
    async fn create_account(
        &self,
        ctx: &Context<'_>,
        account_name: String,
        email: Option<String>,
    ) -> Result<CreateAccountResult> {
        ensure_account_can_provision_accounts(ctx, &self.account.id).await?;

        let Ok(account_name) = odf::AccountName::from_str(&account_name) else {
            return Ok(CreateAccountResult::InvalidAccountName(
                AccountNameInvalid::default(),
            ));
        };

        let new_email = email.unwrap_or(
            if let Some(parent_account_parts) = self.account.email.to_string().split_once('@') {
                format!(
                    "{}_{}@{}",
                    parent_account_parts.0, account_name, parent_account_parts.1
                )
            } else {
                format!("{account_name}@example.com",)
            },
        );

        let Ok(new_email) = Email::parse(&new_email) else {
            return Ok(CreateAccountResult::InvalidEmail(
                CreateAccountEmailInvalid::default(),
            ));
        };

        let random_password =
            random_strings::get_random_string(None, 10, &AllowedSymbols::AsciiSymbols);

        let account_service = from_catalog_n!(ctx, dyn AccountService);
        match account_service
            .create_account(&account_name, new_email, random_password, &self.account.id)
            .await
        {
            Ok(created_account) => Ok(CreateAccountResult::Success(CreateAccountSuccess {
                account: AccountView::from_account(created_account),
            })),
            // TODO for some reason duplicate email returns duplicate id
            Err(CreateAccountError::Duplicate(err)) => Ok(
                CreateAccountResult::NonUniqueAccountField(AccountFieldNonUnique {
                    field: err.account_field.to_string(),
                }),
            ),
            Err(CreateAccountError::Internal(e)) => Err(e.into()),
        }
    }

    // Reset password for selected account
    #[graphql(guard = "AdminGuard::new()")]
    async fn modify_password(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
        password: String,
    ) -> Result<ModifyPasswordResult> {
        let account_service = from_catalog_n!(ctx, dyn AccountService);
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
            Err(ModifyPasswordError::Internal(e)) => Err(e.into()),
        }
    }

    /// Access to the mutable flow configurations of this account
    async fn flows(&self) -> AccountFlowsMut {
        AccountFlowsMut::new(self.account.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UpdateEmailResult {
    Success(UpdateEmailSuccess),
    InvalidEmail(UpdateEmailInvalid),
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
pub struct UpdateEmailInvalid {
    message: String,
}

impl Default for UpdateEmailInvalid {
    fn default() -> Self {
        Self {
            message: "Invalid email".to_string(),
        }
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

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateAccountResult {
    Success(CreateAccountSuccess),
    InvalidAccountName(AccountNameInvalid),
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

#[derive(SimpleObject, Debug)]
pub struct AccountNameInvalid {
    message: String,
}

impl Default for AccountNameInvalid {
    fn default() -> Self {
        Self {
            message: "Invalid account name".to_string(),
        }
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

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ModifyPasswordResult {
    Success(ModifyPasswordSuccess),
    AccountNotFound(AccountNotFound),
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
