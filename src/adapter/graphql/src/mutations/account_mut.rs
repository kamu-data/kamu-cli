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
use crate::queries::{CreateAccessTokenResultSuccess, CreatedAccessToken};
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
        let update_account_email_use_case = from_catalog_n!(ctx, dyn UpdateAccountEmailUseCase);

        match update_account_email_use_case
            .execute(&self.account, new_email.clone().into())
            .await
        {
            Ok(_) => Ok(UpdateEmailResult::Success(UpdateEmailSuccess {
                new_email: new_email.as_ref().to_string(),
            })),
            Err(UpdateAccountEmailError::Duplicate(_)) => Ok(UpdateEmailResult::NonUniqueEmail(
                UpdateEmailNonUnique::default(),
            )),
            Err(UpdateAccountEmailError::Internal(e)) => Err(e.int_err().into()),
            Err(UpdateAccountEmailError::Access(_)) => {
                Err(GqlError::gql_extended("Account access error", |eev| {
                    eev.set("account_name", self.account.account_name.to_string());
                }))
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

    #[tracing::instrument(level = "info", name = AccountMut_create_access_token, skip_all)]
    async fn create_access_token(
        &self,
        ctx: &Context<'_>,
        token_name: String,
    ) -> Result<CreateTokenResult> {
        let access_token_service = from_catalog_n!(ctx, dyn kamu_accounts::AccessTokenService);

        match access_token_service
            .create_access_token(&token_name, &self.account.id.clone())
            .await
        {
            Ok(created_token) => Ok(CreateTokenResult::Success(Box::new(
                CreateAccessTokenResultSuccess {
                    token: CreatedAccessToken::new(
                        created_token,
                        self.account.id.clone().into(),
                        &token_name,
                    ),
                },
            ))),
            Err(err) => match err {
                CreateAccessTokenError::Duplicate(_) => Ok(CreateTokenResult::DuplicateName(
                    CreateAccessTokenResultDuplicate { token_name },
                )),
                CreateAccessTokenError::Internal(internal_err) => {
                    Err(GqlError::Internal(internal_err))
                }
            },
        }
    }

    #[tracing::instrument(level = "info", name = AccountMut_revoke_access_token, skip_all)]
    async fn revoke_access_token(
        &self,
        ctx: &Context<'_>,
        token_id: AccessTokenID<'static>,
    ) -> Result<RevokeResult> {
        utils::check_access_token_valid(ctx, &token_id).await?;

        let access_token_service = from_catalog_n!(ctx, dyn kamu_accounts::AccessTokenService);

        match access_token_service.revoke_access_token(&token_id).await {
            Ok(_) => Ok(RevokeResult::Success(RevokeResultSuccess { token_id })),
            Err(RevokeTokenError::AlreadyRevoked) => {
                Ok(RevokeResult::AlreadyRevoked(RevokeResultAlreadyRevoked {
                    token_id,
                }))
            }
            Err(RevokeTokenError::NotFound(_)) => Err(GqlError::Gql(async_graphql::Error::new(
                "Access token not found",
            ))),
            Err(RevokeTokenError::Internal(e)) => Err(e.into()),
        }
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

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RevokeResult<'a> {
    Success(RevokeResultSuccess<'a>),
    AlreadyRevoked(RevokeResultAlreadyRevoked<'a>),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct RevokeResultSuccess<'a> {
    pub token_id: AccessTokenID<'a>,
}

#[ComplexObject]
impl RevokeResultSuccess<'_> {
    async fn message(&self) -> String {
        "Access token revoked successfully".to_string()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct RevokeResultAlreadyRevoked<'a> {
    pub token_id: AccessTokenID<'a>,
}

#[ComplexObject]
impl RevokeResultAlreadyRevoked<'_> {
    async fn message(&self) -> String {
        format!("Access token with id {} already revoked", self.token_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateTokenResult<'a> {
    Success(Box<CreateAccessTokenResultSuccess<'a>>),
    DuplicateName(CreateAccessTokenResultDuplicate),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateAccessTokenResultDuplicate {
    pub token_name: String,
}

#[ComplexObject]
impl CreateAccessTokenResultDuplicate {
    pub async fn message(&self) -> String {
        format!("Access token with {} name already exists", self.token_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
