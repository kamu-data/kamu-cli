// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;
use kamu_accounts::{Account, AccountRepository, UpdateAccountError};

use super::AccountFlowsMut;
use crate::prelude::*;

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
            return Ok(UpdateEmailResult::InvalidEmail(UpdateEmailInvalid {
                dummy: false,
            }));
        };

        let account_repo = from_catalog_n!(ctx, dyn AccountRepository);
        match account_repo
            .update_account_email(&self.account.id, new_email.clone())
            .await
        {
            Ok(_) => Ok(UpdateEmailResult::Success(UpdateEmailSuccess {
                new_email: new_email.as_ref().to_string(),
            })),

            Err(UpdateAccountError::Duplicate(_)) => {
                Ok(UpdateEmailResult::NonUniqueEmail(UpdateEmailNonUnique {
                    dummy: false,
                }))
            }

            Err(UpdateAccountError::NotFound(e)) => Err(e.int_err().into()),
            Err(UpdateAccountError::Internal(e)) => Err(e.into()),
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
#[graphql(complex)]
pub struct UpdateEmailInvalid {
    pub dummy: bool,
}

#[ComplexObject]
impl UpdateEmailInvalid {
    pub async fn message(&self) -> String {
        "Invalid email".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct UpdateEmailNonUnique {
    pub dummy: bool,
}

#[ComplexObject]
impl UpdateEmailNonUnique {
    pub async fn message(&self) -> String {
        "Non-unique email".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
