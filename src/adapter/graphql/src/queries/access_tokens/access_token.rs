// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_accounts::{AccessToken as ViewKamuAccessToken, KamuAccessToken};

use crate::prelude::*;
use crate::queries::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ViewAccessToken {
    token: ViewKamuAccessToken,
}

#[Object]
impl ViewAccessToken {
    #[graphql(skip)]
    pub fn new(token: ViewKamuAccessToken) -> Self {
        Self { token }
    }

    /// Unique identifier of the access token
    async fn id(&self) -> AccessTokenID {
        (&self.token.id).into()
    }

    /// Name of the access token
    async fn name(&self) -> &String {
        &self.token.token_name
    }

    /// Date of token creation
    async fn created_at(&self) -> DateTime<Utc> {
        self.token.created_at
    }

    /// Date of token revocation
    async fn revoked_at(&self) -> Option<DateTime<Utc>> {
        self.token.revoked_at
    }

    /// Access token account owner
    async fn account(&self, ctx: &Context<'_>) -> Result<Account> {
        let account = Account::from_account_id(ctx, self.token.account_id.clone()).await?;

        Ok(account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CreatedAccessToken {
    token: KamuAccessToken,
    account_id: AccountID<'static>,
    token_name: String,
}

#[Object]
impl CreatedAccessToken {
    #[graphql(skip)]
    pub fn new(token: KamuAccessToken, account_id: AccountID<'static>, token_name: &str) -> Self {
        Self {
            token,
            account_id,
            token_name: token_name.to_string(),
        }
    }

    /// Unique identifier of the access token
    async fn id(&self) -> AccessTokenID {
        self.token.id.into()
    }

    /// Name of the access token
    async fn name(&self) -> &String {
        &self.token_name
    }

    /// Composed original token
    async fn composed(&self) -> &String {
        &self.token.composed_token
    }

    /// Access token account owner
    async fn account(&self, ctx: &Context<'_>) -> Result<Account> {
        let account = Account::from_account_id(ctx, self.account_id.clone().into()).await?;

        Ok(account)
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateAccessTokenResultSuccess {
    pub token: CreatedAccessToken,
}

#[ComplexObject]
impl CreateAccessTokenResultSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}
