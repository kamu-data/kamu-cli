// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{Context, ErrorExtensions};
use kamu_accounts::{AuthenticationService, CurrentAccountSubject};
use opendatafabric::AccountName as OdfAccountName;

use crate::mutations::AccountMut;
use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

pub struct AccountsMut;

///////////////////////////////////////////////////////////////////////////////

#[Object]
impl AccountsMut {
    /// Returns a mutable account by its id
    async fn by_id(&self, ctx: &Context<'_>, account_id: AccountID) -> Result<Option<AccountMut>> {
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();
        let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();

        if let CurrentAccountSubject::Logged(logged_account) = current_account_subject.as_ref() {
            if logged_account.account_id == account_id.clone().into() {
                let account_maybe = authentication_service.account_by_id(&account_id).await?;
                return Ok(account_maybe.map(AccountMut::new));
            }
            return Err(GqlError::Gql(
                async_graphql::Error::new("Account datasets access error")
                    .extend_with(|_, eev| eev.set("account_id", account_id.to_string())),
            ));
        }
        Ok(None)
    }

    /// Returns a mutable account by its name
    async fn by_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName,
    ) -> Result<Option<AccountMut>> {
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();
        let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();

        if let CurrentAccountSubject::Logged(logged_account) = current_account_subject.as_ref() {
            if logged_account.account_name == OdfAccountName::from(account_name.clone()) {
                let account_maybe = authentication_service
                    .account_by_name(&account_name)
                    .await?;
                return Ok(account_maybe.map(AccountMut::new));
            }
            return Err(GqlError::Gql(
                async_graphql::Error::new("Account datasets access error")
                    .extend_with(|_, eev| eev.set("account_name", account_name.to_string())),
            ));
        }
        Ok(None)
    }
}
