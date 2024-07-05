// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::AccessTokenPaginationOpts;

use super::ViewAccessToken;
use crate::prelude::*;
use crate::utils::check_logged_account_id_match;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Auth;

#[Object]
impl Auth {
    const DEFAULT_PER_PAGE: i64 = 15;

    #[allow(clippy::unused_async)]
    async fn enabled_login_methods(&self, ctx: &Context<'_>) -> Result<Vec<&'static str>> {
        let authentication_service =
            from_catalog::<dyn kamu_accounts::AuthenticationService>(ctx).unwrap();

        Ok(authentication_service.supported_login_methods())
    }

    async fn list_access_tokens(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID,
        page: Option<i64>,
        per_page: Option<i64>,
    ) -> Result<AccessTokenConnection> {
        check_logged_account_id_match(ctx, &account_id)?;

        let access_token_service =
            from_catalog::<dyn kamu_accounts::AccessTokenService>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let access_token_listing = access_token_service
            .get_access_tokens_by_account_id(
                &account_id,
                &AccessTokenPaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                },
            )
            .await
            .int_err()?;

        let access_tokens: Vec<_> = access_token_listing
            .list
            .into_iter()
            .map(ViewAccessToken::new)
            .collect();

        Ok(AccessTokenConnection::new(
            access_tokens,
            usize::try_from(page).unwrap(),
            usize::try_from(per_page).unwrap(),
            access_token_listing.total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(ViewAccessToken, AccessTokenConnection, AccessTokenEdge);
