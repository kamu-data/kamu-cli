// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;

use crate::prelude::*;
use crate::queries::ViewAccessToken;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountAccessTokens<'a> {
    account_id: &'a AccountID<'a>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> AccountAccessTokens<'a> {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account_id: &'a AccountID) -> Self {
        Self { account_id }
    }

    #[tracing::instrument(level = "info", name = AccountAccessTokens_list_access_tokens, skip_all)]
    async fn list_access_tokens(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AccessTokenConnection> {
        let access_token_service = from_catalog_n!(ctx, dyn kamu_accounts::AccessTokenService);

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let access_token_listing = access_token_service
            .get_access_tokens_by_account_id(
                self.account_id,
                &PaginationOpts::from_page(page, per_page),
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
            page,
            per_page,
            access_token_listing.total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(ViewAccessToken, AccessTokenConnection, AccessTokenEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
