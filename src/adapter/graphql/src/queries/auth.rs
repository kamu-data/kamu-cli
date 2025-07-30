// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Auth;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Auth {
    #[allow(clippy::unused_async)]
    #[tracing::instrument(level = "info", name = Auth_enabled_providers, skip_all)]
    async fn enabled_providers(&self, ctx: &Context<'_>) -> Result<Vec<AccountProvider>> {
        use std::str::FromStr;

        let authentication_service = from_catalog_n!(ctx, dyn kamu_accounts::AuthenticationService);

        let providers = authentication_service
            .supported_login_methods()
            .into_iter()
            .map(kamu_accounts::AccountProvider::from_str)
            .map(|res| res.map(Into::into))
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        Ok(providers)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
