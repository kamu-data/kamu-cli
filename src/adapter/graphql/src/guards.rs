// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{Context, Guard, Result};

use crate::prelude::from_catalog;
use crate::utils::extract_access_token;

////////////////////////////////////////////////////////////////////////////////////////

pub struct LoggedInGuard {}

impl LoggedInGuard {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Guard for LoggedInGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let maybe_access_token = extract_access_token(ctx);
        if let Some(access_token) = maybe_access_token {
            let authentication_service =
                from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

            match authentication_service
                .get_account_info(access_token.token)
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => Err(async_graphql::Error::new(e.to_string())),
            }
        } else {
            Err(async_graphql::Error::new("Anonymous access forbidden"))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////
