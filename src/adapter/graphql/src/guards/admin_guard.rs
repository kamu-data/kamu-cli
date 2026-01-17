// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{Context, Guard, Result};
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac::{RebacService, RebacServiceExt};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const STAFF_ONLY_MESSAGE: &str = "Access restricted to administrators only";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AdminGuard;

impl AdminGuard {
    pub fn new() -> Self {
        Self {}
    }
}

impl Guard for AdminGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let (current_account_subject, rebac_service) =
            from_catalog_n!(ctx, CurrentAccountSubject, dyn RebacService);

        match current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(a) => {
                if rebac_service
                    .is_account_admin(&a.account_id)
                    .await
                    .int_err()?
                {
                    return Ok(());
                }
                Err(async_graphql::Error::new(STAFF_ONLY_MESSAGE))
            }
            _ => Err(async_graphql::Error::new(STAFF_ONLY_MESSAGE)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
