// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::AdminResources;
use crate::{AdminGuard, utils};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Admin;

#[Object]
impl Admin {
    #[allow(clippy::unused_async)]
    #[graphql(guard = "AdminGuard::new()")]
    async fn self_test(&self) -> Result<String> {
        Ok("OK".to_string())
    }

    #[graphql(guard = "AdminGuard::new()")]
    async fn resources(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'_>,
    ) -> Result<AdminResources> {
        Ok(AdminResources::from_account(
            utils::get_account_by_id(ctx, &account_id).await?,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
