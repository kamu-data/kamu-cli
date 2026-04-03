// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::{AdminResourcesMut, AdminSearchMut};
use crate::prelude::*;
use crate::{AdminGuard, utils};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AdminMut;

#[Object]
impl AdminMut {
    #[allow(clippy::unused_async)]
    #[graphql(guard = "AdminGuard::new()")]
    async fn search(&self) -> AdminSearchMut {
        AdminSearchMut
    }

    #[graphql(guard = "AdminGuard::new()")]
    async fn resources(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'_>,
    ) -> Result<AdminResourcesMut> {
        Ok(AdminResourcesMut::from_account(
            utils::get_account_by_id(ctx, &account_id).await?,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
