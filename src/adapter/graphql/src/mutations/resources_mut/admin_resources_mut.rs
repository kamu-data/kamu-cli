// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::ResourceDeleteResult;
use crate::prelude::*;
use crate::queries::ResourceSelectorInput;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AdminResourcesMut {
    account: kamu_accounts::Account,
}

impl AdminResourcesMut {
    pub fn from_account(account: kamu_accounts::Account) -> Self {
        Self { account }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl AdminResourcesMut {
    #[tracing::instrument(level = "info", name = AdminResourcesMut_delete, skip_all)]
    async fn delete(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<ResourceDeleteResult> {
        super::helpers::delete_resource(
            ctx,
            selector,
            Some(kamu_resources::ResourceManifestAccount {
                id: Some(self.account.id.clone()),
                name: None,
            }),
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
