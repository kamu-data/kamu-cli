// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotasMut<'a> {
    #[expect(dead_code)]
    account: &'a Account,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> AccountQuotasMut<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// Setting quotas at the user level.
    #[tracing::instrument(level = "info", name = AccountQuotasMut_set_user_level_quotas, skip_all)]
    pub async fn set_user_level_quotas(
        &self,
        quotas: SetUserLevelQuotasInput,
    ) -> Result<SetUserLevelQuotasResult> {
        let _ = quotas;
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct SetUserLevelQuotasInput {
    pub storage: Option<SetUserLevelQuotasStorageInput>,
}

#[derive(InputObject, Debug)]
pub struct SetUserLevelQuotasStorageInput {
    pub limit_total_bytes: Option<u64>,
}

#[derive(SimpleObject, Debug)]
pub struct SetUserLevelQuotasResult {
    dummy: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
