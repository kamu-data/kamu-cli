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

pub const CAN_PROVISION_ACCOUNTS_GUARD_MESSAGE: &str =
    "Account is not authorized to provision accounts";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CanProvisionAccountsGuard;

impl Guard for CanProvisionAccountsGuard {
    async fn check(&self, ctx: &Context<'_>) -> Result<()> {
        let (current_account_subject, rebac_service) =
            from_catalog_n!(ctx, CurrentAccountSubject, dyn RebacService);

        let Some(logged_account_id) = current_account_subject.get_maybe_logged_account_id() else {
            unreachable!(
                r#"Most likely you are mistaken this guard should be used together with LoggedInGuard: #[graphql(guard = "LoggedInGuard.and(CanProvisionAccountsGuard)")]"#
            )
        };

        let can_provision_accounts = rebac_service
            .can_provision_accounts(logged_account_id)
            .await
            .int_err()?;

        if can_provision_accounts {
            Ok(())
        } else {
            Err(Error::new(CAN_PROVISION_ACCOUNTS_GUARD_MESSAGE))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
