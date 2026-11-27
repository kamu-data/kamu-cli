// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_auth_rebac::{RebacService, RebacServiceExt};
use kamu_resources::{ResourceAccountAccessPort, ResourceOperationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The node, the account itself, or a platform administrator may use the
/// resources of an account
#[dill::component(pub)]
#[dill::interface(dyn ResourceAccountAccessPort)]
pub struct ResourceAccountAccessAdapter {
    rebac_service: Arc<dyn RebacService>,
}

#[async_trait::async_trait]
impl ResourceAccountAccessPort for ResourceAccountAccessAdapter {
    async fn check_can_use_account(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
    ) -> Result<(), ResourceOperationError> {
        if principal.is_node() {
            return Ok(());
        }

        let Some(principal_did) = principal.user_account_did() else {
            return Err(denied(account));
        };
        if principal_did == account.did {
            return Ok(());
        }

        let is_admin = self
            .rebac_service
            .is_account_admin(&principal_did)
            .await
            .int_err()?;
        if is_admin {
            Ok(())
        } else {
            Err(denied(account))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn denied(account: &odf::ResourceOwnerHandle) -> ResourceOperationError {
    odf::AccessError::Unauthorized(
        format!(
            "Current subject is not allowed to use resources of account '{}'",
            account.name
        )
        .into(),
    )
    .into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
