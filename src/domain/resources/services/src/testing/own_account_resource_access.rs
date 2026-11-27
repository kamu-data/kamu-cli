// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ResourceAccountAccessPort, ResourceOperationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A stand-in for the accounts adapter without administrators: the node may
/// use any account, a user their own
#[dill::component]
#[dill::interface(dyn ResourceAccountAccessPort)]
pub(crate) struct OwnAccountResourceAccess;

#[async_trait::async_trait]
impl ResourceAccountAccessPort for OwnAccountResourceAccess {
    async fn check_can_use_account(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
    ) -> Result<(), ResourceOperationError> {
        if principal.is_node() || principal.user_account_did().as_ref() == Some(&account.did) {
            Ok(())
        } else {
            Err(odf::AccessError::Unauthorized(
                format!("Not allowed to use resources of account '{}'", account.name).into(),
            )
            .into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
