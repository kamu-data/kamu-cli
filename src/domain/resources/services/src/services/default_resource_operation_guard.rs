// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_resources::{
    DeclarativeResource,
    ResourceAccountAccess,
    ResourceAccountAccessPort,
    ResourceAccountOperation,
    ResourceOperationError,
    ResourceOperationGuard,
    ResourceOperationRequest,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The guard of types without one of their own, and the one a type's guard
/// may delegate to: every operation is allowed to a principal who may use the
/// account ([`ResourceAccountAccessPort`])
#[dill::component(pub)]
pub struct DefaultResourceOperationGuard {
    account_access_port: Arc<dyn ResourceAccountAccessPort>,
}

impl DefaultResourceOperationGuard {
    pub async fn check_account_rule(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
    ) -> Result<(), ResourceOperationError> {
        self.account_access_port
            .check_can_use_account(principal, account)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<R: DeclarativeResource> ResourceOperationGuard<R> for DefaultResourceOperationGuard {
    async fn check_account(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
        _operation: ResourceAccountOperation,
    ) -> Result<ResourceAccountAccess, ResourceOperationError> {
        self.check_account_rule(principal, account).await?;
        Ok(ResourceAccountAccess::Allowed)
    }

    async fn check_operation(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        request: &ResourceOperationRequest<'_, R>,
    ) -> Result<(), ResourceOperationError> {
        self.check_account_rule(principal, request.account).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
