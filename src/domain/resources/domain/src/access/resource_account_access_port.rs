// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceOperationError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Whether a principal may use the resources of an account: the rule for
/// types without a `ResourceOperationGuard` of their own, and the one a guard
/// may delegate to.
///
/// The context that owns accounts implements it, so the framework does not
/// learn what makes an account usable. A denial is
/// [`ResourceOperationError::Access`].
#[async_trait::async_trait]
pub trait ResourceAccountAccessPort: Send + Sync {
    async fn check_can_use_account(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        account: &odf::ResourceOwnerHandle,
    ) -> Result<(), ResourceOperationError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
