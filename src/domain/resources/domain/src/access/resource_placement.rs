// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ApplyResourceRejection, DeclarativeResource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decides which account owns a resource, from its prepared spec, such as the
/// owner of a dataset the spec targets.
///
/// Runs on every apply, after [`ResourceSpecPreApply`](crate::ResourceSpecPreApply). A manifest
/// naming another account than the placement is rejected. A manifest naming none is applied in
/// the placed account, so the current spec is looked up there and the spec is prepared again.
/// Placing that spec again gives the same account.
#[async_trait::async_trait]
pub trait ResourcePlacement<R: DeclarativeResource>: Send + Sync {
    async fn place(
        &self,
        principal: &odf::metadata::auth::PrincipalHandle,
        spec: &R::SpecInput,
    ) -> Result<ResourcePlacementOutcome, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ResourcePlacementOutcome {
    /// The account the manifest names, or the principal's own
    Default,
    Account(odf::ResourceOwnerHandle),
    /// For a spec that cannot be placed, such as one targeting a missing object
    Rejected(ApplyResourceRejection),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
