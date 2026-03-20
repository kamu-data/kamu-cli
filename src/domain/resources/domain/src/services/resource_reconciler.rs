// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{DeclarativeResource, ResourceStatusLike};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait Reconciler<R>: Send + Sync {
    type Success;
    type Error: ReconcileError;

    async fn reconcile(&self, resource: &R) -> Result<Self::Success, Self::Error>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn needs_reconciliation<R>(resource: &R) -> bool
where
    R: DeclarativeResource,
    R::Status: ResourceStatusLike,
{
    resource.status().resource_status().observed_generation < resource.metadata().generation
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcileError: std::error::Error {
    fn reason_code(&self) -> &'static str;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
