// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ReconcilableResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait Reconciler<R: ReconcilableResource>: Send + Sync {
    async fn reconcile(&self, resource: &R) -> Result<R::ReconcileSuccess, R::ReconcicleError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceReconcileError: std::error::Error + Send + Sync {
    fn reason_code(&self) -> &'static str;
    fn user_message(&self) -> String;
    fn is_transient(&self) -> bool;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
