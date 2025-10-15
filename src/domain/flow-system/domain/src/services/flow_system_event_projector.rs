// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::FlowSystemEvent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSystemEventProjector: Send + Sync {
    /// Stable name; used as key in the `flow_system_projected_events` ledger.
    fn name(&self) -> &'static str;

    /// Apply a *single* event using the open transaction.
    /// Must be idempotent: safe to re-run for the same event id.
    async fn apply(&self, e: &FlowSystemEvent) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
