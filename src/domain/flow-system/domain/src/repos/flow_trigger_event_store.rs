// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventStore;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowTriggerEventStore: EventStore<FlowTriggerState> {
    /// Returns all existing flow bindings, where triggers are active
    fn stream_all_active_flow_bindings(&self) -> FlowBindingStream<'_>;

    /// Returns all bindings for a given scope where triggers are defined
    /// regardless of status
    async fn all_trigger_bindings_for_scope(
        &self,
        scope: &FlowScope,
    ) -> Result<Vec<FlowBinding>, InternalError>;

    /// Checks if there are any active triggers for the given list of scopes
    async fn has_active_triggers_for_scopes(
        &self,
        scopes: &[FlowScope],
    ) -> Result<bool, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
