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
pub trait FlowConfigurationEventStore: EventStore<FlowConfigurationState> {
    /// Returns all existing flow bindings, where configurations exist
    fn stream_all_existing_flow_bindings(&self) -> FlowBindingStream<'_>;

    /// Returns all bindings for a given scope where configs exist
    async fn all_bindings_for_scope(
        &self,
        scope: &FlowScope,
    ) -> Result<Vec<FlowBinding>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
