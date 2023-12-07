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

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SystemFlowEventStore: EventStore<SystemFlowState> {
    /// Generates new unique flow identifier
    fn new_flow_id(&self) -> SystemFlowID;

    /// Returns the last flow of certain type
    fn get_last_specific_flow(&self, flow_type: SystemFlowType) -> Option<SystemFlowID>;

    /// Returns the flows of certain type in reverse chronological order based
    /// on creation time
    fn get_specific_flows<'a>(&'a self, flow_type: SystemFlowType) -> SystemFlowIDStream<'a>;
}

/////////////////////////////////////////////////////////////////////////////////////////
