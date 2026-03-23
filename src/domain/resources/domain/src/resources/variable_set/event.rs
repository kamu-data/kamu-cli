// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ProjectionEvent;
use serde::{Deserialize, Serialize};

use crate::{
    ReconcilableResourceEvent,
    ResourceID,
    VariableSetReconcileSuccess,
    VariableSetSpec,
    VariableSetStats,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetEvent = ReconcilableResourceEvent<
    VariableSetSpec,
    VariableSetReconcileSuccess,
    VariableSetFailureDetails,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetFailureDetails {
    pub stats: VariableSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<ResourceID> for VariableSetEvent {
    fn matches_query(&self, query: &ResourceID) -> bool {
        self.resource_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
