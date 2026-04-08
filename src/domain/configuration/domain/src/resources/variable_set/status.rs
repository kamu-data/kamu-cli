// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    PendingStatusFromSpec,
    ReconcilableStatusProjector,
    ResourceStatus,
    ResourceStatusLike,
};
use serde::{Deserialize, Serialize};

use crate::{VariableSetFailureDetails, VariableSetReconcileSuccess, VariableSetSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VariableSetStatus {
    #[serde(flatten)]
    pub resource_status: ResourceStatus,

    pub stats: VariableSetStats,
}

impl VariableSetStatus {
    pub fn new_pending(stats: VariableSetStats) -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
            stats,
        }
    }
}

impl ResourceStatusLike for VariableSetStatus {
    fn resource_status(&self) -> &ResourceStatus {
        &self.resource_status
    }

    fn resource_status_mut(&mut self) -> &mut ResourceStatus {
        &mut self.resource_status
    }
}

impl PendingStatusFromSpec<VariableSetSpec> for VariableSetStatus {
    fn pending_from_spec(spec: &VariableSetSpec) -> Self {
        Self::new_pending(VariableSetStats::pending_from_spec(spec))
    }

    fn reset_pending_from_spec(&mut self, spec: &VariableSetSpec) {
        self.stats = VariableSetStats::pending_from_spec(spec);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetStatusProjector;

impl
    ReconcilableStatusProjector<
        VariableSetSpec,
        VariableSetReconcileSuccess,
        VariableSetFailureDetails,
    > for VariableSetStatusProjector
{
    type Status = VariableSetStatus;

    fn on_reconciliation_succeeded(
        status: &mut Self::Status,
        success: VariableSetReconcileSuccess,
    ) {
        status.stats = success.stats;
    }

    fn on_reconciliation_failed(status: &mut Self::Status, details: VariableSetFailureDetails) {
        status.stats = details.stats;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct VariableSetStats {
    pub total_variables: usize,
    pub valid_variables: usize,
    pub invalid_variables: usize,
}

impl VariableSetStats {
    pub fn pending_from_spec(spec: &VariableSetSpec) -> Self {
        let total = spec.variables.len();
        Self {
            total_variables: total,
            valid_variables: 0,
            invalid_variables: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
