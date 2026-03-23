// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{
    PendingStatusFromSpec,
    ReconcilableStatusProjector,
    ResourceStatus,
    ResourceStatusLike,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretSetStatus {
    #[serde(flatten)]
    pub resource_status: ResourceStatus,

    pub stats: SecretSetStats,
}

impl SecretSetStatus {
    pub fn new_pending(stats: SecretSetStats) -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
            stats,
        }
    }
}

impl ResourceStatusLike for SecretSetStatus {
    fn resource_status(&self) -> &ResourceStatus {
        &self.resource_status
    }

    fn resource_status_mut(&mut self) -> &mut ResourceStatus {
        &mut self.resource_status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PendingStatusFromSpec<crate::SecretSetSpec> for SecretSetStatus {
    fn pending_from_spec(spec: &crate::SecretSetSpec) -> Self {
        Self::new_pending(SecretSetStats::pending_from_spec(spec))
    }

    fn reset_pending_from_spec(&mut self, spec: &crate::SecretSetSpec) {
        self.stats = SecretSetStats::pending_from_spec(spec);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetStatusProjector;

impl
    ReconcilableStatusProjector<
        crate::SecretSetSpec,
        crate::SecretSetReconcileSuccess,
        crate::SecretSetFailureDetails,
    > for SecretSetStatusProjector
{
    type Status = SecretSetStatus;

    fn on_reconciliation_succeeded(
        status: &mut Self::Status,
        success: crate::SecretSetReconcileSuccess,
    ) {
        status.stats = success.stats;
    }

    fn on_reconciliation_failed(
        status: &mut Self::Status,
        details: crate::SecretSetFailureDetails,
    ) {
        status.stats = details.stats;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SecretSetStats {
    pub total_secrets: usize,
    pub valid_secrets: usize,
    pub invalid_secrets: usize,
}

impl SecretSetStats {
    pub fn pending_from_spec(spec: &crate::SecretSetSpec) -> Self {
        let total = spec.secrets.len();
        Self {
            total_secrets: total,
            valid_secrets: 0,
            invalid_secrets: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
