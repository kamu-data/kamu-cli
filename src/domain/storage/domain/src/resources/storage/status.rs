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

use crate::{StorageFailureDetails, StorageReconcileSuccess, StorageSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatus {
    #[serde(flatten)]
    pub resource_status: ResourceStatus,
    pub provider_kind: StorageProviderKind,
    pub references: StorageReferenceStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageStatus {
    pub fn new_pending(provider_kind: StorageProviderKind) -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
            provider_kind,
            references: StorageReferenceStatus::default(),
        }
    }
}

impl ResourceStatusLike for StorageStatus {
    fn resource_status(&self) -> &ResourceStatus {
        &self.resource_status
    }

    fn resource_status_mut(&mut self) -> &mut ResourceStatus {
        &mut self.resource_status
    }
}

impl PendingStatusFromSpec<StorageSpec> for StorageStatus {
    fn pending_from_spec(spec: &StorageSpec) -> Self {
        Self::new_pending(spec.provider.kind())
    }

    fn reset_pending_from_spec(&mut self, spec: &StorageSpec) {
        self.provider_kind = spec.provider.kind();
        self.references = StorageReferenceStatus::default();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StorageStatusProjector;

impl ReconcilableStatusProjector<StorageSpec, StorageReconcileSuccess, StorageFailureDetails>
    for StorageStatusProjector
{
    type Status = StorageStatus;

    fn on_reconciliation_succeeded(status: &mut Self::Status, success: StorageReconcileSuccess) {
        status.provider_kind = success.provider_kind;
        status.references = success.references;
    }

    fn on_reconciliation_failed(status: &mut Self::Status, details: StorageFailureDetails) {
        status.references = details.references;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageProviderKind {
    LocalFs,
    S3,
    Ipfs,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageReferenceStatus {
    pub total_references: usize,
    pub resolved_references: usize,
    pub unresolved_references: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
