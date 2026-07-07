// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{ReconcilableStatusProjector, ResourceStatus};
use serde::{Deserialize, Serialize};
use strum::Display;

use crate::{StorageFailureDetails, StorageReconcileSuccess, StorageSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type StorageStatus = ResourceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StorageStatusProjector;

impl ReconcilableStatusProjector<StorageSpec, StorageReconcileSuccess, StorageFailureDetails>
    for StorageStatusProjector
{
    type Status = StorageStatus;

    fn on_reconciliation_succeeded(_status: &mut Self::Status, _success: StorageReconcileSuccess) {}

    fn on_reconciliation_failed(_status: &mut Self::Status, _details: StorageFailureDetails) {}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[strum(serialize_all = "camelCase")]
pub enum StorageProviderKind {
    LocalFs,
    S3,
    Ipfs,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
