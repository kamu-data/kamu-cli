// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_accounts::CurrentAccountSubject;

use crate::{DatasetPermission, DatasetPermissionError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks whether a principal, given explicitly, may run an operation on a
/// dataset.
///
/// Owns the rule mapping each operation to the permissions it needs, on top of
/// [`crate::DatasetPermissionChecker`], which checks one permission at a time:
///
/// | Operation | Target | Inputs |
/// | --- | --- | --- |
/// | `Transform` | `Write` | `Read` |
/// | `PollingIngest`, `PushIngest`, `SyncFrom`, `SetWatermark` | `Write` | none |
/// | `SyncTo` | `Read` | none |
/// | `Compaction`, `ResetToMetadata`, `Reset` | `Maintain` | none |
/// | `Verify` | `Read` | `Read` on the replayed inputs |
///
/// Roles nest (`Maintainer` includes `Editor`, which includes `Reader`), so
/// `Maintain` implies `Write`.
///
/// A push (`SyncTo`) also saves a push alias on the local dataset; that needs
/// no more than `Read`.
///
/// For each request, the target's error comes first, then the first failing
/// input in input order. Errors name the dataset that failed.
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetOperationGuard: Send + Sync {
    async fn check_operation(
        &self,
        principal: &CurrentAccountSubject,
        request: &DatasetOperationRequest,
    ) -> Result<(), DatasetPermissionError>;

    /// One result per request, in request order
    async fn check_operations(
        &self,
        principal: &CurrentAccountSubject,
        requests: &[DatasetOperationRequest],
    ) -> Result<Vec<Result<(), DatasetPermissionError>>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetOperationRequest {
    pub target: odf::DatasetHandle,
    pub operation: DatasetOperation,
}

impl DatasetOperationRequest {
    pub fn new(target: odf::DatasetHandle, operation: DatasetOperation) -> Self {
        Self { target, operation }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An operation on a target dataset. Only the operations that read other
/// datasets carry them.
#[derive(Debug, Clone)]
pub enum DatasetOperation {
    Transform {
        inputs: Vec<odf::DatasetHandle>,
    },
    PollingIngest,
    PushIngest,
    SyncFrom,
    SyncTo,
    Compaction,
    ResetToMetadata,
    Reset,
    SetWatermark,
    Verify {
        replayed_inputs: Vec<odf::DatasetHandle>,
    },
}

impl DatasetOperation {
    pub fn target_permission(&self) -> DatasetPermission {
        match self {
            Self::Transform { .. }
            | Self::PollingIngest
            | Self::PushIngest
            | Self::SyncFrom
            | Self::SetWatermark => DatasetPermission::Write,
            Self::SyncTo | Self::Verify { .. } => DatasetPermission::Read,
            Self::Compaction | Self::ResetToMetadata | Self::Reset => DatasetPermission::Maintain,
        }
    }

    /// Datasets the operation reads, each needing [`DatasetPermission::Read`]
    pub fn read_inputs(&self) -> &[odf::DatasetHandle] {
        match self {
            Self::Transform { inputs } => inputs,
            Self::Verify { replayed_inputs } => replayed_inputs,
            Self::PollingIngest
            | Self::PushIngest
            | Self::SyncFrom
            | Self::SyncTo
            | Self::Compaction
            | Self::ResetToMetadata
            | Self::Reset
            | Self::SetWatermark => &[],
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
