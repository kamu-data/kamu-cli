// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::ResolvedDatasetsMap;
use thiserror::Error;

use super::{TransformPlanPayload, TransformPreliminaryPlan};
use crate::engine::TransformRequestExt;
use crate::{InputNotPulledError, InvalidInputIntervalError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait TransformElaborationService: Send + Sync {
    /// Reads the inputs at their current heads and builds the transform
    /// request for the current time. Writes nothing.
    ///
    /// Run it in the transaction that built `plan`, so that the output and
    /// input heads come from one snapshot of the database, then detach the
    /// datasets of the result before executing it.
    async fn elaborate_transform(
        &self,
        plan: TransformPreliminaryPlan,
    ) -> Result<TransformElaboration, TransformElaborateError>;

    /// Rebuilds the request of a stored plan at its pinned heads and system
    /// time. `plan` must be built at `stored.expected_head`.
    async fn rebuild_transform(
        &self,
        plan: TransformPreliminaryPlan,
        stored: &TransformPlanPayload,
    ) -> Result<TransformPlan, TransformRebuildError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TransformElaboration {
    Elaborated(TransformPlan),
    UpToDate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformPlan {
    pub request: TransformRequestExt,
    pub datasets_map: ResolvedDatasetsMap,
}

impl std::fmt::Debug for TransformPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.request.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformElaborateError {
    #[error(transparent)]
    InputNotPulled(
        #[from]
        #[backtrace]
        InputNotPulledError,
    ),
    #[error(transparent)]
    InvalidInputInterval(
        #[from]
        #[backtrace]
        InvalidInputIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformRebuildError {
    #[error(transparent)]
    Mismatch(
        #[from]
        #[backtrace]
        TransformPlanMismatchError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/// The request rebuilt from a stored plan differs from that plan. Metadata at
/// pinned heads is immutable, so this indicates a defect; the stored plan
/// cannot be trusted.
#[derive(Debug, Error)]
#[error("Transform of {dataset_id} rebuilt from its stored plan differs from that plan: {reason}")]
pub struct TransformPlanMismatchError {
    pub dataset_id: odf::DatasetID,
    pub reason: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
