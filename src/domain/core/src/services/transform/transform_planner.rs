// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{ResolvedDataset, ResolvedDatasetsMap};

use crate::engine::TransformRequestExt;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plans transforms of derivative datasets. Every method only reads.
///
/// Inputs are resolved through the dataset registry, so call it in a
/// transaction, with `target` resolved in that transaction: the heads of the
/// target and of its inputs then come from one snapshot of the database.
/// Detach the datasets of an elaborated plan before executing it.
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait TransformPlanner: Send + Sync {
    /// Reads the transform `target` defines, without elaborating it
    async fn describe_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformDescription, TransformPlanError>;

    /// Plans the next transform of `target` at the current heads of the
    /// target and its inputs, for the current time
    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformElaboration, TransformElaborateError>;

    /// Rebuilds the request of a stored plan at the heads and the system time
    /// it names
    async fn rebuild_transform(
        &self,
        target: ResolvedDataset,
        stored: &TransformPlanPayload,
    ) -> Result<TransformExecutionPlan, TransformRebuildError>;

    /// Tells whether `target` has input data to transform, without building
    /// the request
    async fn evaluate_transform_status(
        &self,
        target: ResolvedDataset,
    ) -> Result<TransformStatus, TransformStatusError>;

    /// Plans the replay of the transforms recorded in `block_range`
    async fn build_transform_verification_plan(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<VerifyTransformOperation, VerifyTransformPlanError>;

    /// The inputs that replaying the transforms in `block_range` reads, the
    /// same ones [`Self::build_transform_verification_plan`] resolves, without
    /// reading them
    async fn get_verification_inputs(
        &self,
        target: ResolvedDataset,
        block_range: (Option<odf::Multihash>, Option<odf::Multihash>),
    ) -> Result<Vec<odf::DatasetHandle>, VerifyTransformPlanError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The transform a derivative dataset defines
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformDescription {
    /// Hash of the `SetTransform` block in effect
    pub transform_block: odf::Multihash,
    /// The query and the engine that runs it
    pub transform: odf::metadata::Transform,
    /// Input datasets, in `SetTransform` order
    pub inputs: Vec<odf::DatasetHandle>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TransformElaboration {
    Elaborated(TransformExecutionPlan),
    UpToDate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An elaborated transform, ready to execute
pub struct TransformExecutionPlan {
    pub request: TransformRequestExt,
    pub datasets_map: ResolvedDatasetsMap,
}

impl std::fmt::Debug for TransformExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.request.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq)]
pub enum TransformStatus {
    UpToDate,
    NewInputDataAvailable {
        input_advancements: Vec<odf::metadata::ExecuteTransformInput>,
    },
    /// The history of at least one input was rewritten: its head no longer
    /// descends from the block the last transform consumed.
    InputBreakingChange {
        breaking_inputs: Vec<odf::metadata::ExecuteTransformInput>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VerifyTransformStep {
    pub request: TransformRequestExt,
    pub expected_block: odf::MetadataBlock,
    pub expected_hash: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VerifyTransformOperation {
    pub steps: Vec<VerifyTransformStep>,
    pub datasets_map: ResolvedDatasetsMap,
}

impl std::fmt::Debug for VerifyTransformOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.steps.iter()).finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
