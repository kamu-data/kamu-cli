// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{CompactionExecutionError, CompactionPlan, CompactionPlanningError};
use crate::CompactionResult;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait CompactionListener: Send + Sync {
    fn begin(&self) {}

    fn plan_success(&self, _plan: &CompactionPlan) {}
    fn execute_success(&self, _res: &CompactionResult) {}

    fn plan_error(&self, _err: &CompactionPlanningError) {}
    fn execute_error(&self, _err: &CompactionExecutionError) {}

    fn begin_phase(&self, _phase: CompactionPhase) {}
    fn end_phase(&self, _phase: CompactionPhase) {}
}

pub struct NullCompactionListener;
impl CompactionListener for NullCompactionListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait CompactionMultiListener: Send + Sync {
    fn begin_compact(&self, _dataset: &odf::DatasetHandle) -> Option<Arc<dyn CompactionListener>> {
        None
    }
}

pub struct NullCompactionMultiListener;
impl CompactionMultiListener for NullCompactionMultiListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPhase {
    GatherChainInfo,
    MergeDataslices,
    CommitNewBlocks,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
