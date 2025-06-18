// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_task_system as ts;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSupportService: Send + Sync {
    async fn interpret_input_dataset_result(
        &self,
        dataset_id: &odf::DatasetID,
        input_result: &ts::TaskResult,
    ) -> Result<FlowInputResultInterpretation, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowInputResultInterpretation {
    pub new_records_count: u64,
    pub was_compacted: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum DownstreamDependencyTriggerType {
    TriggerAllEnabledExecuteTransform,
    TriggerOwnHardCompaction,
    Empty,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
