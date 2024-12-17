// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::ErrorIntoInternal;
use kamu_core::*;
use kamu_ingest_datafusion::DataWriterDataFusion;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SetWatermarkExecutionService)]
pub struct SetWatermarkExecutionServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SetWatermarkExecutionService for SetWatermarkExecutionServiceImpl {
    async fn execute_set_watermark(
        &self,
        target: ResolvedDataset,
        plan: SetWatermarkPlan,
    ) -> Result<SetWatermarkResult, SetWatermarkExecutionError> {
        let mut writer = DataWriterDataFusion::builder(
            target.clone(),
            datafusion::prelude::SessionContext::new(),
        )
        .with_metadata_state(*plan.metadata_state)
        .build();

        match writer
            .write_watermark(
                plan.new_watermark,
                WriteWatermarkOpts {
                    system_time: plan.system_time,
                    new_source_state: None,
                },
            )
            .await
        {
            Ok(res) => Ok(SetWatermarkResult::Updated {
                old_head: Some(res.old_head),
                new_head: res.new_head,
            }),
            Err(
                WriteWatermarkError::EmptyCommit(_)
                | WriteWatermarkError::CommitError(CommitError::MetadataAppendError(
                    AppendError::InvalidBlock(AppendValidationError::WatermarkIsNotMonotonic),
                )),
            ) => Ok(SetWatermarkResult::UpToDate),
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
