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
#[interface(dyn SetWatermarkExecutor)]
pub struct SetWatermarkExecutorImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SetWatermarkExecutor for SetWatermarkExecutorImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            target=%target.get_handle(),
            new_watermark=%plan.new_watermark
        )
    )]
    async fn execute(
        &self,
        target: ResolvedDataset,
        plan: SetWatermarkPlan,
    ) -> Result<SetWatermarkResult, SetWatermarkExecutionError> {
        let mut writer = DataWriterDataFusion::from_metadata_state(
            datafusion::prelude::SessionContext::new(),
            target.clone(),
            *plan.metadata_state,
        );

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
                | WriteWatermarkError::CommitError(odf::dataset::CommitError::MetadataAppendError(
                    odf::dataset::AppendError::InvalidBlock(
                        odf::dataset::AppendValidationError::WatermarkIsNotMonotonic,
                    ),
                )),
            ) => Ok(SetWatermarkResult::UpToDate),
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
