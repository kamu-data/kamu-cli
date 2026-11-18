// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_core::*;

use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs one transform job of `kamu pull`: the transform, and, when asked, a
/// reset of the derivative to metadata on a diverged input followed by the
/// transform once more
pub(crate) struct PullTransformJob {
    transform_job: Arc<dyn TransformJob>,
    compaction_job: Arc<dyn CompactionJob>,
}

#[common_macros::method_names_consts]
impl PullTransformJob {
    pub(crate) fn new(
        transform_job: Arc<dyn TransformJob>,
        compaction_job: Arc<dyn CompactionJob>,
    ) -> Self {
        Self {
            transform_job,
            compaction_job,
        }
    }

    #[tracing::instrument(level = "info", name = PullTransformJob_run, skip_all, fields(target = %pti.target, outcome))]
    pub(crate) async fn run(
        self,
        pti: PullTransformItem,
        transform_options: TransformOptions,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<PullResponse, InternalError> {
        let response = self.run_job(pti, transform_options, maybe_listener).await;
        SpanOutcome::of_pull_response(&response).record();
        response
    }

    async fn run_job(
        self,
        pti: PullTransformItem,
        transform_options: TransformOptions,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<PullResponse, InternalError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullTransformListener));

        let transform_result = self
            .transform(&pti.target, transform_options, listener.clone())
            .await;

        match &transform_result {
            Ok(result) => listener.success(result),
            Err(e) => listener.error(e),
        }

        Ok(PullResponse {
            maybe_original_request: pti.maybe_original_request,
            maybe_local_ref: Some(pti.target.as_local_ref()),
            maybe_remote_ref: None,
            result: transform_result.map(Into::into),
        })
    }

    async fn transform(
        &self,
        target_handle: &odf::DatasetHandle,
        transform_options: TransformOptions,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, PullError> {
        match self
            .transform_job
            .run(target_handle, Some(listener.clone()))
            .await
        {
            Err(TransformJobError::Elaborate(TransformElaborateError::InvalidInputInterval(e)))
                if transform_options.reset_derivatives_on_diverged_input =>
            {
                tracing::warn!(error = %e, "Input diverged, resetting the derivative to metadata");

                let Some((old_head, new_head)) = self.reset_to_metadata(target_handle).await?
                else {
                    return Err(Self::into_pull_error(TransformJobError::Elaborate(
                        TransformElaborateError::InvalidInputInterval(e),
                    )));
                };

                match self.transform_job.run(target_handle, Some(listener)).await {
                    Ok(transform_result) => Ok(Self::combine_with_reset(
                        old_head,
                        new_head,
                        transform_result,
                    )),
                    Err(source) => Err(PullError::TransformStopped(Box::new(
                        TransformStoppedError {
                            old_head,
                            new_head,
                            source: Self::into_pull_error(source),
                        },
                    ))),
                }
            }
            result => result.map_err(Self::into_pull_error),
        }
    }

    /// Resets the derivative to metadata, returning the heads before and
    /// after, or `None` when there was nothing to reset. Resetting drops the
    /// derivative's data, so the job checks the right to maintain the
    /// dataset, beyond the write the pull needs
    async fn reset_to_metadata(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<Option<(odf::Multihash, odf::Multihash)>, PullError> {
        let options = CompactionOptions {
            keep_metadata_only: true,
            ..Default::default()
        };
        match self.compaction_job.run(target_handle, options, None).await {
            Ok(CompactionResult::NothingToDo) => Ok(None),
            Ok(CompactionResult::Success {
                old_head, new_head, ..
            }) => Ok(Some((old_head, new_head))),
            Err(CompactionError::NotFound(e)) => Err(PullError::NotFound(e)),
            Err(
                CompactionError::Access(e)
                | CompactionError::Planning(CompactionPlanningError::Access(e)),
            ) => Err(PullError::Access(e)),
            Err(CompactionError::RefCASFailed(e)) => Err(PullError::RefMoved(e)),
            Err(CompactionError::Internal(e)) => Err(PullError::Internal(e)),
            Err(
                e @ (CompactionError::Planning(
                    CompactionPlanningError::InvalidDatasetKind(_)
                    | CompactionPlanningError::Internal(_),
                )
                | CompactionError::Execution(_)),
            ) => Err(PullError::Internal(e.int_err())),
        }
    }

    fn into_pull_error(error: TransformJobError) -> PullError {
        match error {
            TransformJobError::NotFound(e) => PullError::NotFound(e),
            TransformJobError::Access(e) => PullError::Access(e),
            TransformJobError::RefMoved(e) => PullError::RefMoved(e),
            TransformJobError::Elaborate(e) => TransformError::from(e).into(),
            TransformJobError::Execute(e) => TransformError::from(e).into(),
            TransformJobError::Internal(e) => PullError::Internal(e),
        }
    }

    /// A reset and the transform after it are reported as one update. The
    /// offsets and the increment stay the transform's own: the reset rewrote
    /// history, so no interval spans both
    fn combine_with_reset(
        old_head: odf::Multihash,
        reset_head: odf::Multihash,
        transform_result: TransformResult,
    ) -> TransformResult {
        match transform_result {
            TransformResult::Updated {
                new_head,
                new_offset_interval,
                increment,
                ..
            } => TransformResult::Updated {
                old_head,
                new_head,
                new_offset_interval,
                increment,
            },
            TransformResult::UpToDate => TransformResult::Updated {
                old_head,
                new_head: reset_head,
                new_offset_interval: None,
                increment: odf::dataset::MetadataChainIncrementInterval::default(),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
