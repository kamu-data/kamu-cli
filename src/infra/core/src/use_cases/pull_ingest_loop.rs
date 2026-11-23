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

/// Repeats polling ingest iterations of one dataset for `kamu pull`; each
/// iteration commits its own `HEAD`
pub(crate) struct PullIngestLoop {
    polling_ingest_job: Arc<dyn PollingIngestJob>,
}

#[common_macros::method_names_consts]
impl PullIngestLoop {
    pub(crate) fn new(polling_ingest_job: Arc<dyn PollingIngestJob>) -> Self {
        Self { polling_ingest_job }
    }

    /// Runs the ingest job of one dataset and reports it as the pull's response
    #[tracing::instrument(level = "info", name = PullIngestLoop_run, skip_all, fields(target = %pii.target, outcome))]
    pub(crate) async fn run(
        self,
        pii: PullIngestItem,
        options: PullOptions,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PullResponse, InternalError> {
        let response = self.run_job(pii, options, maybe_listener).await;
        SpanOutcome::of_pull_response(&response).record();
        response
    }

    async fn run_job(
        self,
        pii: PullIngestItem,
        options: PullOptions,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PullResponse, InternalError> {
        let result = self
            .ingest(
                &pii.target,
                options.ingest_options,
                options.exhaust_sources,
                maybe_listener,
            )
            .await
            .map(Into::into);

        Ok(PullResponse {
            maybe_original_request: pii.maybe_original_request,
            maybe_local_ref: Some(pii.target.as_local_ref()),
            maybe_remote_ref: None,
            result,
        })
    }

    /// Ingests until the source reports no more data, or once when
    /// `exhaust_sources` is off. An iteration that fails after others
    /// committed is [`PullError::IngestStopped`]. The listener learns the
    /// result of each iteration, and the failure that ends the ingest
    async fn ingest(
        &self,
        target_handle: &odf::DatasetHandle,
        options: PollingIngestOptions,
        exhaust_sources: bool,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PullError> {
        let result = self
            .plan_and_ingest(
                target_handle,
                options,
                exhaust_sources,
                maybe_listener.clone(),
            )
            .await;

        if let (Some(listener), Err(e)) = (&maybe_listener, &result) {
            listener.error(e);
        }

        result
    }

    async fn plan_and_ingest(
        &self,
        target_handle: &odf::DatasetHandle,
        options: PollingIngestOptions,
        exhaust_sources: bool,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PullError> {
        let mut combined: Option<PollingIngestResult> = None;
        let mut maybe_continuation = None;
        loop {
            let result = match self
                .polling_ingest_job
                .run(
                    target_handle,
                    options.clone(),
                    maybe_continuation.take(),
                    maybe_listener.clone(),
                )
                .await
            {
                Ok(result) => result,
                Err(PollingIngestJobError::Planning(PollingIngestPlanError::SourceNotDefined(
                    _,
                ))) if combined.is_none() => {
                    if let Some(listener) = &maybe_listener {
                        listener.no_polling_source();
                    }
                    return Ok(PollingIngestResult::UpToDate { uncacheable: false });
                }
                Err(e) => return Err(Self::stopped(combined, Self::into_pull_error(e))),
            };

            maybe_continuation = match &result {
                PollingIngestResult::UpToDate { .. } => None,
                PollingIngestResult::Updated {
                    has_more,
                    metadata_state,
                    ..
                } => (*has_more && exhaust_sources).then(|| metadata_state.clone()),
            };

            if let Some(listener) = &maybe_listener {
                listener.success(&result);
            }

            combined = Some(Self::merge_results(combined, result));

            if maybe_continuation.is_none() {
                break;
            }
        }

        Ok(combined.expect("The loop runs at least once"))
    }

    fn into_pull_error(error: PollingIngestJobError) -> PullError {
        match error {
            PollingIngestJobError::NotFound(e) => PullError::NotFound(e),
            PollingIngestJobError::Access(e) => PullError::Access(e),
            PollingIngestJobError::RefCASFailed(e) => PullError::RefMoved(e),
            PollingIngestJobError::Planning(PollingIngestPlanError::ScanMetadata(e)) => e.into(),
            PollingIngestJobError::Planning(e @ PollingIngestPlanError::SourceNotDefined(_)) => {
                PullError::Internal(e.int_err())
            }
            PollingIngestJobError::Planning(PollingIngestPlanError::Internal(e))
            | PollingIngestJobError::Internal(e) => PullError::Internal(e),
            PollingIngestJobError::Ingest(e) => e.into(),
        }
    }

    /// The error of an iteration, as [`PullError::IngestStopped`] when earlier
    /// iterations committed
    fn stopped(combined: Option<PollingIngestResult>, error: PullError) -> PullError {
        match combined {
            Some(PollingIngestResult::Updated {
                old_head,
                new_head,
                increment,
                ..
            }) => PullError::IngestStopped(Box::new(IngestStoppedError {
                old_head,
                new_head,
                increment,
                source: error,
            })),
            None | Some(PollingIngestResult::UpToDate { .. }) => error,
        }
    }

    /// Keeps the first `old_head` and the last `new_head`, and adds up the
    /// increments
    fn merge_results(
        combined: Option<PollingIngestResult>,
        next: PollingIngestResult,
    ) -> PollingIngestResult {
        match (combined, next) {
            (None | Some(PollingIngestResult::UpToDate { .. }), next) => next,
            (
                Some(PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    increment,
                    new_source_state,
                    metadata_state,
                    ..
                }),
                PollingIngestResult::UpToDate { uncacheable },
            ) => PollingIngestResult::Updated {
                old_head,
                new_head,
                has_more: false,
                uncacheable,
                increment,
                new_source_state,
                metadata_state,
            },
            (
                Some(PollingIngestResult::Updated {
                    old_head,
                    mut increment,
                    ..
                }),
                PollingIngestResult::Updated {
                    new_head,
                    has_more,
                    uncacheable,
                    increment: next_increment,
                    new_source_state,
                    metadata_state,
                    ..
                },
            ) => {
                increment += next_increment;
                PollingIngestResult::Updated {
                    old_head,
                    new_head,
                    has_more,
                    uncacheable,
                    increment,
                    new_source_state,
                    metadata_state,
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
