// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::{
    DatasetExternallyChangedMessage,
    DatasetHeadCommitter,
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
    ResolvedDataset,
};
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn PushIngestDataUseCase)]
pub struct PushIngestDataUseCaseImpl {
    push_ingest_planner: Arc<dyn PushIngestPlanner>,
    push_ingest_executor: Arc<dyn PushIngestExecutor>,
    dataset_head_committer: Arc<dyn DatasetHeadCommitter>,
    dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
    current_account_subject: Arc<CurrentAccountSubject>,
    outbox: Arc<dyn Outbox>,
}

impl PushIngestDataUseCaseImpl {
    async fn authorize(&self, target: &ResolvedDataset) -> Result<(), PushIngestDataError> {
        self.dataset_operation_guard
            .check_operation(
                &self.current_account_subject,
                &DatasetOperationRequest::new(
                    target.get_handle().clone(),
                    DatasetOperation::PushIngest,
                ),
            )
            .await
            .map_err(|e| {
                use DatasetPermissionError as E;

                match e {
                    E::NotFound(e) => PushIngestDataError::NotFound(e),
                    E::Access(e) => PushIngestDataError::Access(e),
                    e @ E::Internal(_) => PushIngestDataError::Internal(e.int_err()),
                }
            })
    }

    async fn run_single_ingest(
        &self,
        target: ResolvedDataset,
        data_source: DataSource,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        let PushIngestDataUseCaseOptions {
            source_name,
            source_event_time,
            is_ingest_from_upload,
            media_type,
            expected_head,
            skip_quota_check,
        } = options;

        let opts = PushIngestOpts {
            media_type,
            source_event_time,
            schema_inference: SchemaInferenceOpts::default(),
            expected_head,
            skip_quota_check,
        };

        if is_ingest_from_upload
            && let Some(push_source) = self
                .push_ingest_planner
                .plan_push_source_creation(target.clone(), source_name.as_deref(), &opts)
                .await?
        {
            self.add_push_source(&target, push_source, source_event_time)
                .await?;
        }

        let ingest_plan = self
            .push_ingest_planner
            .plan_ingest(target.clone(), source_name.as_deref(), opts)
            .await?;

        self.push_ingest_executor
            .execute_ingest(target, ingest_plan, data_source, listener_maybe)
            .await
            .map_err(Into::into)
    }

    /// Commits the push source on top of `HEAD` as it is now
    async fn add_push_source(
        &self,
        target: &ResolvedDataset,
        push_source: odf::metadata::AddPushSource,
        system_time: Option<DateTime<Utc>>,
    ) -> Result<(), PushIngestDataError> {
        let old_head = target
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;

        let commit_result = target
            .commit_event(
                odf::MetadataEvent::AddPushSource(push_source),
                odf::dataset::CommitOpts {
                    system_time,
                    prev_block_hash: Some(Some(&old_head)),
                    update_block_ref: false,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| PushIngestDataError::Execution(e.into()))?;

        self.dataset_head_committer
            .commit_head(target, Some(&old_head), &commit_result.new_head)
            .await
            .map_err(|e| PushIngestDataError::Execution(odf::dataset::CommitError::from(e).into()))
    }

    async fn finalize_head_update(
        &self,
        target: &ResolvedDataset,
        source_name: Option<String>,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<(), PushIngestDataError> {
        self.dataset_head_committer
            .commit_head(target, Some(old_head), new_head)
            .await
            .map_err(|e| {
                PushIngestDataError::Execution(odf::dataset::CommitError::from(e).into())
            })?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
                DatasetExternallyChangedMessage::ingest_http(
                    target.get_id(),
                    source_name,
                    Some(old_head),
                    new_head,
                ),
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn ingest_and_commit(
        &self,
        target: ResolvedDataset,
        data_source: DataSource,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.authorize(&target).await?;

        let source_name = options.source_name.clone();
        let push_execute_result = self
            .run_single_ingest(target.clone(), data_source, options, listener_maybe)
            .await?;

        if let PushIngestResult::Updated {
            old_head, new_head, ..
        } = &push_execute_result
        {
            self.finalize_head_update(&target, source_name, old_head, new_head)
                .await?;
        }

        Ok(push_execute_result)
    }

    /// Ingests the sources one after another and commits `HEAD` once, after
    /// the last of them
    async fn ingest_all_and_commit(
        &self,
        target: ResolvedDataset,
        data_sources: Vec<DataSource>,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        self.authorize(&target).await?;

        if data_sources.is_empty() {
            return Ok(PushIngestResult::UpToDate);
        }

        let base_options = options;
        let source_name = base_options.source_name.clone();
        let mut next_expected_head = base_options.expected_head.clone();
        let mut total_blocks = 0usize;
        let mut last_system_time = Utc::now();
        let mut head_transition: Option<(odf::Multihash, odf::Multihash)> = None;

        for data_source in data_sources {
            let mut iteration_options = base_options.clone();
            iteration_options
                .expected_head
                .clone_from(&next_expected_head);

            let ingest_result = self
                .run_single_ingest(
                    target.clone(),
                    data_source,
                    iteration_options,
                    listener_maybe.clone(),
                )
                .await?;

            if let PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks,
                system_time,
            } = ingest_result
            {
                total_blocks += num_blocks;
                last_system_time = system_time;

                match &mut head_transition {
                    None => head_transition = Some((old_head, new_head.clone())),
                    Some((_, final_new)) => *final_new = new_head.clone(),
                }

                next_expected_head = Some(new_head);
            }
        }

        if let Some((old_head, new_head)) = head_transition {
            self.finalize_head_update(&target, source_name, &old_head, &new_head)
                .await?;

            Ok(PushIngestResult::Updated {
                old_head,
                new_head,
                num_blocks: total_blocks,
                system_time: last_system_time,
            })
        } else {
            Ok(PushIngestResult::UpToDate)
        }
    }

    /// Ends the listener's progress once `HEAD` is committed or the ingest
    /// failed
    fn report(
        listener_maybe: Option<&dyn PushIngestListener>,
        result: &Result<PushIngestResult, PushIngestDataError>,
    ) {
        if let Some(listener) = listener_maybe {
            match result {
                Ok(ingest_result) => listener.success(ingest_result),
                Err(e) => listener.error(e),
            }
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl PushIngestDataUseCase for PushIngestDataUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = PushIngestDataUseCaseImpl_execute,
        skip_all,
        fields(dataset_id = %target.get_id())
    )]
    async fn execute(
        &self,
        target: ResolvedDataset,
        data_source: DataSource,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        let result = self
            .ingest_and_commit(target, data_source, options, listener_maybe.clone())
            .await;
        Self::report(listener_maybe.as_deref(), &result);
        result
    }

    #[tracing::instrument(
        level = "info",
        name = PushIngestDataUseCaseImpl_execute_multi,
        skip_all,
        fields(dataset_id = %target.get_id())
    )]
    async fn execute_multi(
        &self,
        target: ResolvedDataset,
        data_sources: Vec<DataSource>,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        let result = self
            .ingest_all_and_commit(target, data_sources, options, listener_maybe.clone())
            .await;
        Self::report(listener_maybe.as_deref(), &result);
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
