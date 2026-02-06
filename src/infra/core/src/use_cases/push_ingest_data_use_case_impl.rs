// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::{component, interface};
use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_datasets::{
    DatasetExternallyChangedMessage,
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
    outbox: Arc<dyn Outbox>,
}

impl PushIngestDataUseCaseImpl {
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
            ignore_quota_check,
        } = options;

        let ingest_plan = self
            .push_ingest_planner
            .plan_ingest(
                target.clone(),
                source_name.as_deref(),
                PushIngestOpts {
                    media_type,
                    source_event_time,
                    auto_create_push_source: is_ingest_from_upload,
                    schema_inference: SchemaInferenceOpts::default(),
                    expected_head,
                    ignore_quota_check,
                },
            )
            .await?;

        self.push_ingest_executor
            .execute_ingest(target, ingest_plan, data_source, listener_maybe)
            .await
            .map_err(Into::into)
    }

    async fn finalize_head_update(
        &self,
        target: &ResolvedDataset,
        source_name: Option<String>,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<(), PushIngestDataError> {
        target
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(Some(old_head)),
                },
            )
            .await
            .map_err(|e| {
                PushIngestDataError::Execution(
                    odf::dataset::CommitError::MetadataAppendError(e.into()).into(),
                )
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
