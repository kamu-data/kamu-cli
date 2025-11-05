// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_datasets::{DatasetExternallyChangedMessage, MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER};
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn PushIngestDataUseCase)]
pub struct PushIngestDataUseCaseImpl {
    push_ingest_planner: Arc<dyn PushIngestPlanner>,
    push_ingest_executor: Arc<dyn PushIngestExecutor>,
    outbox: Arc<dyn Outbox>,
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl PushIngestDataUseCase for PushIngestDataUseCaseImpl {
    async fn execute(
        &self,
        target: ResolvedDataset,
        data_source: DataSource,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError> {
        let ingest_plan = self
            .push_ingest_planner
            .plan_ingest(
                target.clone(),
                options.source_name.as_deref(),
                PushIngestOpts {
                    media_type: options.media_type,
                    source_event_time: options.source_event_time,
                    auto_create_push_source: options.is_ingest_from_upload,
                    schema_inference: SchemaInferenceOpts::default(),
                    expected_head: options.expected_head,
                },
            )
            .await?;

        let push_execute_result = self
            .push_ingest_executor
            .execute_ingest(target.clone(), ingest_plan, data_source, listener_maybe)
            .await?;

        if let PushIngestResult::Updated {
            old_head, new_head, ..
        } = &push_execute_result
        {
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
                        options.source_name,
                        Some(old_head),
                        new_head,
                    ),
                )
                .await
                .int_err()?;
        }

        Ok(push_execute_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
