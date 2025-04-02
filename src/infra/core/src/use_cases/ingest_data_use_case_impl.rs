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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::*;
use kamu_datasets::{DatasetExternallyChangedMessage, MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER};
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn IngestDataUseCase)]
pub struct IngestDataUseCaseImpl {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    push_ingest_planner: Arc<dyn PushIngestPlanner>,
    push_ingest_executor: Arc<dyn PushIngestExecutor>,
    outbox: Arc<dyn Outbox>,
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl IngestDataUseCase for IngestDataUseCaseImpl {
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
        data_source: DataSource,
        options: IngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, IngestDataError> {
        let target = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(dataset_ref, auth::DatasetAction::Write)
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(err) => IngestDataError::NotFound(err),
                RebacDatasetRefUnresolvedError::Access(err) => IngestDataError::Access(err),
                _ => IngestDataError::Internal(e.int_err()),
            })?;

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
                .int_err()?;

            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
                    DatasetExternallyChangedMessage::ingest_http(
                        target.get_id(),
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
