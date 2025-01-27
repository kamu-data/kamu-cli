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
use internal_error::ErrorIntoInternal;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::{
    CommitDatasetEventUseCase,
    DatasetLifecycleMessage,
    DatasetRegistry,
    ViewMultiResponse,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};
use odf::dataset::{AppendError, InvalidEventError};
use odf::metadata::{EnumWithVariants, TransformInputExt};

use crate::utils::access_dataset_helper::{AccessDatasetHelper, DatasetAccessError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CommitDatasetEventUseCase)]
pub struct CommitDatasetEventUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    outbox: Arc<dyn Outbox>,
}

impl CommitDatasetEventUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_action_authorizer,
            outbox,
        }
    }

    async fn validate_event(
        &self,
        access_dataset_helper: AccessDatasetHelper<'_>,
        event: odf::MetadataEvent,
    ) -> Result<odf::MetadataEvent, odf::dataset::CommitError> {
        if let Some(set_transform) = event.as_variant::<odf::metadata::SetTransform>() {
            let mut inputs_dataset_refs = Vec::with_capacity(set_transform.inputs.len());
            for input in &set_transform.inputs {
                let input_dataset_ref = input.as_sanitized_dataset_ref()?;
                inputs_dataset_refs.push(input_dataset_ref);
            }

            let view_multi_result: ViewMultiResponse = access_dataset_helper
                .access_multi_dataset(inputs_dataset_refs, DatasetAction::Read)
                .await
                .map(Into::into)?;

            if !view_multi_result.inaccessible_refs.is_empty() {
                let message = view_multi_result.into_inaccessible_input_datasets_message();

                return Err(AppendError::InvalidBlock(
                    InvalidEventError::new(event, message).into(),
                )
                .into());
            }
        }

        Ok(event)
    }
}

#[async_trait::async_trait]
impl CommitDatasetEventUseCase for CommitDatasetEventUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "CommitDatasetEventUseCase::execute",
        skip_all,
        fields(dataset_handle, ?event, ?opts)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        event: odf::MetadataEvent,
        opts: odf::dataset::CommitOpts<'_>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        let access_dataset_helper =
            AccessDatasetHelper::new(&self.dataset_registry, &self.dataset_action_authorizer);

        access_dataset_helper
            .access_dataset(&dataset_handle.as_local_ref(), DatasetAction::Write)
            .await
            .map_err(|e| {
                use odf::dataset::CommitError;
                match e {
                    DatasetAccessError::Access(e) => CommitError::Access(e),
                    unexpected_error => CommitError::Internal(unexpected_error.int_err()),
                }
            })?;

        let event = self.validate_event(access_dataset_helper, event).await?;

        let resolved_dataset = self.dataset_registry.get_dataset_by_handle(dataset_handle);

        let commit_result = resolved_dataset.commit_event(event, opts).await?;

        if !commit_result.new_upstream_ids.is_empty() {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                    DatasetLifecycleMessage::dependencies_updated(
                        dataset_handle.id.clone(),
                        commit_result.new_upstream_ids.clone(),
                    ),
                )
                .await?;
        }

        Ok(commit_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
