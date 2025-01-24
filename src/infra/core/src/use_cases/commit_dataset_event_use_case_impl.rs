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
use kamu_core::auth::DatasetActionUnauthorizedError;
use kamu_core::{
    CommitDatasetEventUseCase,
    DatasetLifecycleMessage,
    DatasetRegistry,
    EditDatasetUseCase,
    EditDatasetUseCaseError,
    NotAccessibleError,
    ViewDatasetUseCase,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};
use odf::dataset::{AppendError, InvalidEventError};
use odf::metadata::{EnumWithVariants, TransformInputExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CommitDatasetEventUseCase)]
pub struct CommitDatasetEventUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    view_dataset_use_case: Arc<dyn ViewDatasetUseCase>,
    edit_dataset_use_case: Arc<dyn EditDatasetUseCase>,
    outbox: Arc<dyn Outbox>,
}

impl CommitDatasetEventUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        view_dataset_use_case: Arc<dyn ViewDatasetUseCase>,
        edit_dataset_use_case: Arc<dyn EditDatasetUseCase>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            dataset_registry,
            view_dataset_use_case,
            edit_dataset_use_case,
            outbox,
        }
    }

    async fn validate_event(
        &self,
        event: odf::MetadataEvent,
    ) -> Result<odf::MetadataEvent, odf::dataset::CommitError> {
        if let Some(set_transform) = event.as_variant::<odf::metadata::SetTransform>() {
            let mut inputs_dataset_refs = Vec::with_capacity(set_transform.inputs.len());
            for input in &set_transform.inputs {
                let input_dataset_ref = input.as_sanitized_dataset_ref()?;
                inputs_dataset_refs.push(input_dataset_ref);
            }

            let view_result = self
                .view_dataset_use_case
                .execute_multi(inputs_dataset_refs)
                .await?;

            if !view_result.inaccessible_refs.is_empty() {
                let message = view_result.into_inaccessible_input_datasets_message();

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
        self.edit_dataset_use_case
            .execute(&dataset_handle.as_local_ref())
            .await
            .map_err(map_edit_use_case_error)?;

        let event = self.validate_event(event).await?;

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
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_edit_use_case_error(e: EditDatasetUseCaseError) -> odf::dataset::CommitError {
    use odf::dataset::CommitError;

    match e {
        EditDatasetUseCaseError::NotAccessible(e) => match e {
            NotAccessibleError::Unauthorized(e) => match e {
                DatasetActionUnauthorizedError::Access(e) => CommitError::Access(e),
                unexpected_error => CommitError::Internal(unexpected_error.int_err()),
            },
            unexpected_error => CommitError::Internal(unexpected_error.int_err()),
        },
        unexpected_error => CommitError::Internal(unexpected_error.int_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
