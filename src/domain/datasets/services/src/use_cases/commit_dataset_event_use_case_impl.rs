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
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::auth;
use kamu_datasets::CommitDatasetEventUseCase;
use odf::dataset::{AppendError, CommitError, InvalidEventError};
use odf::metadata::EnumWithVariants;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn CommitDatasetEventUseCase)]
pub struct CommitDatasetEventUseCaseImpl {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl CommitDatasetEventUseCaseImpl {
    async fn validate_event(
        &self,
        event: odf::MetadataEvent,
    ) -> Result<odf::MetadataEvent, CommitError> {
        if let Some(set_transform) = event.as_variant::<odf::metadata::SetTransform>() {
            let inputs_dataset_refs = set_transform
                .inputs
                .iter()
                .map(|input| input.dataset_ref.clone())
                .collect::<Vec<_>>();

            let classify_response = self
                .rebac_dataset_registry_facade
                .classify_dataset_refs_by_allowance(inputs_dataset_refs, auth::DatasetAction::Read)
                .await?;

            if !classify_response.inaccessible_refs.is_empty() {
                let dataset_ref_alias_map = set_transform.as_dataset_ref_alias_map();
                let message = classify_response
                    .into_inaccessible_input_datasets_message(&dataset_ref_alias_map);

                return Err(AppendError::InvalidBlock(
                    InvalidEventError::new(event, message).into(),
                )
                .into());
            }
        }

        Ok(event)
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CommitDatasetEventUseCase for CommitDatasetEventUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = CommitDatasetEventUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, ?event)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        event: odf::MetadataEvent,
    ) -> Result<odf::dataset::CommitResult, CommitError> {
        let resolved_dataset = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(&dataset_handle.as_local_ref(), auth::DatasetAction::Write)
            .await
            .map_err(|e| {
                use RebacDatasetRefUnresolvedError as E;
                match e {
                    E::Access(e) => CommitError::Access(e),
                    e @ (E::NotFound(_) | E::Internal(_)) => CommitError::Internal(e.int_err()),
                }
            })?;
        let event = self.validate_event(event).await?;

        use odf::dataset::CommitOpts;
        let commit_result = resolved_dataset
            .commit_event(
                event,
                CommitOpts {
                    system_time: Some(self.system_time_source.now()),
                    ..CommitOpts::default()
                },
            )
            .await?;

        Ok(commit_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
