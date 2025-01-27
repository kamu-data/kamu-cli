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
use internal_error::InternalError;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::{DatasetRegistry, ViewDatasetUseCase, ViewDatasetUseCaseError, ViewMultiResponse};

use crate::utils::access_dataset_helper::AccessDatasetHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ViewDatasetUseCase)]
pub struct ViewDatasetUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl ViewDatasetUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ViewDatasetUseCase
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ViewDatasetUseCase for ViewDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "ViewDatasetUseCase::execute",
        skip_all,
        fields(dataset_ref)
    )]
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, ViewDatasetUseCaseError> {
        let access_dataset_helper =
            AccessDatasetHelper::new(&self.dataset_registry, &self.dataset_action_authorizer);

        access_dataset_helper
            .access_dataset(dataset_ref, DatasetAction::Read)
            .await
            .map_err(Into::into)
    }

    #[tracing::instrument(
        level = "info",
        name = "ViewDatasetUseCase::execute_multi",
        skip_all,
        fields(dataset_refs)
    )]
    async fn execute_multi(
        &self,
        dataset_refs: Vec<odf::DatasetRef>,
    ) -> Result<ViewMultiResponse, InternalError> {
        let access_dataset_helper =
            AccessDatasetHelper::new(&self.dataset_registry, &self.dataset_action_authorizer);

        access_dataset_helper
            .access_multi_dataset(dataset_refs, DatasetAction::Read)
            .await
            .map(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
