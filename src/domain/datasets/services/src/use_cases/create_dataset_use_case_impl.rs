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
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{
    CreateDatasetError,
    CreateDatasetResult,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
};

use crate::utils::DatasetCreateHelper;
use crate::DependencyGraphWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CreateDatasetUseCase)]
pub struct CreateDatasetUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
    dataset_create_helper: Arc<DatasetCreateHelper>,
}

impl CreateDatasetUseCaseImpl {
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
        dataset_create_helper: Arc<DatasetCreateHelper>,
    ) -> Self {
        Self {
            current_account_subject,
            dependency_graph_writer,
            dataset_create_helper,
        }
    }
}

#[async_trait::async_trait]
impl CreateDatasetUseCase for CreateDatasetUseCaseImpl {
    #[tracing::instrument(level = "info", skip_all, fields(dataset_alias, ?seed_block, ?options))]
    async fn execute(
        &self,
        dataset_alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        // There must be a logged in user
        let logged_account_id = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot create dataset");
            }
            CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        };

        // Adjust alias for current tenancy configuration
        let canonical_alias = self
            .dataset_create_helper
            .canonical_dataset_alias(dataset_alias);

        // Dataset entry goes first, this guarantees name collision check
        self.dataset_create_helper
            .create_dataset_entry(
                &seed_block.event.dataset_id,
                &logged_account_id,
                &canonical_alias,
            )
            .await?;

        // Graph node is created
        // TODO:
        //  - separate in-memory and database-level updates
        //  - database-level update may be a part of current transaction
        //  - in-memory update should only happen if database-level update is committed
        self.dependency_graph_writer
            .create_dataset_node(&seed_block.event.dataset_id)
            .await?;

        // Make storage level dataset (no HEAD yet)
        let store_result = self
            .dataset_create_helper
            .store_dataset(&canonical_alias, seed_block)
            .await?;

        // Set initial dataset HEAD
        self.dataset_create_helper
            .set_created_head(&store_result.dataset_id, &store_result.seed)
            .await?;

        // Notify interested parties the dataset was created
        self.dataset_create_helper
            .notify_dataset_created(
                &store_result.dataset_id,
                &canonical_alias.dataset_name,
                &logged_account_id,
                options.dataset_visibility,
            )
            .await?;

        Ok(CreateDatasetResult::from_stored(
            store_result,
            canonical_alias,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
