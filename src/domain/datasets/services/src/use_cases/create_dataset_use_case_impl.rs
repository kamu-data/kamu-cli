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
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetLifecycleMessage,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};

use crate::{CreateDatasetEntryError, DatasetEntryWriter, DependencyGraphWriter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CreateDatasetUseCase)]
pub struct CreateDatasetUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    outbox: Arc<dyn Outbox>,
}

impl CreateDatasetUseCaseImpl {
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
        dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
        dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            current_account_subject,
            dataset_entry_writer,
            dependency_graph_writer,
            dataset_storage_unit_writer,
            outbox,
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
    ) -> Result<odf::CreateDatasetResult, odf::dataset::CreateDatasetError> {
        let logged_account_id = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot create dataset");
            }
            CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        };

        self.dataset_entry_writer
            .create_entry(
                &seed_block.event.dataset_id,
                &logged_account_id,
                &dataset_alias.dataset_name,
            )
            .await
            .map_err(|e| match e {
                CreateDatasetEntryError::Internal(e) => {
                    odf::dataset::CreateDatasetError::Internal(e)
                }
                CreateDatasetEntryError::DuplicateId(e) => {
                    odf::dataset::CreateDatasetError::Internal(e.int_err())
                }
                CreateDatasetEntryError::NameCollision(e) => {
                    odf::dataset::CreateDatasetError::NameCollision(
                        odf::dataset::NameCollisionError {
                            alias: odf::DatasetAlias::new(
                                dataset_alias.account_name.clone(),
                                e.dataset_name,
                            ),
                        },
                    )
                }
            })?;

        self.dependency_graph_writer
            .create_dataset_node(&seed_block.event.dataset_id)
            .await?;

        let create_result = self
            .dataset_storage_unit_writer
            .create_dataset(dataset_alias, seed_block)
            .await?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    create_result.dataset_handle.id.clone(),
                    logged_account_id,
                    options.dataset_visibility,
                    dataset_alias.dataset_name.clone(),
                ),
            )
            .await?;

        Ok(create_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
