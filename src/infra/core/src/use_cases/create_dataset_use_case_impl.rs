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
use kamu_core::messages::DatasetCreatedMessage;
use kamu_core::{
    CreateDatasetError,
    CreateDatasetResult,
    CreateDatasetUseCase,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{post_outbox_message, Outbox};
use opendatafabric::{DatasetAlias, MetadataBlockTyped, Seed};

use crate::DatasetRepositoryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CreateDatasetUseCase)]
pub struct CreateDatasetUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    outbox: Arc<dyn Outbox>,
}

impl CreateDatasetUseCaseImpl {
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            current_account_subject,
            dataset_repo_writer,
            outbox,
        }
    }
}

#[async_trait::async_trait]
impl CreateDatasetUseCase for CreateDatasetUseCaseImpl {
    async fn execute(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError> {
        let create_result = self
            .dataset_repo_writer
            .create_dataset(dataset_alias, seed_block)
            .await?;

        let message = DatasetCreatedMessage {
            dataset_id: create_result.dataset_handle.id.clone(),
            owner_account_id: match self.current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Anonymous account cannot create dataset");
                }
                CurrentAccountSubject::Logged(l) => l.account_id.clone(),
            },
        };

        post_outbox_message(
            self.outbox.as_ref(),
            MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            message,
        )
        .await?;

        Ok(create_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
