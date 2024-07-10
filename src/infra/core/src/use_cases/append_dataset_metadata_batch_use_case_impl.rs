// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::Arc;

use dill::{component, interface};
use kamu_core::{
    AppendDatasetMetadataBatchUseCase,
    AppendError,
    AppendOpts,
    BlockRef,
    Dataset,
    DatasetDependenciesUpdatedMessage,
    GetSummaryOpts,
    HashedMetadataBlock,
    ResultIntoInternal,
    SetRefOpts,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{post_outbox_message, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AppendDatasetMetadataBatchUseCaseImpl {
    outbox: Arc<dyn Outbox>,
}

#[component(pub)]
#[interface(dyn AppendDatasetMetadataBatchUseCase)]
impl AppendDatasetMetadataBatchUseCaseImpl {
    pub fn new(outbox: Arc<dyn Outbox>) -> Self {
        Self { outbox }
    }
}

#[async_trait::async_trait]
impl AppendDatasetMetadataBatchUseCase for AppendDatasetMetadataBatchUseCaseImpl {
    async fn execute(
        &self,
        dataset: &dyn Dataset,
        new_blocks: VecDeque<HashedMetadataBlock>,
        force_update_if_diverged: bool,
    ) -> Result<(), AppendError> {
        if new_blocks.is_empty() {
            return Ok(());
        }

        let old_head = new_blocks.front().unwrap().1.prev_block_hash.clone();
        let new_head = new_blocks.back().unwrap().0.clone();

        let metadata_chain = dataset.as_metadata_chain();

        let mut new_upstream_ids: Vec<opendatafabric::DatasetID> = vec![];

        for (hash, block) in new_blocks {
            tracing::debug!(sequence_numer = %block.sequence_number, hash = %hash, "Appending block");

            if let opendatafabric::MetadataEvent::SetTransform(transform) = &block.event {
                // Collect only the latest upstream dataset IDs
                new_upstream_ids.clear();
                for new_input in &transform.inputs {
                    if let Some(id) = new_input.dataset_ref.id() {
                        new_upstream_ids.push(id.clone());
                    } else {
                        // Input references must be resolved to IDs here, but we
                        // ignore the errors and let the metadata chain reject
                        // this event
                    }
                }
            }

            metadata_chain
                .append(
                    block,
                    AppendOpts {
                        update_ref: None,
                        expected_hash: Some(&hash),
                        ..AppendOpts::default()
                    },
                )
                .await?;
        }

        metadata_chain
            .set_ref(
                &BlockRef::Head,
                &new_head,
                SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: if force_update_if_diverged {
                        None
                    } else {
                        Some(old_head.as_ref())
                    },
                },
            )
            .await?;

        if !new_upstream_ids.is_empty() {
            let summary = dataset
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?;

            post_outbox_message(
                self.outbox.as_ref(),
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetDependenciesUpdatedMessage {
                    dataset_id: summary.id.clone(),
                    new_upstream_ids,
                },
            )
            .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
