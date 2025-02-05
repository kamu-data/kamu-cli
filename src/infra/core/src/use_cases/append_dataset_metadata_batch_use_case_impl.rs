// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use dill::{component, interface};
use kamu_core::AppendDatasetMetadataBatchUseCase;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AppendDatasetMetadataBatchUseCaseImpl {
    // TODO
}

#[component(pub)]
#[interface(dyn AppendDatasetMetadataBatchUseCase)]
impl AppendDatasetMetadataBatchUseCaseImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl AppendDatasetMetadataBatchUseCase for AppendDatasetMetadataBatchUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "AppendDatasetMetadataBatchUseCase::execute",
        skip_all,
        fields(dataset_handle, ?new_blocks, force_update_if_diverged)
    )]
    async fn execute(
        &self,
        dataset: &dyn odf::Dataset,
        new_blocks: VecDeque<odf::dataset::HashedMetadataBlock>,
        force_update_if_diverged: bool,
    ) -> Result<(), odf::dataset::AppendError> {
        if new_blocks.is_empty() {
            return Ok(());
        }

        let old_head = new_blocks.front().unwrap().1.prev_block_hash.clone();
        let new_head = new_blocks.back().unwrap().0.clone();

        let metadata_chain = dataset.as_metadata_chain();

        for (hash, block) in new_blocks {
            tracing::debug!(sequence_numer = %block.sequence_number, hash = %hash, "Appending block");

            metadata_chain
                .append(
                    block,
                    odf::dataset::AppendOpts {
                        update_ref: None,
                        expected_hash: Some(&hash),
                        ..odf::dataset::AppendOpts::default()
                    },
                )
                .await?;
        }

        metadata_chain
            .set_ref(
                &odf::BlockRef::Head,
                &new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: if force_update_if_diverged {
                        None
                    } else {
                        Some(old_head.as_ref())
                    },
                },
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
