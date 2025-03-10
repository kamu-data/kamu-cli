// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface};
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseBlockAppendError,
    AppendDatasetMetadataBatchUseCaseError,
    AppendDatasetMetadataBatchUseCaseOptions,
    SetRefCheckRefMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AppendDatasetMetadataBatchUseCase)]
pub struct AppendDatasetMetadataBatchUseCaseImpl {}

impl AppendDatasetMetadataBatchUseCaseImpl {
    fn append_block_options<'a>(
        block_hash: &'a odf::Multihash,
        options: &AppendDatasetMetadataBatchUseCaseOptions,
    ) -> odf::dataset::AppendOpts<'a> {
        let mut opts = odf::dataset::AppendOpts {
            update_ref: None,
            expected_hash: Some(block_hash),
            ..Default::default()
        };
        if let Some(trust_source_hashes) = options.trust_source_hashes {
            opts.precomputed_hash = if !trust_source_hashes {
                None
            } else {
                Some(block_hash)
            };
        }
        if let Some(validation) = options.append_validation {
            opts.validation = validation;
        }
        opts
    }

    fn set_ref_options<'a>(
        old_head: Option<&'a odf::Multihash>,
        options: &'a AppendDatasetMetadataBatchUseCaseOptions,
    ) -> odf::dataset::SetRefOpts<'a> {
        let mut opts = odf::dataset::SetRefOpts {
            validate_block_present: false,
            ..Default::default()
        };
        if let Some(set_ref_check_ref_mode) = &options.set_ref_check_ref_mode {
            opts.check_ref_is = match set_ref_check_ref_mode {
                SetRefCheckRefMode::ForceUpdateIfDiverged(force_update_if_diverged) => {
                    if *force_update_if_diverged {
                        None
                    } else {
                        Some(old_head)
                    }
                }
                SetRefCheckRefMode::Explicit(head) => Some(head.as_ref()),
            };
        }
        opts
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl AppendDatasetMetadataBatchUseCase for AppendDatasetMetadataBatchUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = AppendDatasetMetadataBatchUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, ?new_blocks_len = new_blocks_into_it.size_hint(), options)
    )]
    async fn execute(
        &self,
        dataset: &dyn odf::Dataset,
        new_blocks_into_it: Box<dyn Iterator<Item = odf::dataset::HashedMetadataBlock> + Send>,
        options: AppendDatasetMetadataBatchUseCaseOptions,
    ) -> Result<(), AppendDatasetMetadataBatchUseCaseError> {
        let mut new_blocks_it = new_blocks_into_it.into_iter().peekable();

        let old_head = if let Some(first_block) = new_blocks_it.peek() {
            first_block.1.prev_block_hash.clone()
        } else {
            // No blocks
            return Ok(());
        };

        let metadata_chain = dataset.as_metadata_chain();

        let mut new_head = None;

        while let Some((hash, block)) = new_blocks_it.next() {
            tracing::debug!(sequence_numer = %block.sequence_number, hash = %hash, "Appending block");

            let block_sequence_number = block.sequence_number;
            if let Err(append_error) = metadata_chain
                .append(block, Self::append_block_options(&hash, &options))
                .await
            {
                return Err(AppendDatasetMetadataBatchUseCaseBlockAppendError {
                    append_error,
                    block_sequence_number,
                    block_hash: hash,
                }
                .into());
            }

            let is_last_block = new_blocks_it.peek().is_none();
            if is_last_block {
                new_head = Some(hash);
            }
        }

        // Safety: there are blocks, so we are guaranteed to have
        //         a hash of the last one.
        let new_head = new_head.as_ref().unwrap();
        metadata_chain
            .set_ref(
                &odf::BlockRef::Head,
                new_head,
                Self::set_ref_options(old_head.as_ref(), &options),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
