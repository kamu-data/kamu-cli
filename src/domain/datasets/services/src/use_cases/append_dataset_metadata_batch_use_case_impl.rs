// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, Catalog};
use internal_error::ResultIntoInternal;
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseBlockAppendError,
    AppendDatasetMetadataBatchUseCaseError,
    AppendDatasetMetadataBatchUseCaseOptions,
    SetRefCheckRefMode,
};

use crate::DependencyGraphWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AppendDatasetMetadataBatchUseCaseImpl {
    catalog: Catalog,
    dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
}

#[component(pub)]
#[interface(dyn AppendDatasetMetadataBatchUseCase)]
impl AppendDatasetMetadataBatchUseCaseImpl {
    pub fn new(catalog: Catalog, dependency_graph_writer: Arc<dyn DependencyGraphWriter>) -> Self {
        Self {
            catalog,
            dependency_graph_writer,
        }
    }

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

        let mut new_upstream_ids: Vec<odf::DatasetID> = vec![];
        let mut dependencies_modified = false;

        let mut new_head = None;

        while let Some((hash, block)) = new_blocks_it.next() {
            tracing::debug!(sequence_numer = %block.sequence_number, hash = %hash, "Appending block");

            if let odf::MetadataEvent::SetTransform(transform) = &block.event {
                // Collect only the latest upstream dataset IDs
                dependencies_modified = true;
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

        // Note: modify dependencies only after `set_ref` succeeds.
        // TODO: the dependencies should be updated as a part of HEAD change
        if dependencies_modified {
            let summary = dataset
                .get_summary(odf::dataset::GetSummaryOpts::default())
                .await
                .int_err()?;

            self.dependency_graph_writer
                .update_dataset_node_dependencies(&self.catalog, &summary.id, new_upstream_ids)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
