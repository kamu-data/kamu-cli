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

use dill::{component, interface, Catalog};
use internal_error::ResultIntoInternal;
use kamu_datasets::AppendDatasetMetadataBatchUseCase;

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

        let mut new_upstream_ids: Vec<odf::DatasetID> = vec![];
        let mut dependencies_modified = false;

        for (hash, block) in new_blocks {
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
