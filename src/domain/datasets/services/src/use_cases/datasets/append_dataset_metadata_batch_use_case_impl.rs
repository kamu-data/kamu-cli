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
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseError,
    AppendDatasetMetadataBatchUseCaseOptions,
    DatasetHeadCommitter,
    ResolvedDataset,
};
use odf::dataset::MetadataChainExt as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AppendDatasetMetadataBatchUseCaseImpl {
    dataset_head_committer: Arc<dyn DatasetHeadCommitter>,
}

#[component(pub)]
#[interface(dyn AppendDatasetMetadataBatchUseCase)]
impl AppendDatasetMetadataBatchUseCaseImpl {
    pub fn new(dataset_head_committer: Arc<dyn DatasetHeadCommitter>) -> Self {
        Self {
            dataset_head_committer,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl AppendDatasetMetadataBatchUseCase for AppendDatasetMetadataBatchUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = AppendDatasetMetadataBatchUseCaseImpl_execute,
        skip_all,
        fields(dataset_id = %target.get_id(), ?new_blocks_len = new_blocks_into_it.size_hint())
    )]
    async fn execute(
        &self,
        target: &ResolvedDataset,
        new_blocks_into_it: Box<
            dyn Iterator<Item = odf::dataset::HashedMetadataBlockBytesDecoded> + Send,
        >,
        options: AppendDatasetMetadataBatchUseCaseOptions,
    ) -> Result<Option<odf::Multihash>, AppendDatasetMetadataBatchUseCaseError> {
        let Some(appended) = target
            .as_metadata_chain()
            .append_batch(new_blocks_into_it, options.append_opts)
            .await?
        else {
            return Ok(None);
        };

        let expected_head = options
            .set_ref_check_ref_mode
            .expected_head(appended.base_head);

        self.dataset_head_committer
            .commit_head(target, expected_head.as_ref(), &appended.new_head)
            .await?;

        Ok(Some(appended.new_head))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
