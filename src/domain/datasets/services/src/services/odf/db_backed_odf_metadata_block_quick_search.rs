// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, RwLock};

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{DatasetKeyBlockRepository, MetadataEventType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedOdfMetadataBlockQuickSearch {
    dataset_id: odf::DatasetID,
    state: RwLock<State>,
}

struct State {
    maybe_dataset_key_blocks_repo: Option<Arc<dyn DatasetKeyBlockRepository>>,
}

impl State {
    fn new(maybe_dataset_key_blocks_repo: Option<Arc<dyn DatasetKeyBlockRepository>>) -> Self {
        Self {
            maybe_dataset_key_blocks_repo,
        }
    }
}

impl DatabaseBackedOdfMetadataBlockQuickSearch {
    pub fn new(
        dataset_id: odf::DatasetID,
        dataset_key_blocks_repo: Arc<dyn DatasetKeyBlockRepository>,
    ) -> Self {
        Self {
            dataset_id,
            state: RwLock::new(State::new(Some(dataset_key_blocks_repo))),
        }
    }
}

#[async_trait::async_trait]
impl odf::dataset::MetadataChainBlockQuickSearch for DatabaseBackedOdfMetadataBlockQuickSearch {
    /// Detaches this metadata chain from any transaction references
    fn detach_from_transaction(&self) {
        let mut write_guard = self.state.write().unwrap();
        write_guard.maybe_dataset_key_blocks_repo = None;
    }

    async fn quick_search_of_blocks(
        &self,
        flags: odf::metadata::MetadataEventTypeFlags,
        head_block: &odf::MetadataBlock,
        maybe_tail_block: Option<&odf::MetadataBlock>,
    ) -> Result<Vec<(odf::Multihash, odf::MetadataBlock)>, InternalError> {
        // No quick answers if repository is detached
        let dataset_key_blocks_repo = {
            let guard = self.state.read().unwrap();
            match &guard.maybe_dataset_key_blocks_repo {
                Some(ref_repo) => ref_repo.clone(),
                None => return Ok(vec![]),
            }
        };

        // Form a list of metadata event types to search for
        // Exclude non-key event types from flags
        let key_flags = flags
            & !(
                odf::metadata::MetadataEventTypeFlags::ADD_DATA
                    | odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM
                    | odf::metadata::MetadataEventTypeFlags::ADD_PUSH_SOURCE
                // TODO: remove ADD_PUSH_SOURCE, it's a workaround
            );
        let kinds = MetadataEventType::multiple_from_metadata_event_flags(key_flags);

        // Request key blocks from the repository
        let key_blocks = dataset_key_blocks_repo
            .find_latest_blocks_of_kinds_in_range(
                &self.dataset_id,
                &odf::BlockRef::Head,
                &kinds,
                maybe_tail_block.map(|tail_block| tail_block.sequence_number),
                head_block.sequence_number,
            )
            .await
            .int_err()?;

        // Map the key blocks to the result format
        let mut result = Vec::with_capacity(key_blocks.len());
        for key_block in key_blocks {
            let block = odf::storage::deserialize_metadata_block(
                &key_block.block_hash,
                &key_block.block_payload,
            )
            .int_err()?;

            result.push((key_block.block_hash, block));
        }

        // Success
        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
