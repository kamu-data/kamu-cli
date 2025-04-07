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
use kamu_datasets::DatasetKeyBlockRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainDatabaseBackedImpl<TMetadataChain>
where
    TMetadataChain: odf::MetadataChain + Send + Sync,
{
    dataset_id: odf::DatasetID,
    metadata_chain: TMetadataChain,
    state: RwLock<State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct State {
    /// Cached key blocks with hashes
    cached_key_blocks: Vec<(odf::Multihash, odf::MetadataBlock)>,

    /// A detachable key block repository component
    maybe_dataset_key_block_repo: Option<Arc<dyn DatasetKeyBlockRepository>>,
}

impl State {
    fn new(dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>) -> Self {
        Self {
            cached_key_blocks: Vec::new(),
            maybe_dataset_key_block_repo: Some(dataset_key_block_repo),
        }
    }

    fn get_cached_key_blocks_for_range(
        &self,
        (min_boundary, max_boundary): (u64, u64),
    ) -> Option<&[(odf::Multihash, odf::MetadataBlock)]> {
        // Ensure correct boundary
        assert!(min_boundary < max_boundary);

        // No blocks yet?
        if self.cached_key_blocks.is_empty() {
            return None;
        }

        // Find the first block that is greater than or equal to the min boundary
        let start_index = self
            .cached_key_blocks
            .binary_search_by_key(&min_boundary, |(_, block)| block.sequence_number)
            .unwrap_or_else(|x| x);

        // Use the start_index to reduce the search space for the max boundary
        let end_index = self.cached_key_blocks[start_index..]
            .binary_search_by_key(&max_boundary, |(_, block)| block.sequence_number)
            .unwrap_or_else(|x| x + start_index);

        // The slice is [start_index, end_index)
        //   [ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9 ]
        //           min=2                  max=8
        //             ^ start_index           |
        //                                     ^ end_index
        Some(&self.cached_key_blocks[start_index..end_index])
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TMetadataChain> MetadataChainDatabaseBackedImpl<TMetadataChain>
where
    TMetadataChain: odf::MetadataChain + Send + Sync,
{
    pub fn new(
        dataset_id: odf::DatasetID,
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
        metadata_chain: TMetadataChain,
    ) -> Self {
        Self {
            dataset_id,
            metadata_chain,
            state: RwLock::new(State::new(dataset_key_block_repo)),
        }
    }

    fn try_select_prev_block_satisfying_hints(
        blocks: &[(odf::Multihash, odf::MetadataBlock)],
        hint_flags: odf::metadata::MetadataEventTypeFlags,
    ) -> Option<(odf::Multihash, odf::MetadataBlock)> {
        blocks
            .iter()
            .rev()
            .find(|(_, block)| {
                odf::metadata::MetadataEventTypeFlags::from(&block.event).intersects(hint_flags)
            })
            .cloned()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<TMetadataChain> odf::MetadataChain for MetadataChainDatabaseBackedImpl<TMetadataChain>
where
    TMetadataChain: odf::MetadataChain + Send + Sync,
{
    async fn contains_block(
        &self,
        hash: &odf::Multihash,
    ) -> Result<bool, odf::storage::ContainsBlockError> {
        self.metadata_chain.contains_block(hash).await
    }

    async fn get_block_size(
        &self,
        hash: &odf::Multihash,
    ) -> Result<u64, odf::storage::GetBlockDataError> {
        self.metadata_chain.get_block_size(hash).await
    }

    async fn get_block_bytes(
        &self,
        hash: &odf::Multihash,
    ) -> Result<bytes::Bytes, odf::storage::GetBlockDataError> {
        self.metadata_chain.get_block_bytes(hash).await
    }

    async fn get_block(
        &self,
        hash: &odf::Multihash,
    ) -> Result<odf::MetadataBlock, odf::storage::GetBlockError> {
        self.metadata_chain.get_block(hash).await
    }

    async fn append<'a>(
        &'a self,
        block: odf::MetadataBlock,
        opts: odf::dataset::AppendOpts<'a>,
    ) -> Result<odf::Multihash, odf::dataset::AppendError> {
        self.metadata_chain.append(block, opts).await
    }

    async fn resolve_ref(
        &self,
        r: &odf::BlockRef,
    ) -> Result<odf::Multihash, odf::storage::GetRefError> {
        self.metadata_chain.resolve_ref(r).await
    }

    async fn set_ref<'a>(
        &'a self,
        r: &odf::BlockRef,
        hash: &odf::Multihash,
        opts: odf::dataset::SetRefOpts<'a>,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        self.metadata_chain.set_ref(r, hash, opts).await
    }

    fn as_uncached_ref_repo(&self) -> &dyn odf::storage::ReferenceRepository {
        self.metadata_chain.as_uncached_ref_repo()
    }

    fn detach_from_transaction(&self) {
        // Pass over to the next level chain
        self.metadata_chain.detach_from_transaction();

        // Detach the repository, as it's holding the transaction
        let mut write_guard = self.state.write().unwrap();
        write_guard.maybe_dataset_key_block_repo = None;
    }

    async fn get_preceding_block_with_hint(
        &self,
        head_block: &odf::MetadataBlock,
        tail_sequence_number: Option<u64>,
        hint: odf::dataset::MetadataVisitorDecision,
    ) -> Result<Option<(odf::Multihash, odf::MetadataBlock)>, odf::storage::GetBlockError> {
        // Guard against stopped hint
        assert!(hint != odf::dataset::MetadataVisitorDecision::Stop);

        // Have we reached the tail? (if specified the boundary, otherwise Seed=0)
        let tail_sequence_number = tail_sequence_number.unwrap_or_default();
        if tail_sequence_number >= head_block.sequence_number {
            // We are at the tail, no need to go further
            return Ok(None);
        }

        // Is there a previous block in general?
        let Some(prev_block_hash) = &head_block.prev_block_hash else {
            return Ok(None);
        };

        // Yes. If we are looking for key blocks only, we can use the cache
        if let odf::dataset::MetadataVisitorDecision::NextOfType(hint_flags) = hint
            && !hint_flags.has_data_flags()
        {
            // This is the working range of our iteration [min, max).
            // We should not return blocks outside of this range.
            let requested_boundary = (tail_sequence_number, head_block.sequence_number);

            // Trace the decision
            tracing::trace!(
                dataset_id=%self.dataset_id,
                requested_boundary=?requested_boundary,
                hint_flags=?hint_flags,
                "Expecting key blocks only, looking for quick answer"
            );

            // First, try to read the ready answer from the cache
            let maybe_key_block_repository = {
                let read_guard = self.state.read().unwrap();
                if let Some(cached_blocks_in_range) =
                    read_guard.get_cached_key_blocks_for_range(requested_boundary)
                {
                    // Report cache hit
                    tracing::trace!(
                        dataset_id=%self.dataset_id,
                        num_blocks = cached_blocks_in_range.len(),
                        "Found key blocks in the cache"
                    );

                    // We have cached blocks in the requested range
                    // Filter them by the requested flags, starting from the last one.
                    // Note that if there is no cached block matching the hints,
                    // it means we can stop searching in general
                    return Ok(Self::try_select_prev_block_satisfying_hints(
                        cached_blocks_in_range,
                        hint_flags,
                    ));
                }

                // If we are here, the cache is empty or does not contain the requested range
                // Check if the repository is attached
                read_guard.maybe_dataset_key_block_repo.clone()
            };

            // At this point we must ensure the cache could be still filled
            if let Some(dataset_key_blocks_repo) = maybe_key_block_repository {
                // Read all key blocks of this dataset
                // No worries, there usually are not many of those, we can read entirely
                let key_block_records = dataset_key_blocks_repo
                    .get_all_key_blocks(&self.dataset_id, &odf::BlockRef::Head)
                    .await
                    .int_err()?;

                // Convert the key blocks to the metadata blocks
                let key_blocks = key_block_records
                    .into_iter()
                    .map(|key_block| {
                        odf::storage::deserialize_metadata_block(
                            &key_block.block_hash,
                            &key_block.block_payload,
                        )
                        .int_err()
                        .map(|metadata_block| (key_block.block_hash, metadata_block))
                    })
                    .collect::<Result<Vec<_>, InternalError>>()?;

                // Cache the key blocks
                let mut write_guard = self.state.write().unwrap();
                write_guard.cached_key_blocks = key_blocks;

                // Report cache miss
                tracing::trace!(
                    dataset_id=%self.dataset_id,
                    num_blocks = write_guard.cached_key_blocks.len(),
                    "No key blocks cached. Loaded key blocks from the repository"
                );

                // Pick the matching slice for the requested range
                if let Some(cached_blocks_in_range) =
                    write_guard.get_cached_key_blocks_for_range(requested_boundary)
                {
                    // Filter them by the requested flags, starting from the last one.
                    // Note that if there is no cached block matching the hints,
                    // it means we can stop searching in general
                    return Ok(Self::try_select_prev_block_satisfying_hints(
                        cached_blocks_in_range,
                        hint_flags,
                    ));
                }

                // Trace confidently. At least Seed exists in every dataset.
                // If the result is empty, there was no indexing yet
                tracing::warn!(dataset_id=%self.dataset_id, "Key blocks of the dataset are not indexed");
            } else {
                // The repository is detached. We cannot provide any quick answers,
                //  so fall back to the linear iteration
                tracing::warn!(dataset_id=%self.dataset_id, "Key blocks repository is detached. Cannot read key blocks");
            }
        }

        // Read the previous block without jumps
        let block = self.metadata_chain.get_block(prev_block_hash).await?;
        Ok(Some((prev_block_hash.clone(), block)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
