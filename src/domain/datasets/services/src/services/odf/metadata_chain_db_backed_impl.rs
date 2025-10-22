// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{DatasetDataBlockRepository, DatasetKeyBlockRepository};

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
    /// Key blocks
    cached_key_blocks: CachedBlocksRange,

    /// Data blocks
    cached_data_blocks: CachedBlocksRange,

    /// A detachable key block repository component
    maybe_dataset_key_block_repo: Option<Arc<dyn DatasetKeyBlockRepository>>,

    /// A detachable data block repository component
    maybe_dataset_data_block_repo: Option<Arc<dyn DatasetDataBlockRepository>>,
}

impl State {
    fn new(
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
        dataset_data_block_repo: Arc<dyn DatasetDataBlockRepository>,
    ) -> Self {
        Self {
            cached_key_blocks: CachedBlocksRange::new(),
            cached_data_blocks: CachedBlocksRange::new(),
            maybe_dataset_key_block_repo: Some(dataset_key_block_repo),
            maybe_dataset_data_block_repo: Some(dataset_data_block_repo),
        }
    }

    fn get_cached_key_blocks_for_range(
        &self,
        (min_boundary, max_boundary): (u64, u64),
    ) -> Option<&[(odf::Multihash, odf::MetadataBlock)]> {
        self.cached_key_blocks
            .get_cached_blocks_for_range((min_boundary, max_boundary))
    }

    fn get_cached_data_blocks_for_range(
        &self,
        (min_boundary, max_boundary): (u64, u64),
    ) -> Option<&[(odf::Multihash, odf::MetadataBlock)]> {
        self.cached_data_blocks
            .get_cached_blocks_for_range((min_boundary, max_boundary))
    }

    fn summary(&self) -> CachedStateSummary {
        CachedStateSummary {
            key_blocks_cached: self.cached_key_blocks.len() > 0,
            data_blocks_cached: self.cached_data_blocks.len() > 0,
            maybe_key_blocks_repo: self.maybe_dataset_key_block_repo.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CachedStateSummary {
    key_blocks_cached: bool,
    data_blocks_cached: bool,
    maybe_key_blocks_repo: Option<Arc<dyn DatasetKeyBlockRepository>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CachedBlocksRange {
    /// Cached blocks with hashes in chronological order
    blocks: Vec<(odf::Multihash, odf::MetadataBlock)>,

    /// Original block payloads, in the same order as in `blocks`
    original_block_payloads: Vec<bytes::Bytes>,

    /// The same info in lookup-friendly form:
    ///  index in `blocks` by block hash
    blocks_lookup: HashMap<odf::Multihash, usize>,
}

impl CachedBlocksRange {
    fn new() -> Self {
        Self {
            blocks: Vec::new(),
            original_block_payloads: Vec::new(),
            blocks_lookup: HashMap::new(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.blocks.len()
    }

    fn contains_block(&self, hash: &odf::Multihash) -> bool {
        self.blocks_lookup.contains_key(hash)
    }

    fn try_get_block(&self, hash: &odf::Multihash) -> Option<odf::MetadataBlock> {
        self.blocks_lookup
            .get(hash)
            .map(|&idx| self.blocks[idx].1.clone())
    }

    fn try_get_block_size(&self, hash: &odf::Multihash) -> Option<u64> {
        self.blocks_lookup
            .get(hash)
            .map(|&idx| self.original_block_payloads[idx].len() as u64)
    }

    fn try_get_block_bytes(&self, hash: &odf::Multihash) -> Option<bytes::Bytes> {
        self.blocks_lookup
            .get(hash)
            .map(|&idx| self.original_block_payloads[idx].clone())
    }

    fn cache_blocks(&mut self, blocks: Vec<(odf::Multihash, bytes::Bytes, odf::MetadataBlock)>) {
        self.blocks = Vec::with_capacity(blocks.len());
        self.original_block_payloads = Vec::with_capacity(blocks.len());

        for (hash, payload, block) in blocks {
            self.blocks.push((hash, block));
            self.original_block_payloads.push(payload);
        }

        self.blocks_lookup = self
            .blocks
            .iter()
            .enumerate()
            .map(|(idx, (hash, _))| (hash.clone(), idx))
            .collect();
    }

    fn append_block(&mut self, hash: &odf::Multihash, block: &odf::MetadataBlock) {
        let payload = bytes::Bytes::from(odf::storage::serialize_metadata_block(block).unwrap());

        self.blocks_lookup.insert(hash.clone(), self.blocks.len());
        self.blocks.push((hash.clone(), block.clone()));
        self.original_block_payloads.push(payload);
    }

    fn get_cached_blocks_for_range(
        &self,
        (min_boundary, max_boundary): (u64, u64),
    ) -> Option<&[(odf::Multihash, odf::MetadataBlock)]> {
        // Ensure correct boundary
        assert!(min_boundary < max_boundary);

        // No blocks yet?
        if self.blocks.is_empty() {
            return None;
        }

        // Find the first block that is greater than or equal to the min boundary
        let start_index = self
            .blocks
            .binary_search_by_key(&min_boundary, |(_, block)| block.sequence_number)
            .unwrap_or_else(|x| x);

        // Use the start_index to reduce the search space for the max boundary
        let end_index = self.blocks[start_index..]
            .binary_search_by_key(&max_boundary, |(_, block)| block.sequence_number)
            .map(|x| x + start_index)
            .unwrap_or_else(|x| x + start_index);

        // The slice is [start_index, end_index)
        //   [ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9 ]
        //           min=2                  max=8
        //             ^ start_index           |
        //                                     ^ end_index
        Some(&self.blocks[start_index..end_index])
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
        dataset_data_block_repo: Arc<dyn DatasetDataBlockRepository>,
        metadata_chain: TMetadataChain,
    ) -> Self {
        Self {
            dataset_id,
            metadata_chain,
            state: RwLock::new(State::new(dataset_key_block_repo, dataset_data_block_repo)),
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

    /// Generic helper method for block operations that follow the same pattern:
    ///  - check existing caches for keys and data blocks first
    ///  - if missing, and key blocks cache is not yet loaded, attempt loading
    ///  - don't attempt loading data blocks into cache, this is expensive
    ///  - if still not found, fall back to the underlying chain operation
    async fn get_from_cache_or_fallback<T, E, F, G>(
        &self,
        hash: &odf::Multihash,
        cache_lookup: F,
        fallback_operation: G,
    ) -> Result<T, E>
    where
        F: Fn(&CachedBlocksRange, &odf::Multihash) -> Option<T>,
        G: std::future::Future<Output = Result<T, E>>,
        E: From<InternalError>,
    {
        // First, try to find the block in the cache - keys or data
        let cache_summary = {
            // Summarize state of the cache
            let read_guard = self.state.read().unwrap();
            let summary = read_guard.summary();

            // Check key blocks cache
            if summary.key_blocks_cached
                && let Some(result) = cache_lookup(&read_guard.cached_key_blocks, hash)
            {
                return Ok(result);
            }

            // Check data blocks cache
            if summary.data_blocks_cached
                && let Some(result) = cache_lookup(&read_guard.cached_data_blocks, hash)
            {
                return Ok(result);
            }

            // No answer in caches yet
            summary
        };

        // If we are here, the block is not in the cache
        // Maybe there is no key block cache at all yet?
        if !cache_summary.key_blocks_cached {
            // Attempt loading key blocks into cache
            if let Some(dataset_key_blocks_repo) = cache_summary.maybe_key_blocks_repo {
                self.try_load_key_blocks_into_cache(dataset_key_blocks_repo.as_ref())
                    .await?;
            }

            // Check again in the cache - just the key blocks
            let read_guard = self.state.read().unwrap();
            if let Some(result) = cache_lookup(&read_guard.cached_key_blocks, hash) {
                return Ok(result);
            }
        }

        // Do not attempt loading data blocks into cache - this is expensive

        // Fall back to the underlying operation in the raw chain
        fallback_operation.await
    }

    async fn try_load_key_blocks_into_cache(
        &self,
        key_block_repository: &dyn DatasetKeyBlockRepository,
    ) -> Result<(), InternalError> {
        // Read all key blocks of this dataset
        // No worries, there usually are not many of those, we can read entirely
        let key_block_records = key_block_repository
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
                .map(|metadata_block| {
                    (
                        key_block.block_hash,
                        key_block.block_payload,
                        metadata_block,
                    )
                })
                .int_err()
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        // Cache the key blocks
        let mut write_guard = self.state.write().unwrap();
        assert!(write_guard.cached_key_blocks.len() == 0);
        write_guard.cached_key_blocks.cache_blocks(key_blocks);

        // Report cache miss
        tracing::trace!(
            dataset_id=%self.dataset_id,
            num_blocks = write_guard.cached_key_blocks.len(),
            "No key blocks cached. Loaded key blocks from the repository"
        );
        Ok(())
    }

    async fn get_key_block_with_hint(
        &self,
        requested_boundary: (u64, u64),
        hint_flags: odf::metadata::MetadataEventTypeFlags,
    ) -> Result<BlockLookupResult, odf::storage::GetBlockError> {
        // First, try to read the ready answer from the cache
        let cache_summary = {
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
                if let Some(found_block_hint) =
                    Self::try_select_prev_block_satisfying_hints(cached_blocks_in_range, hint_flags)
                {
                    return Ok(BlockLookupResult::Found(found_block_hint));
                }

                // Note that if there is no cached block matching the hints,
                // it means we can stop searching in general
                return Ok(BlockLookupResult::Stop);
            }

            // If we are here, the cache is empty or does not contain the requested range
            read_guard.summary()
        };

        // At this point we must ensure the cache could be still filled
        if !cache_summary.key_blocks_cached
            && let Some(dataset_key_blocks_repo) = cache_summary.maybe_key_blocks_repo
        {
            // Attempt loading cache
            self.try_load_key_blocks_into_cache(dataset_key_blocks_repo.as_ref())
                .await?;

            // Pick the matching slice for the requested range
            let read_guard = self.state.read().unwrap();
            if let Some(cached_blocks_in_range) =
                read_guard.get_cached_key_blocks_for_range(requested_boundary)
            {
                // Filter them by the requested flags, starting from the last one.
                if let Some(found_block_hint) =
                    Self::try_select_prev_block_satisfying_hints(cached_blocks_in_range, hint_flags)
                {
                    return Ok(BlockLookupResult::Found(found_block_hint));
                }

                // Note that if there is no cached block matching the hints,
                // it means we can stop searching in general
                return Ok(BlockLookupResult::Stop);
            }

            // Trace confidently. At least Seed exists in every dataset.
            // If the result is empty, there was no indexing yet
            tracing::warn!(dataset_id=%self.dataset_id, "Key blocks of the dataset are not indexed");
        } else {
            // The repository is detached. We cannot provide any quick answers,
            //  so fall back to the linear iteration
            tracing::warn!(dataset_id=%self.dataset_id, "Key blocks repository is detached. Cannot read key blocks");
        }

        Ok(BlockLookupResult::NotFound)
    }

    async fn get_data_block_with_hint(
        &self,
        requested_boundary: (u64, u64),
        hint_flags: odf::metadata::MetadataEventTypeFlags,
    ) -> Result<BlockLookupResult, odf::storage::GetBlockError> {
        // First, try to read the ready answer from the cache
        let maybe_data_block_repository = {
            let read_guard = self.state.read().unwrap();
            if let Some(cached_blocks_in_range) =
                read_guard.get_cached_data_blocks_for_range(requested_boundary)
            {
                // Report cache hit
                tracing::trace!(
                    dataset_id=%self.dataset_id,
                    num_blocks = cached_blocks_in_range.len(),
                    "Found data blocks in the cache"
                );

                // We have cached blocks in the requested range
                // Filter them by the requested flags, starting from the last one.
                if let Some(found_block_hint) =
                    Self::try_select_prev_block_satisfying_hints(cached_blocks_in_range, hint_flags)
                {
                    return Ok(BlockLookupResult::Found(found_block_hint));
                }

                // Note that if there is no cached block matching the hints, this does not mean
                // anything, unlike key blocks. There might be still data blocks
                // in the earlier pages, we don't know that for sure.
                return Ok(BlockLookupResult::NotFound);
            }

            // If we are here, the cache is empty or does not contain the requested range
            // Check if the repository is attached
            read_guard.maybe_dataset_data_block_repo.clone()
        };

        // At this point we must ensure the cache could be still filled
        if let Some(dataset_data_block_repo) = maybe_data_block_repository {
            // Temporary: read all data blocks of this dataset
            // TODO: read in pages, not all at once
            let data_block_records = dataset_data_block_repo
                .get_all_data_blocks(&self.dataset_id, &odf::BlockRef::Head)
                .await
                .int_err()?;

            // Convert the data blocks to the metadata blocks
            let data_blocks = data_block_records
                .into_iter()
                .map(|data_block| {
                    odf::storage::deserialize_metadata_block(
                        &data_block.block_hash,
                        &data_block.block_payload,
                    )
                    .map(|metadata_block| {
                        (
                            data_block.block_hash,
                            data_block.block_payload,
                            metadata_block,
                        )
                    })
                    .int_err()
                })
                .collect::<Result<Vec<_>, InternalError>>()?;

            // Cache the data blocks
            let mut write_guard = self.state.write().unwrap();
            write_guard.cached_data_blocks.cache_blocks(data_blocks);

            // Report cache miss
            tracing::trace!(
                dataset_id=%self.dataset_id,
                num_blocks = write_guard.cached_data_blocks.len(),
                "No data blocks cached. Loaded data blocks from the repository"
            );

            // Pick the matching slice for the requested range
            if let Some(cached_blocks_in_range) =
                write_guard.get_cached_data_blocks_for_range(requested_boundary)
            {
                // Filter them by the requested flags, starting from the last one.
                if let Some(found_block_hint) =
                    Self::try_select_prev_block_satisfying_hints(cached_blocks_in_range, hint_flags)
                {
                    return Ok(BlockLookupResult::Found(found_block_hint));
                }

                // Note that if there is no cached block matching the hints, this does not mean
                // anything, unlike key blocks. There might be still data blocks
                // in the earlier pages, we don't know that for sure.
                return Ok(BlockLookupResult::NotFound);
            }

            // We don't know if there are data blocks at all or they haven't
            // been indexed yet
        } else {
            // The repository is detached. We cannot provide any quick answers,
            //  so fall back to the linear iteration
            tracing::warn!(dataset_id=%self.dataset_id, "Data blocks repository is detached. Cannot read data blocks");
        }

        Ok(BlockLookupResult::NotFound)
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
        self.get_from_cache_or_fallback(
            hash,
            |cache, hash| {
                if cache.contains_block(hash) {
                    Some(true)
                } else {
                    None
                }
            },
            self.metadata_chain.contains_block(hash),
        )
        .await
    }

    async fn get_block_size(
        &self,
        hash: &odf::Multihash,
    ) -> Result<u64, odf::storage::GetBlockDataError> {
        self.get_from_cache_or_fallback(
            hash,
            CachedBlocksRange::try_get_block_size,
            self.metadata_chain.get_block_size(hash),
        )
        .await
    }

    async fn get_block_bytes(
        &self,
        hash: &odf::Multihash,
    ) -> Result<bytes::Bytes, odf::storage::GetBlockDataError> {
        self.get_from_cache_or_fallback(
            hash,
            CachedBlocksRange::try_get_block_bytes,
            self.metadata_chain.get_block_bytes(hash),
        )
        .await
    }

    async fn get_block(
        &self,
        hash: &odf::Multihash,
    ) -> Result<odf::MetadataBlock, odf::storage::GetBlockError> {
        self.get_from_cache_or_fallback(
            hash,
            CachedBlocksRange::try_get_block,
            self.metadata_chain.get_block(hash),
        )
        .await
    }

    async fn append<'a>(
        &'a self,
        block: odf::MetadataBlock,
        opts: odf::dataset::AppendOpts<'a>,
    ) -> Result<odf::Multihash, odf::dataset::AppendError> {
        // Determine if we have any caches loaded
        let cache_summary = {
            let read_guard = self.state.read().unwrap();
            read_guard.summary()
        };

        // Classify the block type
        let block_flags = odf::metadata::MetadataEventTypeFlags::from(&block.event);

        // Key block and key blocks are cached?
        if block_flags.has_key_block_flags() && cache_summary.key_blocks_cached {
            // Write the block to the underlying chain
            let hash = self.metadata_chain.append(block.clone(), opts).await?;

            // Update the cache of key blocks
            let mut write_guard = self.state.write().unwrap();
            write_guard.cached_key_blocks.append_block(&hash, &block);

            // Resulting hash
            Ok(hash)

        // Data block and data blocks are cached?
        } else if block_flags.has_data_flags() && cache_summary.data_blocks_cached {
            // Write the block to the underlying chain
            let hash = self.metadata_chain.append(block.clone(), opts).await?;

            // Update the cache of data blocks
            let mut write_guard = self.state.write().unwrap();
            write_guard.cached_data_blocks.append_block(&hash, &block);

            // Resulting hash
            Ok(hash)
        } else {
            // No caches to update, just append to the underlying chain
            self.metadata_chain.append(block, opts).await
        }
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

        // Detach the repositories, as they are holding the transaction
        let mut write_guard = self.state.write().unwrap();
        write_guard.maybe_dataset_key_block_repo = None;
        write_guard.maybe_dataset_data_block_repo = None;
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

        // This is the working range of our iteration [min, max).
        // We should not return blocks outside of this range.
        let requested_boundary = (tail_sequence_number, head_block.sequence_number);

        // If we are looking for specific block types, try to use the caches first
        if let odf::dataset::MetadataVisitorDecision::NextOfType(hint_flags) = hint {
            // Try key blocks from cache, if flags expect key nodes
            let key_block_result = if hint_flags.has_key_block_flags() {
                self.get_key_block_with_hint(requested_boundary, hint_flags)
                    .await?
            } else {
                BlockLookupResult::Stop
            };

            // Try data blocks from cache, if flags expect data nodes
            let data_block_result = if hint_flags.has_data_flags() {
                self.get_data_block_with_hint(requested_boundary, hint_flags)
                    .await?
            } else {
                BlockLookupResult::Stop
            };

            // Decide which block to return, if any
            match (key_block_result, data_block_result) {
                (
                    BlockLookupResult::Found(key_block_hint),
                    BlockLookupResult::Found(data_block_hint),
                ) => {
                    // Both key and data blocks are available, pick the one with the higher sequence
                    // number
                    if key_block_hint.1.sequence_number > data_block_hint.1.sequence_number {
                        return Ok(Some(key_block_hint));
                    }
                    return Ok(Some(data_block_hint));
                }
                (BlockLookupResult::Found(key_block_hint), _) => {
                    // Only key block is available
                    return Ok(Some(key_block_hint));
                }
                (_, BlockLookupResult::Found(data_block_hint)) => {
                    // Only data block is available
                    return Ok(Some(data_block_hint));
                }
                (BlockLookupResult::Stop, BlockLookupResult::Stop) => {
                    // Both algorithms decided to stop searching with confidence
                    return Ok(None);
                }
                (BlockLookupResult::NotFound, _) | (_, BlockLookupResult::NotFound) => {
                    // We could not find anything in the caches,
                    // similarly we don't have stop hints,
                    // so fall back to linear iteration
                }
            }
        }

        // Read the previous block without jumps, looks like caches are inaccessible
        let block = self.metadata_chain.get_block(prev_block_hash).await?;
        Ok(Some((prev_block_hash.clone(), block)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum BlockLookupResult {
    Found((odf::Multihash, odf::MetadataBlock)),
    NotFound,
    Stop,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
