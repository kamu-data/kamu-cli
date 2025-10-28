// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, RwLock};

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::{DatasetDataBlockRepository, DatasetKeyBlockRepository};

use super::cached_blocks_range::{BlockLookupResult, CachedBlocksRange};
use super::merge_iterator::{CachedBlocksMergeIterator, load_data_blocks_from_repository};
use crate::MetadataChainDbBackedConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MetadataChainDatabaseBackedImpl<TMetadataChain>
where
    TMetadataChain: odf::MetadataChain + Send + Sync,
{
    // --- immutable fields
    config: MetadataChainDbBackedConfig,
    dataset_id: odf::DatasetID,
    metadata_chain: TMetadataChain,
    // --- mutable state
    state: RwLock<State>,
    key_blocks_loading_lock: tokio::sync::Mutex<()>,
    data_blocks_loading_lock: tokio::sync::Mutex<()>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct State {
    /// Key blocks
    cached_key_blocks: Option<Arc<CachedBlocksRange>>,

    /// Data blocks
    cached_data_blocks: Option<Arc<CachedBlocksRange>>,

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
            cached_key_blocks: None,
            cached_data_blocks: None,
            maybe_dataset_key_block_repo: Some(dataset_key_block_repo),
            maybe_dataset_data_block_repo: Some(dataset_data_block_repo),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TMetadataChain> MetadataChainDatabaseBackedImpl<TMetadataChain>
where
    TMetadataChain: odf::MetadataChain + Send + Sync,
{
    pub fn new(
        config: MetadataChainDbBackedConfig,
        dataset_id: odf::DatasetID,
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
        dataset_data_block_repo: Arc<dyn DatasetDataBlockRepository>,
        metadata_chain: TMetadataChain,
    ) -> Self {
        Self {
            config,
            dataset_id,
            metadata_chain,
            state: RwLock::new(State::new(dataset_key_block_repo, dataset_data_block_repo)),
            key_blocks_loading_lock: tokio::sync::Mutex::default(),
            data_blocks_loading_lock: tokio::sync::Mutex::default(),
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

    async fn ensure_key_blocks_are_preloaded(
        &self,
    ) -> Result<Option<Arc<CachedBlocksRange>>, InternalError> {
        // Try getting access to repository
        let maybe_key_block_repository = {
            // Ignore if already loaded
            let read_guard = self.state.read().unwrap();
            if let Some(cached_key_blocks) = read_guard.cached_key_blocks.as_ref() {
                return Ok(Some(cached_key_blocks.clone()));
            }

            // Not loaded. Try looking at repository
            read_guard.maybe_dataset_key_block_repo.clone()
        };

        // If there is a repository, we could try loading
        if let Some(key_block_repository) = maybe_key_block_repository {
            // Take loading lock
            let loading_guard = self.key_blocks_loading_lock.lock().await;

            // Try again, maybe another task has loaded key blocks already
            {
                // Ignore if already loaded
                let read_guard = self.state.read().unwrap();
                if let Some(cached_key_blocks) = read_guard.cached_key_blocks.as_ref() {
                    return Ok(Some(cached_key_blocks.clone()));
                }
            }

            // Read all key blocks of this dataset
            // No worries, there usually are not many of those, we can read entirely
            let key_block_records = key_block_repository
                .get_all_key_blocks(&self.dataset_id, &odf::BlockRef::Head)
                .await
                .int_err()?;

            if key_block_records.is_empty() {
                // At least Seed exists in every dataset.
                // If the result is empty, there was no indexing yet
                tracing::warn!(dataset_id=%self.dataset_id, "Key blocks of the dataset are not indexed");
                return Ok(None);
            }

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

            // Form resulting structure
            let cached_key_blocks = Arc::new(CachedBlocksRange::new(key_blocks));

            // Report cache fill operation
            tracing::trace!(
                dataset_id=%self.dataset_id,
                num_blocks = cached_key_blocks.len(),
                "No key blocks cached. Loaded key blocks from the repository"
            );

            // Fix loaded state
            {
                let mut write_guard = self.state.write().unwrap();
                write_guard.cached_key_blocks = Some(cached_key_blocks.clone());
            }

            // Unlock loading guard
            drop(loading_guard);

            // Result
            Ok(Some(cached_key_blocks))
        } else {
            // The repository is detached. We cannot provide any quick answers,
            //  so fall back to the linear iteration
            tracing::warn!(dataset_id=%self.dataset_id, "Key blocks repository is detached. Cannot read key blocks");
            Ok(None)
        }
    }

    async fn ensure_data_blocks_are_preloaded(
        &self,
        interested_range: std::ops::RangeInclusive<u64>,
    ) -> Result<Option<Arc<CachedBlocksRange>>, InternalError> {
        // Try getting access to repository
        let maybe_data_block_repository = {
            // Ignore if already covered
            let read_guard = self.state.read().unwrap();
            if let Some(cached_data_blocks) = &read_guard.cached_data_blocks
                && cached_data_blocks
                    .get_covered_range(self.config.data_blocks_page_size)
                    .is_some_and(|cached_range| cached_range.contains(interested_range.end()))
            {
                return Ok(Some(cached_data_blocks.clone()));
            }

            // Not covered. Try looking at repository
            read_guard.maybe_dataset_data_block_repo.clone()
        };

        // If there is a repository, we could try loading blocks to cover the range
        if let Some(data_block_repository) = maybe_data_block_repository {
            // Take loading lock
            let loading_guard = self.data_blocks_loading_lock.lock().await;

            // Try again, maybe another task has loaded required data blocks already
            {
                // Ignore if already covered
                let read_guard = self.state.read().unwrap();
                if let Some(cached_data_blocks) = &read_guard.cached_data_blocks
                    && cached_data_blocks
                        .get_covered_range(self.config.data_blocks_page_size)
                        .is_some_and(|cached_range| cached_range.contains(interested_range.end()))
                {
                    return Ok(Some(cached_data_blocks.clone()));
                }
            }

            // Read standard page of data blocks from the upper boundary.
            // It might not cover the entire interested range,
            // when range is large, load 1 page only.
            // The page might also be larger than interested range.
            let data_blocks = load_data_blocks_from_repository(
                data_block_repository.as_ref(),
                &self.dataset_id,
                self.config.data_blocks_page_size,
                *interested_range.end(),
            )
            .await?;

            // Form resulting structure
            let cached_data_blocks = Arc::new(CachedBlocksRange::new(data_blocks));

            // Report cache load operation
            tracing::trace!(
                dataset_id=%self.dataset_id,
                num_blocks = cached_data_blocks.len(),
                covered_range = ?cached_data_blocks.get_covered_range(self.config.data_blocks_page_size),
                "Data blocks cache miss. Loading page of data blocks from the repository"
            );

            // Fix loaded state: discard previous cache if any
            // Completely random access is undesirable, that would replace pages frequently
            // However, most of the time the access patterns are sequential & go backwards
            {
                let mut write_guard = self.state.write().unwrap();
                write_guard.cached_data_blocks = Some(cached_data_blocks.clone());
            }

            // Unlock loading guard
            drop(loading_guard);

            // Result
            Ok(Some(cached_data_blocks))
        } else {
            // The repository is detached. We cannot provide any quick answers,
            //  so fall back to the linear iteration
            tracing::warn!(dataset_id=%self.dataset_id, "Data blocks repository is detached. Cannot read data blocks");
            Ok(None)
        }
    }

    /// Generic helper method for block operations that follow the same pattern:
    ///  - check existing caches for keys and data blocks first
    ///  - if missing, and key blocks cache is not yet loaded, attempt loading
    ///  - don't attempt loading data blocks into cache, this is expensive
    ///  - try direct repository access for individual data block lookup
    ///  - if still not found, fall back to the underlying chain operation
    async fn get_from_cache_or_fallback<T, E, F, G>(
        &self,
        hash: &odf::Multihash,
        cache_lookup: F,
        data_blocks_repo_lookup: G,
        fallback_operation: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, E>
    where
        F: Fn(&CachedBlocksRange, &odf::Multihash) -> Option<T>,
        G: std::future::Future<Output = Result<Option<T>, InternalError>>,
        E: From<InternalError>,
    {
        // Force loading key blocks unless it was already done, and check in there
        let maybe_cached_key_blocks = self.ensure_key_blocks_are_preloaded().await?;
        if let Some(cached_key_blocks) = maybe_cached_key_blocks
            && let Some(result) = cache_lookup(cached_key_blocks.as_ref(), hash)
        {
            return Ok(result);
        }

        // Try checking existing data blocks cache, without preloading anything
        {
            let read_guard = self.state.read().unwrap();
            if let Some(cached_data_blocks) = read_guard.cached_data_blocks.as_ref()
                && let Some(result) = cache_lookup(cached_data_blocks, hash)
            {
                return Ok(result);
            }
        };

        // Try direct data blocks repository lookup - this should be faster than raw
        // chain, at least for S3, probably the same in local case
        if let Ok(Some(result)) = data_blocks_repo_lookup.await {
            return Ok(result);
        }

        // Fall back to the underlying potentially slow operation in the raw chain
        fallback_operation.await
    }

    async fn try_data_block_repo_lookup<T, F, Fut>(
        &self,
        operation: F,
    ) -> Result<Option<T>, InternalError>
    where
        F: FnOnce(Arc<dyn DatasetDataBlockRepository>, odf::DatasetID) -> Fut,
        Fut: std::future::Future<Output = Result<Option<T>, InternalError>>,
    {
        let maybe_data_block_repository = {
            let read_guard = self.state.read().unwrap();
            read_guard.maybe_dataset_data_block_repo.clone()
        };

        if let Some(data_block_repository) = maybe_data_block_repository {
            operation(data_block_repository, self.dataset_id.clone()).await
        } else {
            Ok(None)
        }
    }

    async fn get_key_block_with_hint(
        &self,
        requested_range: std::ops::RangeInclusive<u64>,
        hint_flags: odf::metadata::MetadataEventTypeFlags,
    ) -> Result<BlockLookupResult, odf::storage::GetBlockError> {
        // Force loading key blocks unless it was already done
        let maybe_key_blocks_cache = self.ensure_key_blocks_are_preloaded().await?;

        // Read what's in the key blocks cache
        if let Some(key_blocks_cache) = maybe_key_blocks_cache
            && let Some(cached_blocks_in_range) =
                key_blocks_cache.get_cached_blocks_for_range(requested_range)
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

        Ok(BlockLookupResult::NotFound)
    }

    async fn get_data_block_with_hint(
        &self,
        requested_range: std::ops::RangeInclusive<u64>,
        hint_flags: odf::metadata::MetadataEventTypeFlags,
    ) -> Result<BlockLookupResult, odf::storage::GetBlockError> {
        // Force loading data blocks unless it was already done
        let maybe_cached_data_blocks = self
            .ensure_data_blocks_are_preloaded(requested_range.clone())
            .await?;

        // Read what's in the data blocks cache
        if let Some(cached_data_blocks) = maybe_cached_data_blocks
            && let Some(cached_blocks_in_range) =
                cached_data_blocks.get_cached_blocks_for_range(requested_range)
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

            // Note that if there is no cached block matching the hints, this
            // does not mean anything, unlike key blocks.
            // There might be still data blocks in the earlier pages,
            // we don't know that for sure.
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
            self.try_data_block_repo_lookup(|repo, dataset_id| async move {
                repo.contains_data_block(&dataset_id, hash)
                    .await
                    .map(|exists| if exists { Some(true) } else { None })
            }),
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
            self.try_data_block_repo_lookup(|repo, dataset_id| async move {
                repo.get_data_block_size(&dataset_id, hash)
                    .await
                    .map(|size_opt| size_opt.map(|size| size as u64))
            }),
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
            CachedBlocksRange::try_get_original_block_payload,
            self.try_data_block_repo_lookup(|repo, dataset_id| async move {
                repo.get_data_block(&dataset_id, hash)
                    .await
                    .map(|block_opt| block_opt.map(|block| block.block_payload))
            }),
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
            self.try_data_block_repo_lookup(|repo, dataset_id| async move {
                match repo.get_data_block(&dataset_id, hash).await {
                    Ok(Some(data_block)) => {
                        match odf::storage::deserialize_metadata_block(
                            &data_block.block_hash,
                            &data_block.block_payload,
                        ) {
                            Ok(metadata_block) => Ok(Some(metadata_block)),
                            Err(e) => Err(e.int_err()),
                        }
                    }
                    Ok(None) => Ok(None),
                    Err(e) => Err(e),
                }
            }),
            self.metadata_chain.get_block(hash),
        )
        .await
    }

    fn iter_blocks_interval<'a>(
        &'a self,
        head_boundary: odf::dataset::MetadataChainIterBoundary<'a>,
        tail_boundary: Option<odf::dataset::MetadataChainIterBoundary<'a>>,
        ignore_missing_tail: bool,
    ) -> odf::dataset::DynMetadataStream<'a> {
        Box::pin(async_stream::try_stream! {
            // Force loading key blocks unless it was already done
            let maybe_cached_key_blocks = self.ensure_key_blocks_are_preloaded().await?;

            // Try getting data block repository
            let maybe_data_block_repository = if maybe_cached_key_blocks.is_some() {
                let read_guard = self.state.read().unwrap();
                read_guard.maybe_dataset_data_block_repo.clone()
            } else {
                None
            };

            // We should have both key blocks and data blocks cache available for optimized iteration
            if let Some(cached_key_blocks) = maybe_cached_key_blocks && let Some(data_block_repository) = maybe_data_block_repository {
                // We need to load starting block to know the sequence number
                let (head_hash, head_block) = match head_boundary {
                    odf::dataset::MetadataChainIterBoundary::Hash(h) => {
                        let block = self.get_block(h).await?;
                        (h.clone(), block)
                    },
                    odf::dataset::MetadataChainIterBoundary::Ref(r) => {
                        let h = self.resolve_ref(r).await?;
                        let block = self.get_block(&h).await?;
                        (h, block)
                    },
                };

                // We need tail hash boundary
                let maybe_tail_hash = match tail_boundary {
                    None => None,
                    Some(odf::dataset::MetadataChainIterBoundary::Hash(h)) => Some(h.clone()),
                    Some(odf::dataset::MetadataChainIterBoundary::Ref(r)) => Some(self.resolve_ref(r).await?),
                };

                // Create and initialize the merge iterator for efficient traversal
                let mut merge_iterator = CachedBlocksMergeIterator::prepare(
                    data_block_repository.as_ref(),
                    &self.dataset_id,
                    self.config.data_blocks_page_size,
                    cached_key_blocks.as_ref(),
                    head_block.sequence_number,
                )
                .await?;

                // Iterate backwards from head to tail using the optimized merge iterator
                loop {
                    // Get the next block with the highest sequence number
                    let Some((block_hash, block)) = merge_iterator.next().await? else {
                        // No more blocks
                        break;
                    };

                    // Check if we reached the tail
                    if Some(&block_hash) == maybe_tail_hash.as_ref() {
                        return;
                    }

                    // Yield the block
                    yield (block_hash, block);
                }

                // Check if tail was expected but not found
                if !ignore_missing_tail && let Some(tail_hash) = maybe_tail_hash {
                    Err(odf::IterBlocksError::InvalidInterval(odf::dataset::InvalidIntervalError {
                        head: head_hash,
                        tail: tail_hash,
                    }))?;
                }

            } else {
                // Data blocks cache is not available, simply forward the stream
                use futures::TryStreamExt;
                let mut stream = self.metadata_chain.iter_blocks_interval(head_boundary, tail_boundary, ignore_missing_tail);
                while let Some((block_hash, block)) = stream.try_next().await? {
                    yield (block_hash, block);
                }
            }
        })
    }

    async fn append<'a>(
        &'a self,
        block: odf::MetadataBlock,
        opts: odf::dataset::AppendOpts<'a>,
    ) -> Result<odf::Multihash, odf::dataset::AppendError> {
        // Classify the block type
        let block_flags = odf::metadata::MetadataEventTypeFlags::from(&block.event);

        // Key block
        if block_flags.has_key_block_flags() {
            // If there is anything cached for key blocks, we must reset it
            let mut write_guard = self.state.write().unwrap();
            write_guard.cached_key_blocks = None;

        // Data block
        } else if block_flags.has_data_flags() {
            // If there is anything cached for data blocks, we must reset it
            let mut write_guard = self.state.write().unwrap();
            write_guard.cached_data_blocks = None;
        }

        // Append the block to the underlying chain
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
        let requested_boundary: std::ops::RangeInclusive<u64> =
            tail_sequence_number..=head_block.sequence_number;

        // If we are looking for specific block types, try to use the caches first
        if let odf::dataset::MetadataVisitorDecision::NextOfType(hint_flags) = hint {
            // Try key blocks from cache, if flags expect key nodes
            let key_block_result = if hint_flags.has_key_block_flags() {
                self.get_key_block_with_hint(requested_boundary.clone(), hint_flags)
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
                    // Both key and data blocks present, pick the by the higher sequence number
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

    fn detach_from_transaction(&self) {
        // Pass over to the next level chain
        self.metadata_chain.detach_from_transaction();

        // Detach the repositories, as they are holding the transaction
        let mut write_guard = self.state.write().unwrap();
        write_guard.maybe_dataset_key_block_repo = None;
        write_guard.maybe_dataset_data_block_repo = None;
    }

    fn as_uncached_chain(&self) -> &dyn odf::MetadataChain {
        self.metadata_chain.as_uncached_chain()
    }

    fn as_uncached_ref_repo(&self) -> &dyn odf::storage::ReferenceRepository {
        self.metadata_chain.as_uncached_ref_repo()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
