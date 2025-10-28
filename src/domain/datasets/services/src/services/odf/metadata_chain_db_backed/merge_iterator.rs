// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;

use super::cached_blocks_range::{CachedBlocksRange, CachedBlocksReverseIterator};
use super::load_helper::load_data_blocks_from_repository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Manages the state for efficiently merging key blocks and data blocks streams
/// during reverse iteration (from highest to lowest sequence numbers)
pub struct CachedBlocksMergeIterator<'a> {
    // Immutable part
    dataset_id: &'a odf::DatasetID,
    data_block_repository: &'a dyn kamu_datasets::DatasetDataBlockRepository,
    page_size: usize,
    start_sequence_number: u64,
    // Mutable part
    key_blocks_iter: CachedBlocksReverseIterator<'a>,
    data_blocks_page: Option<Arc<CachedBlocksRange>>,
    data_blocks_current_index: Option<usize>,
    data_blocks_covered_range: Option<std::ops::RangeInclusive<u64>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> CachedBlocksMergeIterator<'a> {
    /// Create and initialize a new merge iterator with both sub-streams ready
    pub(crate) async fn prepare(
        data_block_repository: &'a dyn kamu_datasets::DatasetDataBlockRepository,
        dataset_id: &'a odf::DatasetID,
        page_size: usize,
        cached_key_blocks: &'a CachedBlocksRange,
        start_sequence_number: u64,
    ) -> Result<Self, InternalError> {
        // Initialize key blocks iterator
        let key_blocks_iter =
            cached_key_blocks.reverse_iter_from_sequence_number(start_sequence_number);

        // Create the iterator instance
        let mut iterator = Self {
            data_block_repository,
            dataset_id,
            page_size,
            start_sequence_number,
            key_blocks_iter,
            data_blocks_page: None,
            data_blocks_current_index: None,
            data_blocks_covered_range: None,
        };

        // Load the initial data blocks page to ensure we have both streams ready
        iterator
            .ensure_data_blocks_page_covers(start_sequence_number)
            .await?;

        // Return the prepared iterator
        Ok(iterator)
    }

    /// Get the next block with automatic data page loading
    pub(crate) async fn next(
        &mut self,
    ) -> Result<Option<(odf::Multihash, odf::MetadataBlock)>, InternalError> {
        // Peek sequence numbers from both iterators
        let key_seq = self.key_blocks_iter.current_sequence_number();
        let data_seq = self.data_blocks_current_sequence_number();

        // Decide which block to return
        match (key_seq, data_seq) {
            (Some(key_seq_num), Some(data_seq_num)) => {
                // Both iterators have blocks, choose the one with higher sequence number
                if key_seq_num >= data_seq_num {
                    Ok(self.key_blocks_iter.next().cloned())
                } else {
                    Ok(self.next_data_block())
                }
            }
            (Some(key_seq_num), None) => {
                // Key blocks available, but no current data blocks
                // Before falling back to key blocks, check if we should load more data blocks
                if self.should_try_loading_more_data_blocks() {
                    // Try to load more data blocks
                    let target_seq_num = self.calculate_next_data_blocks_target_sequence();
                    self.ensure_data_blocks_page_covers(target_seq_num).await?;

                    // After loading, try the decision again
                    let new_data_seq = self.data_blocks_current_sequence_number();
                    if let Some(new_data_seq_num) = new_data_seq {
                        // Now we have both, choose highest
                        if key_seq_num >= new_data_seq_num {
                            Ok(self.key_blocks_iter.next().cloned())
                        } else {
                            Ok(self.next_data_block())
                        }
                    } else {
                        // Still no data blocks after loading, use key block
                        Ok(self.key_blocks_iter.next().cloned())
                    }
                } else {
                    // No more data blocks expected, use key blocks
                    Ok(self.key_blocks_iter.next().cloned())
                }
            }
            (None, Some(_)) => {
                // Only data blocks available
                Ok(self.next_data_block())
            }
            (None, None) => {
                // If we reach here, we've exhausted all blocks
                Ok(None)
            }
        }
    }

    /// Auto-load a new page of data blocks to cover the current sequence number
    async fn ensure_data_blocks_page_covers(
        &mut self,
        sequence_number: u64,
    ) -> Result<(), InternalError> {
        // Check if current page covers the sequence number
        if let Some(ref covered_range) = self.data_blocks_covered_range
            && covered_range.contains(&sequence_number)
        {
            return Ok(()); // Already covered
        }

        // Load new page
        let data_blocks = load_data_blocks_from_repository(
            self.data_block_repository,
            self.dataset_id,
            self.page_size,
            sequence_number,
        )
        .await?;

        // Tune iterator state for the new page

        let cached_data_blocks = Arc::new(CachedBlocksRange::new(data_blocks));

        self.data_blocks_covered_range = cached_data_blocks.get_covered_range(self.page_size);

        self.data_blocks_current_index =
            cached_data_blocks.find_last_block_index_before_or_at(sequence_number);

        self.data_blocks_page = Some(cached_data_blocks);

        Ok(())
    }

    /// Get current data blocks sequence number
    fn data_blocks_current_sequence_number(&self) -> Option<u64> {
        if let Some(ref page) = self.data_blocks_page
            && let Some(idx) = self.data_blocks_current_index
        {
            Some(page.get_block_by_index(idx).unwrap().1.sequence_number)
        } else {
            None
        }
    }

    /// Advance data blocks iterator and return current block
    fn next_data_block(&mut self) -> Option<(odf::Multihash, odf::MetadataBlock)> {
        if let Some(ref page) = self.data_blocks_page
            && let Some(idx) = self.data_blocks_current_index
        {
            let current_block = page.get_block_by_index(idx).unwrap().clone();
            self.data_blocks_current_index = if idx > 0 { Some(idx - 1) } else { None };
            Some(current_block)
        } else {
            None
        }
    }

    /// Check if we should attempt to load more data blocks
    fn should_try_loading_more_data_blocks(&self) -> bool {
        if self.data_blocks_covered_range.is_some() {
            // If we have a covered range but our iterator is exhausted,
            // we need to load the next page
            self.data_blocks_current_index.is_none()
        } else {
            // No data blocks loaded yet, try loading some
            true
        }
    }

    /// Calculate the target sequence number for loading the next data page
    fn calculate_next_data_blocks_target_sequence(&self) -> u64 {
        if let Some(ref covered_range) = self.data_blocks_covered_range {
            // We need to load the page that comes before our current range
            // The next page should end right before our current range starts
            covered_range.start().saturating_sub(1)
        } else {
            // No data blocks loaded yet, start from the highest available
            // Let's use a high number that will trigger loading from the top
            self.start_sequence_number
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
