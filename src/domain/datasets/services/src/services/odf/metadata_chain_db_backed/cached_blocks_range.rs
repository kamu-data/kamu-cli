// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CachedBlocksRange {
    /// Cached blocks with hashes in chronological order
    blocks: Vec<(odf::Multihash, odf::MetadataBlock)>,

    /// Original block payloads, in the same order as in `blocks`
    original_block_payloads: Vec<bytes::Bytes>,

    /// The same info in lookup-friendly form:
    ///  index in `blocks` by block hash
    blocks_lookup: HashMap<odf::Multihash, usize>,
}

impl CachedBlocksRange {
    pub(crate) fn new(block_rows: Vec<(odf::Multihash, bytes::Bytes, odf::MetadataBlock)>) -> Self {
        let mut blocks = Vec::with_capacity(block_rows.len());
        let mut original_block_payloads = Vec::with_capacity(block_rows.len());

        for (hash, payload, block) in block_rows {
            blocks.push((hash, block));
            original_block_payloads.push(payload);
        }

        let blocks_lookup = blocks
            .iter()
            .enumerate()
            .map(|(idx, (hash, _))| (hash.clone(), idx))
            .collect();

        Self {
            blocks,
            original_block_payloads,
            blocks_lookup,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.blocks.len()
    }

    pub(crate) fn contains_block(&self, hash: &odf::Multihash) -> bool {
        self.blocks_lookup.contains_key(hash)
    }

    pub(crate) fn try_get_block(&self, hash: &odf::Multihash) -> Option<odf::MetadataBlock> {
        self.blocks_lookup
            .get(hash)
            .map(|&idx| self.blocks[idx].1.clone())
    }

    pub(crate) fn try_get_block_size(&self, hash: &odf::Multihash) -> Option<u64> {
        self.blocks_lookup
            .get(hash)
            .map(|&idx| self.original_block_payloads[idx].len() as u64)
    }

    pub(crate) fn try_get_original_block_payload(
        &self,
        hash: &odf::Multihash,
    ) -> Option<bytes::Bytes> {
        self.blocks_lookup
            .get(hash)
            .map(|&idx| self.original_block_payloads[idx].clone())
    }

    pub(crate) fn get_block_by_index(
        &self,
        index: usize,
    ) -> Option<&(odf::Multihash, odf::MetadataBlock)> {
        self.blocks.get(index)
    }

    /// Describe a covered range of sequence numbers
    pub(crate) fn get_covered_range(
        &self,
        full_page_size: usize,
    ) -> Option<std::ops::RangeInclusive<u64>> {
        if self.blocks.is_empty() {
            None
        } else {
            let min_boundary = self.blocks.first().unwrap().1.sequence_number;
            let max_boundary = self.blocks.last().unwrap().1.sequence_number;

            // If we've loaded less than full page size, it means we are at the beginning.
            // At the beginning, there are at least some key blocks,
            // so data blocks never start from seq number 0
            if self.blocks.len() < full_page_size {
                Some(0..=max_boundary)
            } else {
                Some(min_boundary..=max_boundary)
            }
        }
    }

    /// Try extracting a slice of cached blocks satisftying given range
    pub(crate) fn get_cached_blocks_for_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> Option<&[(odf::Multihash, odf::MetadataBlock)]> {
        // No blocks yet?
        if self.blocks.is_empty() {
            return None;
        }

        // Range is completely after the cached blocks?
        let min_sequence_number = self.blocks.first().unwrap().1.sequence_number;
        if min_sequence_number >= range.end {
            return None;
        }

        // Find the first block that is greater than or equal to the min boundary
        let start_index = self
            .blocks
            .binary_search_by_key(&range.start, |(_, block)| block.sequence_number)
            .unwrap_or_else(|x| x);

        // Use the start_index to reduce the search space for the max boundary
        let end_index = self.blocks[start_index..]
            .binary_search_by_key(&range.end, |(_, block)| block.sequence_number)
            .map(|x| x + start_index)
            .unwrap_or_else(|x| x + start_index);

        // The slice is [start_index, end_index)
        //   [ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9 ]
        //           min=2                  max=8
        //             ^ start_index           |
        //                                     ^ end_index
        Some(&self.blocks[start_index..end_index])
    }

    /// Try finding the index of the last block with sequence number <= given
    pub(crate) fn find_last_block_index_before_or_at(&self, sequence_number: u64) -> Option<usize> {
        if self.blocks.is_empty() {
            return None;
        }

        match self
            .blocks
            .binary_search_by_key(&sequence_number, |(_, block)| block.sequence_number)
        {
            Ok(idx) => Some(idx), // Exact match
            Err(idx) => {
                // idx is the insertion point, so idx-1 is the last element <= sequence_number
                if idx > 0 { Some(idx - 1) } else { None }
            }
        }
    }

    /// Create a reverse iterator starting from the highest sequence number
    /// block that is <= `start_sequence_number`
    pub(crate) fn reverse_iter_from_sequence_number(
        &self,
        start_sequence_number: u64,
    ) -> CachedBlocksReverseIterator<'_> {
        // Find the last block that is <= start_sequence_number (returns None if empty)
        let start_index = self.find_last_block_index_before_or_at(start_sequence_number);

        // Launch iterator from that position
        CachedBlocksReverseIterator {
            blocks: &self.blocks,
            current_index: start_index,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) enum BlockLookupResult {
    Found((odf::Multihash, odf::MetadataBlock)),
    NotFound,
    Stop,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CachedBlocksReverseIterator<'a> {
    blocks: &'a [(odf::Multihash, odf::MetadataBlock)],
    current_index: Option<usize>,
}

impl CachedBlocksReverseIterator<'_> {
    /// Peek at the current block without advancing the iterator
    pub(crate) fn peek(&self) -> Option<&(odf::Multihash, odf::MetadataBlock)> {
        self.current_index.map(|idx| &self.blocks[idx])
    }

    /// Get the current block's sequence number without advancing the iterator
    pub(crate) fn current_sequence_number(&self) -> Option<u64> {
        self.peek().map(|(_, block)| block.sequence_number)
    }

    /// Advance to the next block (lower sequence number)
    pub(crate) fn next(&mut self) -> Option<&(odf::Multihash, odf::MetadataBlock)> {
        match self.current_index {
            Some(idx) => {
                let current_block = &self.blocks[idx];
                self.current_index = if idx > 0 { Some(idx - 1) } else { None };
                Some(current_block)
            }
            None => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
