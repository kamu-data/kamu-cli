// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::repos::metadata_chain::BlockRef;
use opendatafabric::{MetadataBlock, Multihash};

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Error handling
// TODO: Separate mutable and immutable traits
// See: https://github.com/rust-lang/rfcs/issues/2035
pub trait MetadataChain: Send {
    fn read_ref(&self, r: &BlockRef) -> Option<Multihash>;

    fn get_block(&self, block_hash: &Multihash) -> Option<MetadataBlock>;

    /// Iterates blocks in reverse order, following the previous block links
    fn iter_blocks(&self) -> Box<dyn Iterator<Item = (Multihash, MetadataBlock)>> {
        self.iter_blocks_ref(&BlockRef::Head)
    }

    /// Iterates blocks in reverse order, following the previous block links
    fn iter_blocks_ref(
        &self,
        r: &BlockRef,
    ) -> Box<dyn Iterator<Item = (Multihash, MetadataBlock)>> {
        let block_hash = self
            .read_ref(r)
            .unwrap_or_else(|| panic!("Block reference {:?} does not exist", r));
        self.iter_blocks_starting(&block_hash)
            .unwrap_or_else(|| panic!("Block {} referenced as {:?} does not exist", block_hash, r))
    }

    /// Iterates blocks in reverse order starting with specified hash
    fn iter_blocks_starting(
        &self,
        block_hash: &Multihash,
    ) -> Option<Box<dyn Iterator<Item = (Multihash, MetadataBlock)>>>;

    fn append_ref(&mut self, r: &BlockRef, block: MetadataBlock) -> Multihash;

    fn append(&mut self, block: MetadataBlock) -> Multihash {
        self.append_ref(&BlockRef::Head, block)
    }
}
