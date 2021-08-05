use opendatafabric::{MetadataBlock, Sha3_256};

#[derive(Debug)]
pub enum BlockRef {
    Head,
}

// TODO: Separate mutable and immutable traits
// See: https://github.com/rust-lang/rfcs/issues/2035
pub trait MetadataChain: Send {
    fn read_ref(&self, r: &BlockRef) -> Option<Sha3_256>;

    fn get_block(&self, block_hash: &Sha3_256) -> Option<MetadataBlock>;

    /// Iterates blocks in reverse order, following the previous block links
    fn iter_blocks(&self) -> Box<dyn Iterator<Item = MetadataBlock>> {
        self.iter_blocks_ref(&BlockRef::Head)
    }

    /// Iterates blocks in reverse order, following the previous block links
    fn iter_blocks_ref(&self, r: &BlockRef) -> Box<dyn Iterator<Item = MetadataBlock>>;

    fn append_ref(&mut self, r: &BlockRef, block: MetadataBlock) -> Sha3_256;

    fn append(&mut self, block: MetadataBlock) -> Sha3_256 {
        self.append_ref(&BlockRef::Head, block)
    }
}
