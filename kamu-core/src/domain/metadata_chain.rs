// TODO: Use abstraction
use crate::infra::serde::yaml::MetadataBlock;

#[derive(Debug)]
pub enum BlockRef {
    Head,
}

// TODO: Separate mutable and immutable traits
// See: https://github.com/rust-lang/rfcs/issues/2035
pub trait MetadataChain: Send {
    fn read_ref(&self, r: &BlockRef) -> Option<String>;

    fn get_block(&self, block_hash: &str) -> Option<MetadataBlock>;

    fn iter_blocks(&self) -> Box<dyn Iterator<Item = MetadataBlock>> {
        self.iter_blocks_ref(&BlockRef::Head)
    }

    fn iter_blocks_ref(&self, r: &BlockRef) -> Box<dyn Iterator<Item = MetadataBlock>>;

    fn append_ref(&mut self, r: &BlockRef, block: MetadataBlock) -> String;

    fn append(&mut self, block: MetadataBlock) -> String {
        self.append_ref(&BlockRef::Head, block)
    }
}
