// TODO: Use abstraction
use crate::infra::serde::yaml::MetadataBlock;

#[derive(Debug)]
pub enum BlockRef {
    Head,
}

// TODO: Separate mutable and immutable traits
// See: https://github.com/rust-lang/rfcs/issues/2035
pub trait MetadataChain {
    fn read_ref(&self, r: &BlockRef) -> Option<String>;

    fn iter_blocks<'c>(&'c self) -> Box<dyn Iterator<Item = MetadataBlock> + 'c> {
        self.iter_blocks_ref(&BlockRef::Head)
    }

    fn iter_blocks_ref<'c>(&'c self, r: &BlockRef) -> Box<dyn Iterator<Item = MetadataBlock> + 'c>;

    fn append_ref(&mut self, r: &BlockRef, block: MetadataBlock) -> String;

    fn append(&mut self, block: MetadataBlock) -> String {
        self.append_ref(&BlockRef::Head, block)
    }
}
