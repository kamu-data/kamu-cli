use super::metadata::*;

pub type BlockIterator = dyn Iterator<Item = MetadataBlock>;

pub trait MetadataChain {
    fn iter_blocks(&self) -> Box<BlockIterator>;
}
