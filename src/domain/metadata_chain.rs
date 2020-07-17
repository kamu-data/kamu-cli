// TODO: Use abstraction
use crate::infra::serde::yaml::MetadataBlock;

pub enum BlockRef {
    Head,
}

pub trait MetadataChain {
    // TODO: Use newtype
    fn read_ref(&self, r: &BlockRef) -> String;

    fn list_blocks_ref<'c>(&'c self, r: &BlockRef) -> Box<dyn Iterator<Item = MetadataBlock> + 'c>;

    fn list_blocks<'c>(&'c self) -> Box<dyn Iterator<Item = MetadataBlock> + 'c> {
        self.list_blocks_ref(&BlockRef::Head)
    }
}
