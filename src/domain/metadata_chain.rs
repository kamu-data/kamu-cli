// TODO: Use abstraction
use crate::infra::serde::yaml::MetadataBlock;

pub trait MetadataChain {
    fn list_blocks(&self) -> Vec<MetadataBlock>;
}
