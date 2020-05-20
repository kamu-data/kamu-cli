use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct MetadataBlock {
    pub block_hash: String,
    pub prev_block_hash: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Manifest<T>
where
    T: Serialize,
{
    pub version: u8,
    pub kind: String,
    pub content: T,
}
