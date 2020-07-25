use kamu::infra::serde::yaml::*;

use chrono::{SubsecRound, Utc};

pub struct MetadataFactory;

impl MetadataFactory {
    pub fn metadata_block() -> MetadataBlockBuilder {
        MetadataBlockBuilder::new()
    }
}

pub struct MetadataBlockBuilder {
    v: MetadataBlock,
}

impl MetadataBlockBuilder {
    fn new() -> Self {
        Self {
            v: MetadataBlock {
                block_hash: "".to_owned(),
                prev_block_hash: "".to_owned(),
                system_time: Utc::now().round_subsecs(3),
                output_slice: None,
                output_watermark: None,
                input_slices: None,
                source: None,
            },
        }
    }

    pub fn prev(mut self, prev_block_hash: &str) -> Self {
        self.v.prev_block_hash.clear();
        self.v.prev_block_hash.push_str(prev_block_hash);
        self
    }

    pub fn build(self) -> MetadataBlock {
        self.v
    }
}
