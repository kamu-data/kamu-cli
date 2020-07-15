use crate::domain::*;
use crate::infra::serde::yaml::*;

use indoc::indoc;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};

pub struct MetadataChainFsYaml {
    meta_path: PathBuf,
}

impl MetadataChainFsYaml {
    pub fn new(meta_path: &Path) -> MetadataChainFsYaml {
        MetadataChainFsYaml {
            meta_path: meta_path.to_owned(),
        }
    }
}

impl MetadataChain for MetadataChainFsYaml {
    fn list_blocks(&self) -> Vec<MetadataBlock> {
        let data = indoc!(
            "
            ---
            apiVersion: 1
            kind: MetadataBlock
            content:
              blockHash: ddeeaaddbbeeff
              prevBlockHash: ffeebbddaaeedd
              systemTime: '2020-01-01T12:00:00.000Z'
              source:
                kind: derivative
                inputs:
                - input1
                - input2
                transform:
                  engine: sparkSQL
                  query: >
                    SELECT * FROM input1 UNION ALL SELECT * FROM input2
              outputSlice:
                hash: ffaabb
                interval: '[2020-01-01T12:00:00.000Z, 2020-01-01T12:00:00.000Z]'
                numRecords: 10
              outputWatermark: '2020-01-01T12:00:00.000Z'
              inputSlices:
              - hash: aa
                interval: '(-inf, 2020-01-01T12:00:00.000Z]'
                numRecords: 10
              - hash: zz
                interval: '()'
                numRecords: 0"
        );

        let block: Manifest<MetadataBlock> = serde_yaml::from_str(data).unwrap();

        vec![block.content]
    }
}
