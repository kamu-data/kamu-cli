extern crate kamu;

use kamu::domain::MetadataChain;
use kamu::infra::MetadataChainFsYaml;
use std::path::Path;

#[test]
fn iter_blocks() {
    let chain = MetadataChainFsYaml::new(Path::new(""));
    for b in chain.iter_blocks() {
        println!("{:?}", b);
    }
}
