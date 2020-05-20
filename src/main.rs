extern crate hex;

mod domain;
mod infra;

use domain::*;
use std::path::Path;

fn main() {
    let chain = infra::MetadataChainFsYaml::new(Path::new("."));

    for block in chain.iter_blocks() {
        println!("{:?}", block);
    }
}
