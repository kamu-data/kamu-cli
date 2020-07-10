extern crate kamu;

use super::Command;
use kamu::domain::MetadataChain;
use kamu::infra::MetadataChainFsYaml;
use std::path::Path;

pub struct LogCommand;

impl LogCommand {
    pub fn new() -> LogCommand {
        LogCommand
    }
}

impl Command for LogCommand {
    fn run(&mut self) {
        let chain = MetadataChainFsYaml::new(Path::new("."));

        for block in chain.iter_blocks() {
            println!("{:?}", block);
        }
    }
}
