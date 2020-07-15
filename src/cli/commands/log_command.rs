use super::Command;
use kamu::domain::*;

pub struct LogCommand<'a> {
    metadata_repo: &'a dyn MetadataRepository,
}

impl LogCommand<'_> {
    pub fn new(metadata_repo: &dyn MetadataRepository) -> LogCommand {
        LogCommand {
            metadata_repo: metadata_repo,
        }
    }
}

impl Command for LogCommand<'_> {
    fn run(&mut self) {
        let chain = self
            .metadata_repo
            .get_metadata_chain(&DatasetID::try_from("aaa").unwrap());

        for block in chain.list_blocks() {
            println!("{:?}", block);
        }
    }
}
