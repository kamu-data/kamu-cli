use super::{Command, Error};
use kamu::domain::*;

pub struct LogCommand<'a> {
    metadata_repo: &'a dyn MetadataRepository,
    dataset_id: DatasetIDBuf,
}

impl LogCommand<'_> {
    pub fn new(metadata_repo: &dyn MetadataRepository, dataset_id: DatasetIDBuf) -> LogCommand {
        LogCommand {
            metadata_repo: metadata_repo,
            dataset_id: dataset_id,
        }
    }
}

impl Command for LogCommand<'_> {
    fn run(&mut self) -> Result<(), Error> {
        let chain = self.metadata_repo.get_metadata_chain(&self.dataset_id)?;

        for block in chain.list_blocks() {
            println!("{}", block.block_hash);
        }

        Ok(())
    }
}
