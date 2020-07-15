use super::Command;
use kamu::domain::*;

pub struct ListCommand<'a> {
    metadata_repo: &'a MetadataRepository,
}

impl ListCommand<'_> {
    pub fn new(metadata_repo: &MetadataRepository) -> ListCommand {
        ListCommand {
            metadata_repo: metadata_repo,
        }
    }
}

impl Command for ListCommand<'_> {
    fn run(&mut self) {
        /*self.metadata_repo.get_all_datasets();
        let chain = self.metadata_repo.get_metadata_chain();

        for block in chain.iter_blocks() {
            println!("{:?}", block);
        }*/
    }
}
