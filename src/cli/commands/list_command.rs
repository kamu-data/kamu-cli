use super::Command;
use kamu::domain::*;

pub struct ListCommand<'a> {
    metadata_repo: &'a dyn MetadataRepository,
}

impl ListCommand<'_> {
    pub fn new(metadata_repo: &dyn MetadataRepository) -> ListCommand {
        ListCommand {
            metadata_repo: metadata_repo,
        }
    }
}

impl Command for ListCommand<'_> {
    fn run(&mut self) {
        for id in self.metadata_repo.list_datasets() {
            println!("{:?}", id);
        }
    }
}
