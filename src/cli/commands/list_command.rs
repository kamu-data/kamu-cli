use super::{Command, Error};
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
    fn run(&mut self) -> Result<(), Error> {
        let mut datasets: Vec<DatasetIDBuf> = self.metadata_repo.iter_datasets().collect();

        datasets.sort();

        for id in datasets {
            println!("{}", id);
        }

        Ok(())
    }
}
