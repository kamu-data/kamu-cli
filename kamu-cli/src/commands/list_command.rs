use super::{Command, Error};
use kamu::domain::*;

use std::cell::RefCell;
use std::rc::Rc;

pub struct ListCommand {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
}

impl ListCommand {
    pub fn new(metadata_repo: Rc<RefCell<dyn MetadataRepository>>) -> Self {
        Self {
            metadata_repo: metadata_repo,
        }
    }
}

impl Command for ListCommand {
    fn run(&mut self) -> Result<(), Error> {
        let mut datasets: Vec<DatasetIDBuf> =
            self.metadata_repo.borrow().get_all_datasets().collect();

        datasets.sort();

        for id in datasets {
            println!("{}", id);
        }

        Ok(())
    }
}
