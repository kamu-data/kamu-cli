use super::{Command, Error};
use kamu::domain::*;

use std::cell::RefCell;
use std::rc::Rc;

pub struct AddCommand {
    resource_loader: Rc<RefCell<dyn ResourceLoader>>,
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    snapshot_refs: Vec<String>,
}

impl AddCommand {
    pub fn new<'s, I>(
        resource_loader: Rc<RefCell<dyn ResourceLoader>>,
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        snapshot_refs_iter: I,
    ) -> Self
    where
        I: Iterator<Item = &'s str>,
    {
        Self {
            resource_loader: resource_loader,
            metadata_repo: metadata_repo,
            snapshot_refs: snapshot_refs_iter.map(|s| s.to_owned()).collect(),
        }
    }
}

impl Command for AddCommand {
    fn run(&mut self) -> Result<(), Error> {
        let results: Vec<_> = self
            .snapshot_refs
            .iter()
            .map(|r| {
                self.resource_loader
                    .borrow()
                    .load_dataset_snapshot_from_ref(r)
            })
            .collect();

        let snapshots: Vec<_> = results
            .into_iter()
            .zip(self.snapshot_refs.iter())
            .filter_map(|(res, sref)| match res {
                Ok(s) => Some(s),
                Err(e) => {
                    eprintln!(
                        "{}: {}\n  {}",
                        console::style("Failed to load data from").yellow(),
                        sref,
                        console::style(e).dim()
                    );
                    None
                }
            })
            .collect();

        if snapshots.len() != self.snapshot_refs.len() {
            return Err(Error::Aborted);
        }

        for (id, res) in self.metadata_repo.borrow_mut().add_datasets(snapshots) {
            match res {
                Ok(_) => eprintln!("Added: {}", id),
                Err(err) => eprintln!("Failed: {} - {}", id, err),
            }
        }

        Ok(())
    }
}
