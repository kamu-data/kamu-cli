use super::{Command, Error};
use kamu::domain::*;
use kamu::infra::serde::yaml::*;

use std::cell::RefCell;
use std::io::Read;
use std::rc::Rc;

pub struct AddCommand {
    resource_loader: Rc<RefCell<dyn ResourceLoader>>,
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    snapshot_refs: Vec<String>,
    recursive: bool,
}

impl AddCommand {
    pub fn new<'s, I>(
        resource_loader: Rc<RefCell<dyn ResourceLoader>>,
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        snapshot_refs_iter: I,
        recursive: bool,
    ) -> Self
    where
        I: Iterator<Item = &'s str>,
    {
        Self {
            resource_loader: resource_loader,
            metadata_repo: metadata_repo,
            snapshot_refs: snapshot_refs_iter.map(|s| s.to_owned()).collect(),
            recursive: recursive,
        }
    }
}

impl AddCommand {
    fn load_specific(&self) -> Result<Vec<DatasetSnapshot>, Error> {
        let snapshots: Vec<_> = self
            .snapshot_refs
            .iter()
            .filter_map(|r| {
                match self
                    .resource_loader
                    .borrow()
                    .load_dataset_snapshot_from_ref(r)
                {
                    Ok(s) => Some(s),
                    Err(e) => {
                        eprintln!(
                            "{}: {}\n  {}",
                            console::style("Failed to load data from").yellow(),
                            r,
                            console::style(e).dim()
                        );
                        None
                    }
                }
            })
            .collect();

        if snapshots.len() == self.snapshot_refs.len() {
            Ok(snapshots)
        } else {
            Err(Error::Aborted)
        }
    }

    fn load_recursive(&self) -> Result<Vec<DatasetSnapshot>, Error> {
        self.snapshot_refs
            .iter()
            .map(|r| std::path::Path::new(r).join("**").join("*.yaml"))
            .flat_map(|p| {
                glob::glob(p.to_str().unwrap())
                    .unwrap_or_else(|e| panic!("Failed to read glob {}: {}", p.display(), e))
            })
            .map(|e| e.unwrap())
            .filter(|p| self.is_snapshot_file(p))
            .map(|p| {
                self.resource_loader
                    .borrow()
                    .load_dataset_snapshot_from_path(&p)
                    .map_err(|e| e.into())
            })
            .collect()
    }

    fn is_snapshot_file(&self, path: &std::path::Path) -> bool {
        let mut file = std::fs::File::open(path)
            .unwrap_or_else(|e| panic!("Failed to read file {}: {}", path.display(), e));
        let mut buf = [0; 64];
        let read = file.read(&mut buf).unwrap();
        match std::str::from_utf8(&buf[0..read]) {
            Ok(s) => s.contains("DatasetSnapshot"),
            Err(_) => false,
        }
    }
}

impl Command for AddCommand {
    fn run(&mut self) -> Result<(), Error> {
        let snapshots = if !self.recursive {
            self.load_specific()?
        } else {
            self.load_recursive()?
        };

        let (mut added, mut errors) = (0, 0);

        for (id, res) in self
            .metadata_repo
            .borrow_mut()
            .add_datasets(&mut snapshots.into_iter())
        {
            match res {
                Ok(_) => {
                    added += 1;
                    eprintln!("{}: {}", console::style("Added").green(), id);
                }
                Err(err @ DomainError::AlreadyExists { .. }) => {
                    eprintln!("{}: {}", console::style("Warning").yellow(), err);
                }
                Err(err) => {
                    errors += 1;
                    eprintln!("{}: {}", console::style("Error").red(), err);
                }
            }
        }

        if added != 0 {
            eprintln!(
                "{}",
                console::style(format!("Added {} dataset(s)", added))
                    .green()
                    .bold()
            );
        }

        if errors == 0 {
            Ok(())
        } else if added > 0 {
            Err(Error::PartialFailure)
        } else {
            Err(Error::Aborted)
        }
    }
}
