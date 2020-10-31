use super::{Command, Error};
use kamu::domain::*;
use opendatafabric::*;

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
    fn load_specific(&self) -> Vec<(String, Result<DatasetSnapshot, DomainError>)> {
        self.snapshot_refs
            .iter()
            .map(|r| {
                (
                    r.clone(),
                    self.resource_loader
                        .borrow()
                        .load_dataset_snapshot_from_ref(r)
                        .map_err(|e| DomainError::InfraError(e.into())),
                )
            })
            .collect()
    }

    fn load_recursive(&self) -> Vec<(String, Result<DatasetSnapshot, DomainError>)> {
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
                (
                    p.to_str().unwrap().to_owned(),
                    self.resource_loader
                        .borrow()
                        .load_dataset_snapshot_from_path(&p)
                        .map_err(|e| DomainError::InfraError(e.into())),
                )
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
        let mut load_results = if !self.recursive {
            self.load_specific()
        } else {
            self.load_recursive()
        };

        load_results.sort_by(|(ref_a, _), (ref_b, _)| ref_a.cmp(&ref_b));

        let (mut added, mut errors) = (0, 0);

        load_results.iter().for_each(|(r, res)| match res {
            Err(error) => {
                eprintln!("{}: {}", console::style(r).red(), error);
                errors += 1;
            }
            _ => (),
        });

        if errors != 0 {
            return Err(Error::Aborted);
        }

        let mut add_results =
            self.metadata_repo
                .borrow_mut()
                .add_datasets(
                    &mut load_results.into_iter().filter_map(|(_, res)| match res {
                        Ok(snapshot) => Some(snapshot),
                        _ => None,
                    }),
                );

        add_results.sort_by(|(id_a, _), (id_b, _)| id_a.cmp(&id_b));

        for (id, res) in add_results {
            match res {
                Ok(_) => {
                    added += 1;
                    eprintln!("{}: {}", console::style("Added").green(), id);
                }
                Err(DomainError::AlreadyExists { .. }) => {
                    eprintln!(
                        "{}: {}: Already exists",
                        console::style("Skipped").yellow(),
                        id
                    );
                }
                Err(err) => {
                    errors += 1;
                    eprintln!("{}: {}: {}", console::style("Error").red(), id, err);
                }
            }
        }

        eprintln!(
            "{}",
            console::style(format!("Added {} dataset(s)", added))
                .green()
                .bold()
        );

        if errors == 0 {
            Ok(())
        } else if added > 0 {
            Err(Error::PartialFailure)
        } else {
            Err(Error::Aborted)
        }
    }
}
