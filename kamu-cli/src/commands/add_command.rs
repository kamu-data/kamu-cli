use super::common;
use super::{Command, Error};
use kamu::domain::*;
use opendatafabric::*;

use thiserror::Error;

use std::cell::RefCell;
use std::io::Read;
use std::rc::Rc;

pub struct AddCommand {
    resource_loader: Rc<RefCell<dyn ResourceLoader>>,
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    snapshot_refs: Vec<String>,
    recursive: bool,
    replace: bool,
}

impl AddCommand {
    pub fn new<'s, I>(
        resource_loader: Rc<RefCell<dyn ResourceLoader>>,
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        snapshot_refs_iter: I,
        recursive: bool,
        replace: bool,
    ) -> Self
    where
        I: Iterator<Item = &'s str>,
    {
        Self {
            resource_loader: resource_loader,
            metadata_repo: metadata_repo,
            snapshot_refs: snapshot_refs_iter.map(|s| s.to_owned()).collect(),
            recursive: recursive,
            replace: replace,
        }
    }
}

impl AddCommand {
    fn load_specific(&self) -> Vec<Result<DatasetSnapshot, DatasetAddError>> {
        self.snapshot_refs
            .iter()
            .map(|r| {
                self.resource_loader
                    .borrow()
                    .load_dataset_snapshot_from_ref(r)
                    .map_err(|e| DatasetAddError::new(r.clone(), DomainError::InfraError(e.into())))
            })
            .collect()
    }

    fn load_recursive(&self) -> Vec<Result<DatasetSnapshot, DatasetAddError>> {
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
                    .map_err(|e| {
                        DatasetAddError::new(
                            p.to_str().unwrap().to_owned(),
                            DomainError::InfraError(e.into()),
                        )
                    })
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
        let load_results = if !self.recursive {
            self.load_specific()
        } else {
            self.load_recursive()
        };

        let (snapshots, errors): (Vec<_>, Vec<_>) =
            load_results.into_iter().partition(Result::is_ok);
        let snapshots: Vec<_> = snapshots.into_iter().map(Result::unwrap).collect();
        let mut errors: Vec<_> = errors.into_iter().map(Result::unwrap_err).collect();

        if errors.len() != 0 {
            errors.sort_by(|a, b| a.dataset_ref.cmp(&b.dataset_ref));
            for e in errors {
                eprintln!("{}: {}", console::style(e.dataset_ref).red(), e.source);
            }
            return Err(Error::Aborted);
        }

        // Delete existing datasets if we are replacing
        if self.replace {
            let already_exist: Vec<_> = snapshots
                .iter()
                .filter(|s| self.metadata_repo.borrow().get_summary(&s.id).is_ok())
                .collect();

            if already_exist.len() != 0 {
                let confirmed = common::prompt_yes_no(&format!(
                    "{}: {}\n{}\nDo you whish to continue? [y/N]: ",
                    console::style("You are about to replace following dataset(s)").yellow(),
                    already_exist
                        .iter()
                        .map(|s| s.id.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    console::style("This operation is irreversible!").yellow(),
                ));

                if !confirmed {
                    return Err(Error::Aborted);
                }

                for s in already_exist {
                    self.metadata_repo.borrow_mut().delete_dataset(&s.id)?;
                }
            }
        };

        let mut add_results = self
            .metadata_repo
            .borrow_mut()
            .add_datasets(&mut snapshots.into_iter());

        add_results.sort_by(|(id_a, _), (id_b, _)| id_a.cmp(&id_b));

        let (mut num_added, mut num_errors) = (0, 0);

        for (id, res) in add_results {
            match res {
                Ok(_) => {
                    num_added += 1;
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
                    num_errors += 1;
                    eprintln!("{}: {}: {}", console::style("Error").red(), id, err);
                }
            }
        }

        eprintln!(
            "{}",
            console::style(format!("Added {} dataset(s)", num_added))
                .green()
                .bold()
        );

        if num_errors == 0 {
            Ok(())
        } else if num_added > 0 {
            Err(Error::PartialFailure)
        } else {
            Err(Error::Aborted)
        }
    }
}

#[derive(Error, Debug)]
#[error("Failed to add {dataset_ref}")]
struct DatasetAddError {
    pub dataset_ref: String,
    #[source]
    pub source: DomainError,
}

impl DatasetAddError {
    pub fn new(dataset_ref: String, source: DomainError) -> Self {
        DatasetAddError {
            dataset_ref: dataset_ref,
            source: source,
        }
    }
}
