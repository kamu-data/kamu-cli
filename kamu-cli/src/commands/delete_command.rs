use super::{Command, Error};
use kamu::domain::*;

use std::cell::RefCell;
use std::convert::TryFrom;
use std::rc::Rc;

pub struct DeleteCommand {
    metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
    ids: Vec<String>,
    all: bool,
    recursive: bool,
    no_confirmation: bool,
}

impl DeleteCommand {
    pub fn new<I, S>(
        metadata_repo: Rc<RefCell<dyn MetadataRepository>>,
        ids: I,
        all: bool,
        recursive: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            metadata_repo: metadata_repo,
            ids: ids.map(|s| s.as_ref().to_owned()).collect(),
            all: all,
            recursive: recursive,
            no_confirmation: no_confirmation,
        }
    }

    fn prompt_yes_no(&self, msg: &str) -> bool {
        use read_input::prelude::*;

        let answer: String = input()
            .repeat_msg(msg)
            .default("n".to_owned())
            .add_test(|v| match v.as_ref() {
                "n" | "N" | "no" | "y" | "Y" | "yes" => true,
                _ => false,
            })
            .get();

        match answer.as_ref() {
            "n" | "N" | "no" => false,
            _ => true,
        }
    }
}

impl Command for DeleteCommand {
    fn run(&mut self) -> Result<(), Error> {
        let starting_dataset_ids = self
            .ids
            .iter()
            .map(|s| DatasetIDBuf::try_from(s as &str))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let dataset_ids = if self.all {
            unimplemented!("Recursive deletion is not yet supported")
        } else if self.recursive {
            unimplemented!("Recursive deletion is not yet supported")
        } else {
            starting_dataset_ids
        };

        let confirmed = if self.no_confirmation {
            true
        } else {
            self.prompt_yes_no(&format!(
                "{}: {}\n{}\nDo you whish to continue? [y/N]: ",
                console::style("You are about to delete following dataset(s)").yellow(),
                dataset_ids
                    .iter()
                    .map(|id| id.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                console::style("This operation is irreversible!").yellow(),
            ))
        };

        if !confirmed {
            return Err(Error::Aborted);
        }

        for id in dataset_ids.iter() {
            self.metadata_repo.borrow_mut().delete_dataset(&id)?;
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} dataset(s)", dataset_ids.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}
