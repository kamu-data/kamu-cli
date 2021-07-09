use super::{Command, Error};
use kamu::domain::*;

use std::sync::Arc;

pub struct RemoteDeleteCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    names: Vec<String>,
    all: bool,
    no_confirmation: bool,
}

impl RemoteDeleteCommand {
    pub fn new<I, S>(
        metadata_repo: Arc<dyn MetadataRepository>,
        names: I,
        all: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            metadata_repo: metadata_repo,
            names: names.map(|s| s.as_ref().to_owned()).collect(),
            all: all,
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

impl Command for RemoteDeleteCommand {
    fn run(&mut self) -> Result<(), Error> {
        let remote_ids: Vec<RemoteIDBuf> = if self.all {
            self.metadata_repo.get_all_remotes().collect()
        } else {
            self.names.clone()
        };

        let confirmed = if self.no_confirmation {
            true
        } else {
            self.prompt_yes_no(&format!(
                "{}: {}\nDo you whish to continue? [y/N]: ",
                console::style("You are about to delete following remote(s)").yellow(),
                remote_ids.join(", "),
            ))
        };

        if !confirmed {
            return Err(Error::Aborted);
        }

        for id in remote_ids.iter() {
            self.metadata_repo.delete_remote(id)?;
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} remote(s)", remote_ids.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}
