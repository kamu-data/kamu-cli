use super::{Command, Error};
use kamu::domain::*;

use std::sync::Arc;
use url::Url;

pub struct RemoteAddCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    name: String,
    url: String,
}

impl RemoteAddCommand {
    pub fn new(metadata_repo: Arc<dyn MetadataRepository>, name: &str, url: &str) -> Self {
        Self {
            metadata_repo: metadata_repo,
            name: name.to_owned(),
            url: url.to_owned(),
        }
    }
}

impl Command for RemoteAddCommand {
    fn run(&mut self) -> Result<(), Error> {
        let url = Url::parse(&self.url).map_err(|e| {
            eprintln!("{}: {}", console::style("Invalid URL").red(), e);
            Error::Aborted
        })?;

        self.metadata_repo.add_remote(&self.name, url)?;

        eprintln!("{}: {}", console::style("Added").green(), &self.name);
        Ok(())
    }
}
