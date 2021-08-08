use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::RepositoryID;

use std::sync::Arc;
use url::Url;

pub struct RepositoryAddCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    name: String,
    url: String,
}

impl RepositoryAddCommand {
    pub fn new(metadata_repo: Arc<dyn MetadataRepository>, name: &str, url: &str) -> Self {
        Self {
            metadata_repo: metadata_repo,
            name: name.to_owned(),
            url: url.to_owned(),
        }
    }
}

impl Command for RepositoryAddCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        let url = Url::parse(&self.url).map_err(|e| {
            eprintln!("{}: {}", console::style("Invalid URL").red(), e);
            CLIError::Aborted
        })?;

        self.metadata_repo
            .add_repository(RepositoryID::try_from(&self.name).unwrap(), url)?;

        eprintln!("{}: {}", console::style("Added").green(), &self.name);
        Ok(())
    }
}
