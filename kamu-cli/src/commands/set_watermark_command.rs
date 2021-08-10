use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::*;

use chrono::DateTime;
use std::sync::Arc;

pub struct SetWatermarkCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    pull_svc: Arc<dyn PullService>,
    refs: Vec<String>,
    all: bool,
    recursive: bool,
    watermark: String,
}

impl SetWatermarkCommand {
    pub fn new<I, S, S2>(
        metadata_repo: Arc<dyn MetadataRepository>,
        pull_svc: Arc<dyn PullService>,
        ids: I,
        all: bool,
        recursive: bool,
        watermark: S2,
    ) -> Self
    where
        I: Iterator<Item = S>,
        S: AsRef<str>,
        S2: AsRef<str>,
    {
        Self {
            metadata_repo,
            pull_svc,
            refs: ids.map(|s| s.as_ref().to_owned()).collect(),
            all,
            recursive,
            watermark: watermark.as_ref().to_owned(),
        }
    }
}

impl Command for SetWatermarkCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        if self.refs.len() != 1 {
            return Err(CLIError::UsageError {
                msg: "Only one dataset can be provided when setting a watermark".to_owned(),
            });
        } else if self.recursive || self.all {
            return Err(CLIError::UsageError {
                msg: "Can't use all or recursive flags when setting a watermark".to_owned(),
            });
        }

        let watermark =
            DateTime::parse_from_rfc3339(&self.watermark).map_err(|_| CLIError::UsageError {
                msg: format!(
                    "Invalid timestamp {} should follow RFC3339 format, e.g. 2020-01-01T12:00:00Z",
                    self.watermark
                ),
            })?;

        let dataset_id = DatasetRef::try_from(&self.refs[0]).unwrap().local_id();

        let aliases = self.metadata_repo.get_remote_aliases(dataset_id)?;
        let pull_aliases: Vec<_> = aliases
            .get_by_kind(RemoteAliasKind::Pull)
            .map(|r| r.as_str())
            .collect();

        if !pull_aliases.is_empty() {
            return Err(CLIError::UsageError {
                msg: format!(
                    "Setting watermark on a remote dataset will cause histories to diverge. Existing pull aliases:\n{}",
                    pull_aliases.join("\n- ")
                ),
            });
        }

        match self.pull_svc.set_watermark(dataset_id, watermark.into()) {
            Ok(PullResult::UpToDate) => {
                eprintln!("{}", console::style("Watermark was up-to-date").yellow());
                Ok(())
            }
            Ok(PullResult::Updated { new_head, .. }) => {
                eprintln!(
                    "{}",
                    console::style(format!("Committed new block {}", new_head.short())).green()
                );
                Ok(())
            }
            Err(e) => Err(DomainError::InfraError(e.into()).into()),
        }
    }
}
