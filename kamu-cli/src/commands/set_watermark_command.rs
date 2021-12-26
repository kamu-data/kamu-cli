// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use kamu::domain::*;
use opendatafabric::*;

use chrono::DateTime;
use std::sync::Arc;

pub struct SetWatermarkCommand {
    metadata_repo: Arc<dyn MetadataRepository>,
    pull_svc: Arc<dyn PullService>,
    refs: Vec<DatasetRefAny>,
    all: bool,
    recursive: bool,
    watermark: String,
}

impl SetWatermarkCommand {
    pub fn new<I, R, S>(
        metadata_repo: Arc<dyn MetadataRepository>,
        pull_svc: Arc<dyn PullService>,
        refs: I,
        all: bool,
        recursive: bool,
        watermark: S,
    ) -> Self
    where
        S: Into<String>,
        I: Iterator<Item = R>,
        R: TryInto<DatasetRefAny>,
        <R as TryInto<DatasetRefAny>>::Error: std::fmt::Debug,
    {
        Self {
            metadata_repo,
            pull_svc,
            refs: refs.map(|s| s.try_into().unwrap()).collect(),
            all,
            recursive,
            watermark: watermark.into(),
        }
    }
}

impl Command for SetWatermarkCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        if self.refs.len() != 1 {
            return Err(CLIError::usage_error(
                "Only one dataset can be provided when setting a watermark",
            ));
        } else if self.recursive || self.all {
            return Err(CLIError::usage_error(
                "Can't use --all or --recursive flags when setting a watermark",
            ));
        }

        let watermark = DateTime::parse_from_rfc3339(&self.watermark).map_err(|_| {
            CLIError::usage_error(format!(
                "Invalid timestamp {} should follow RFC3339 format, e.g. 2020-01-01T12:00:00Z",
                self.watermark
            ))
        })?;

        let dataset_ref = self.refs[0]
            .as_local_ref()
            .ok_or_else(|| CLIError::usage_error("Expected a local dataset reference"))?;

        let aliases = self.metadata_repo.get_remote_aliases(&dataset_ref)?;
        let pull_aliases: Vec<_> = aliases
            .get_by_kind(RemoteAliasKind::Pull)
            .map(|r| r.as_str())
            .collect();

        if !pull_aliases.is_empty() {
            return Err(CLIError::usage_error(format!(
                    "Setting watermark on a remote dataset will cause histories to diverge. Existing pull aliases:\n{}",
                    pull_aliases.join("\n- ")
                ))
            );
        }

        match self.pull_svc.set_watermark(&dataset_ref, watermark.into()) {
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
