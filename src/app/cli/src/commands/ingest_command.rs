// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use kamu::domain::*;
use opendatafabric::*;

use super::{CLIError, Command};
use crate::OutputConfig;

///////////////////////////////////////////////////////////////////////////////
// Command
///////////////////////////////////////////////////////////////////////////////

pub struct IngestCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    ingest_svc: Arc<dyn IngestService>,
    output_config: Arc<OutputConfig>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    dataset_ref: DatasetRef,
    files_refs: Vec<String>,
    stdin: bool,
    recursive: bool,
}

impl IngestCommand {
    pub fn new<'s, I>(
        dataset_repo: Arc<dyn DatasetRepository>,
        ingest_svc: Arc<dyn IngestService>,
        output_config: Arc<OutputConfig>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
        dataset_ref: DatasetRef,
        files_refs: I,
        stdin: bool,
        recursive: bool,
    ) -> Self
    where
        I: Iterator<Item = &'s str>,
    {
        Self {
            dataset_repo,
            ingest_svc,
            output_config,
            remote_alias_reg,
            dataset_ref,
            files_refs: files_refs.map(|s| s.to_string()).collect(),
            stdin,
            recursive,
        }
    }

    fn path_to_url(path: &str) -> Result<url::Url, CLIError> {
        let p = PathBuf::from(path)
            .canonicalize()
            .map_err(|e| CLIError::usage_error(format!("Ivalid path {}: {}", path, e)))?;
        url::Url::from_file_path(p)
            .map_err(|_| CLIError::usage_error(format!("Ivalid path {}", path)))
    }
}

#[async_trait::async_trait(?Send)]
impl Command for IngestCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        match (self.stdin, !self.files_refs.is_empty()) {
            (false, false) | (true, true) => Err(CLIError::usage_error(
                "Specify a list of files or pass --stdin",
            )),
            _ => Ok(()),
        }?;

        if self.recursive {
            unimplemented!("Sorry, recursive ingest is not yet implemented")
        }

        let dataset_handle = self
            .dataset_repo
            .resolve_dataset_ref(&self.dataset_ref)
            .await
            .map_err(|e| CLIError::failure(e))?;

        let aliases = self
            .remote_alias_reg
            .get_remote_aliases(&dataset_handle.as_local_ref())
            .await
            .map_err(CLIError::failure)?;
        let pull_aliases: Vec<_> = aliases
            .get_by_kind(RemoteAliasKind::Pull)
            .map(|r| r.to_string())
            .collect();
        if !pull_aliases.is_empty() {
            return Err(CLIError::usage_error(format!(
                "Ingesting data into remote dataset will cause histories to diverge. Existing \
                 pull aliases:\n{}",
                pull_aliases.join("\n- ")
            )));
        }

        let urls = if self.stdin {
            vec![url::Url::parse("file:///dev/fd/0").unwrap()]
        } else {
            self.files_refs
                .iter()
                .map(|path| Self::path_to_url(&path))
                .collect::<Result<Vec<_>, _>>()?
        };

        let listener = if !self.output_config.is_tty {
            None
        } else {
            Some(Arc::new(crate::PrettyIngestProgress::new(
                &dataset_handle,
                Arc::new(indicatif::MultiProgress::new()),
                false,
            )) as Arc<dyn IngestListener>)
        };

        let mut updated = 0;
        for url in urls {
            let result = self
                .ingest_svc
                .push_ingest(&self.dataset_ref, url, listener.clone())
                .await
                .map_err(|e| CLIError::failure(e))?;

            match result {
                IngestResult::UpToDate { .. } => (),
                IngestResult::Updated { .. } => updated += 1,
            }
        }

        if updated != 0 {
            eprintln!("{}", console::style("Dataset updated").green().bold());
        } else {
            eprintln!("{}", console::style("Dataset up-to-date").yellow().bold());
        }

        Ok(())
    }
}
