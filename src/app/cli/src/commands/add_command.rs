// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::*;
use opendatafabric::*;

use super::{common, BatchError, CLIError, Command};
use crate::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AddCommand {
    resource_loader: Arc<dyn ResourceLoader>,
    dataset_repo: Arc<dyn DatasetRepository>,
    snapshot_refs: Vec<String>,
    recursive: bool,
    replace: bool,
    stdin: bool,
    output_config: Arc<OutputConfig>,
}

impl AddCommand {
    pub fn new<'s, I>(
        resource_loader: Arc<dyn ResourceLoader>,
        dataset_repo: Arc<dyn DatasetRepository>,
        snapshot_refs_iter: I,
        recursive: bool,
        replace: bool,
        stdin: bool,
        output_config: Arc<OutputConfig>,
    ) -> Self
    where
        I: Iterator<Item = &'s str>,
    {
        Self {
            resource_loader,
            dataset_repo,
            snapshot_refs: snapshot_refs_iter.map(ToOwned::to_owned).collect(),
            recursive,
            replace,
            stdin,
            output_config,
        }
    }

    async fn load_specific(&self) -> Vec<(String, Result<DatasetSnapshot, ResourceError>)> {
        let mut res = Vec::new();
        for r in &self.snapshot_refs {
            let load = self.resource_loader.load_dataset_snapshot_from_ref(r).await;
            res.push((r.clone(), load));
        }
        res
    }

    async fn load_recursive(&self) -> Vec<(String, Result<DatasetSnapshot, ResourceError>)> {
        let files_iter = self
            .snapshot_refs
            .iter()
            .map(|r| std::path::Path::new(r).join("**").join("*.yaml"))
            .flat_map(|p| {
                glob::glob(p.to_str().unwrap())
                    .unwrap_or_else(|e| panic!("Failed to read glob {}: {}", p.display(), e))
            })
            .map(Result::unwrap)
            .filter(|p| {
                self.is_snapshot_file(p)
                    .unwrap_or_else(|e| panic!("Error while reading file {}: {}", p.display(), e))
            });

        let mut res = Vec::new();
        for path in files_iter {
            let load = self
                .resource_loader
                .load_dataset_snapshot_from_path(&path)
                .await;
            res.push((path.to_str().unwrap().to_owned(), load));
        }
        res
    }

    fn load_stdin(&self) -> Vec<(String, Result<DatasetSnapshot, ResourceError>)> {
        match opendatafabric::serde::yaml::YamlDatasetSnapshotDeserializer
            .read_manifests(std::io::stdin())
        {
            Ok(v) => v
                .into_iter()
                .enumerate()
                .map(|(i, ds)| (format!("STDIN[{i}]"), Ok(ds)))
                .collect(),
            Err(e) => vec![("STDIN".to_string(), Err(ResourceError::serde(e)))],
        }
    }

    fn is_snapshot_file(&self, path: &std::path::Path) -> Result<bool, std::io::Error> {
        use std::io::BufRead;
        let file = std::fs::File::open(path)?;

        let mut lines_read = 0;

        for line_res in std::io::BufReader::new(file).lines() {
            let line = line_res?;
            let line = line.trim();

            // Allow any number of comments in (what is supposedly) a YAML file
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if line.contains("DatasetSnapshot") {
                return Ok(true);
            }

            lines_read += 1;
            if lines_read > 3 {
                break;
            }
        }

        Ok(false)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for AddCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        if self.stdin && !self.snapshot_refs.is_empty() {
            return Err(CLIError::usage_error(
                "Cannot specify --stdin and positional arguments at the same time",
            ));
        }
        if !self.stdin && self.snapshot_refs.is_empty() {
            return Err(CLIError::usage_error(
                "No manifest references or paths were provided",
            ));
        }

        let load_results = if self.recursive {
            self.load_recursive().await
        } else if self.stdin {
            self.load_stdin()
        } else {
            self.load_specific().await
        };

        let (snapshots, mut errors): (Vec<_>, Vec<_>) =
            load_results.into_iter().partition(|(_, r)| r.is_ok());
        let snapshots: Vec<_> = snapshots.into_iter().map(|(_, r)| r.unwrap()).collect();

        if !errors.is_empty() {
            errors.sort_by(|(a, _), (b, _)| a.cmp(b));
            return Err(BatchError::new(
                "Failed to load manifests",
                errors
                    .into_iter()
                    .map(|(p, r)| (r.unwrap_err(), format!("Failed to load from {p}"))),
            )
            .into());
        }

        // Delete existing datasets if we are replacing
        if self.replace {
            let mut already_exist = Vec::new();
            for s in &snapshots {
                if let Some(hdl) = self
                    .dataset_repo
                    .try_resolve_dataset_ref(&s.name.as_local_ref())
                    .await?
                {
                    already_exist.push(hdl);
                }
            }

            if !already_exist.is_empty() {
                let confirmed = common::prompt_yes_no(&format!(
                    "{}: {}\n{}\nDo you wish to continue? [y/N]: ",
                    console::style("You are about to replace following dataset(s)").yellow(),
                    already_exist
                        .iter()
                        .map(|h| h.alias.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    console::style("This operation is irreversible!").yellow(),
                ));

                if !confirmed {
                    return Err(CLIError::Aborted);
                }

                // TODO: delete permissions should be checked in multi-tenant scenario
                for hdl in already_exist {
                    self.dataset_repo
                        .delete_dataset(&hdl.as_local_ref())
                        .await?;
                }
            }
        };

        let mut add_results = self
            .dataset_repo
            .create_datasets_from_snapshots(snapshots)
            .await;

        add_results.sort_by(|(id_a, _), (id_b, _)| id_a.cmp(id_b));

        let mut num_added = 0;

        let mut errors_with_contexts = Vec::new();

        for (id, res) in add_results {
            match res {
                Ok(_) => {
                    num_added += 1;

                    if !self.output_config.quiet {
                        eprintln!("{}: {}", console::style("Added").green(), id);
                    }
                }
                Err(CreateDatasetFromSnapshotError::NameCollision(_)) => {
                    if !self.output_config.quiet {
                        eprintln!(
                            "{}: {}: Already exists",
                            console::style("Skipped").yellow(),
                            id
                        );
                    }
                }
                Err(err) => {
                    errors_with_contexts.push((err, format!("Failed to add dataset {id}")));
                }
            }
        }

        if errors_with_contexts.is_empty() {
            if !self.output_config.quiet {
                eprintln!(
                    "{}",
                    console::style(format!("Added {num_added} dataset(s)"))
                        .green()
                        .bold()
                );
            }

            Ok(())
        } else {
            Err(BatchError::new(
                format!("Failed to load {} manifest(s)", errors_with_contexts.len()),
                errors_with_contexts,
            )
            .into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
