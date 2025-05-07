// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::*;
use kamu::domain::*;
use kamu_datasets::{
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCaseOptions,
    CreateDatasetsFromSnapshotsPlanningError,
    CreateDatasetsFromSnapshotsPlanningResult,
    DeleteDatasetUseCase,
};

use super::{BatchError, CLIError, Command};
use crate::{ConfirmDeleteService, OutputConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct AddCommand {
    output_config: Arc<OutputConfig>,
    resource_loader: Arc<dyn ResourceLoader>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    create_dataset_from_snapshot: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    confirm_delete_service: Arc<ConfirmDeleteService>,

    #[dill::component(explicit)]
    snapshot_refs: Vec<String>,

    #[dill::component(explicit)]
    name: Option<odf::DatasetAlias>,

    #[dill::component(explicit)]
    dry_run: bool,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    replace: bool,

    #[dill::component(explicit)]
    stdin: bool,

    #[dill::component(explicit)]
    dataset_visibility: odf::DatasetVisibility,
}

impl AddCommand {
    async fn load_specific(&self) -> Vec<(String, Result<odf::DatasetSnapshot, ResourceError>)> {
        let mut res = Vec::new();
        for r in &self.snapshot_refs {
            let load = self.resource_loader.load_dataset_snapshot_from_ref(r).await;
            res.push((r.clone(), load));
        }
        res
    }

    async fn load_recursive(&self) -> Vec<(String, Result<odf::DatasetSnapshot, ResourceError>)> {
        let files_iter = self
            .snapshot_refs
            .iter()
            .map(|r| std::path::Path::new(r).join("**").join("*.yaml"))
            .flat_map(|p| {
                glob::glob(p.to_str().unwrap())
                    .unwrap_or_else(|e| panic!("Failed to read glob {}: {e}", p.display()))
            })
            .map(Result::unwrap)
            .filter(|p| {
                self.is_snapshot_file(p)
                    .unwrap_or_else(|e| panic!("Error while reading file {}: {e}", p.display()))
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

    fn load_stdin(&self) -> Vec<(String, Result<odf::DatasetSnapshot, ResourceError>)> {
        match odf::metadata::serde::yaml::YamlDatasetSnapshotDeserializer
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for AddCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
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
        if self.name.is_some() && (self.recursive || !(self.snapshot_refs.len() == 1 || self.stdin))
        {
            return Err(CLIError::usage_error(
                "Name override can be used only when adding a single manifest",
            ));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let load_results = if self.recursive {
            self.load_recursive().await
        } else if self.stdin {
            self.load_stdin()
        } else {
            self.load_specific().await
        };

        let (snapshots, mut errors): (Vec<_>, Vec<_>) =
            load_results.into_iter().partition(|(_, r)| r.is_ok());
        let mut snapshots: Vec<_> = snapshots.into_iter().map(|(_, r)| r.unwrap()).collect();

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

        // Rename snapshot if a new name was provided
        if let Some(name) = &self.name
            && snapshots.len() == 1
        {
            snapshots[0].name = name.clone();
        }

        // Delete existing datasets if we are replacing
        // TODO: Move into use case?
        if self.replace {
            let mut already_exist = Vec::new();
            for s in &snapshots {
                if let Some(hdl) = self
                    .dataset_registry
                    .try_resolve_dataset_handle_by_ref(&s.name.as_local_ref())
                    .await?
                {
                    already_exist.push(hdl);
                }
            }

            if !already_exist.is_empty() {
                self.confirm_delete_service
                    .confirm_delete(&already_exist)
                    .await?;

                // TODO: Private Datasets: delete permissions should be checked in multi-tenant
                //                         scenario
                for hdl in already_exist {
                    self.delete_dataset.execute_via_handle(&hdl).await?;
                }
            }
        };

        let CreateDatasetsFromSnapshotsPlanningResult { plan, errors } = match self
            .create_dataset_from_snapshot
            .prepare(
                snapshots,
                CreateDatasetUseCaseOptions {
                    dataset_visibility: self.dataset_visibility,
                },
            )
            .await
        {
            Ok(res) => Ok(res),
            Err(CreateDatasetsFromSnapshotsPlanningError::CyclicDependency(_)) => {
                Err(CLIError::usage_error("Aborted on cyclic dependency"))
            }
            Err(CreateDatasetsFromSnapshotsPlanningError::Internal(err)) => Err(err.into()),
        }?;

        let mut errors_with_contexts = Vec::new();
        for (snaphot, err) in errors {
            match err {
                CreateDatasetFromSnapshotError::NameCollision(_) => {
                    if !self.output_config.quiet {
                        eprintln!(
                            "{}: {}: Already exists",
                            console::style("Skipped").yellow(),
                            snaphot.name,
                        );
                    }
                }
                err => {
                    errors_with_contexts
                        .push((err, format!("Failed to add dataset {}", snaphot.name)));
                }
            }
        }

        if !errors_with_contexts.is_empty() {
            return Err(BatchError::new(
                format!("Failed to add {} manifest(s)", errors_with_contexts.len()),
                errors_with_contexts,
            )
            .into());
        }

        if self.dry_run {
            let plan = serde_yaml::to_string(&plan).int_err()?;
            eprintln!(
                "{}\n{}\n{}",
                console::style("Execution plan:").yellow().bold(),
                console::style(&plan).dim(),
                console::style("Exiting early due to --dry-run")
                    .yellow()
                    .bold()
            );
            return Ok(());
        }

        let mut add_results = self
            .create_dataset_from_snapshot
            .apply(plan)
            .await
            .int_err()?;

        add_results.sort_by(|a, b| a.dataset_handle.alias.cmp(&b.dataset_handle.alias));

        if !self.output_config.quiet {
            for res in &add_results {
                eprintln!(
                    "{}: {}",
                    console::style("Added").green(),
                    res.dataset_handle.alias
                );
            }
        }

        if !self.output_config.quiet {
            eprintln!(
                "{}",
                console::style(format!("Added {} dataset(s)", add_results.len()))
                    .green()
                    .bold()
            );
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
