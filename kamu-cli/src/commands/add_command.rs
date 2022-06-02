// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::common;
use super::{BatchError, CLIError, Command};
use kamu::domain::*;
use opendatafabric::*;

use std::io::Read;
use std::sync::Arc;

pub struct AddCommand {
    resource_loader: Arc<dyn ResourceLoader>,
    local_repo: Arc<dyn LocalDatasetRepository>,
    snapshot_refs: Vec<String>,
    recursive: bool,
    replace: bool,
}

impl AddCommand {
    pub fn new<'s, I>(
        resource_loader: Arc<dyn ResourceLoader>,
        local_repo: Arc<dyn LocalDatasetRepository>,
        snapshot_refs_iter: I,
        recursive: bool,
        replace: bool,
    ) -> Self
    where
        I: Iterator<Item = &'s str>,
    {
        Self {
            resource_loader,
            local_repo,
            snapshot_refs: snapshot_refs_iter.map(|s| s.to_owned()).collect(),
            recursive,
            replace,
        }
    }

    fn load_specific(&self) -> Vec<(String, Result<DatasetSnapshot, ResourceError>)> {
        self.snapshot_refs
            .iter()
            .map(|r| {
                (
                    r.clone(),
                    self.resource_loader.load_dataset_snapshot_from_ref(r),
                )
            })
            .collect()
    }

    fn load_recursive(&self) -> Vec<(String, Result<DatasetSnapshot, ResourceError>)> {
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
                (
                    p.to_str().unwrap().to_owned(),
                    self.resource_loader.load_dataset_snapshot_from_path(&p),
                )
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

#[async_trait::async_trait(?Send)]
impl Command for AddCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let load_results = if !self.recursive {
            self.load_specific()
        } else {
            self.load_recursive()
        };

        let (snapshots, mut errors): (Vec<_>, Vec<_>) =
            load_results.into_iter().partition(|(_, r)| r.is_ok());
        let snapshots: Vec<_> = snapshots.into_iter().map(|(_, r)| r.unwrap()).collect();

        if errors.len() != 0 {
            errors.sort_by(|(a, _), (b, _)| a.cmp(&b));
            return Err(BatchError::new(
                "Failed to load manifests",
                errors
                    .into_iter()
                    .map(|(p, r)| (r.unwrap_err(), format!("Failed to load from {}", p))),
            )
            .into());
        }

        // Delete existing datasets if we are replacing
        if self.replace {
            let mut already_exist = Vec::new();
            for s in &snapshots {
                if let Some(hdl) = self
                    .local_repo
                    .try_resolve_dataset_ref(&s.name.as_local_ref())
                    .await?
                {
                    already_exist.push(hdl);
                }
            }

            if already_exist.len() != 0 {
                let confirmed = common::prompt_yes_no(&format!(
                    "{}: {}\n{}\nDo you whish to continue? [y/N]: ",
                    console::style("You are about to replace following dataset(s)").yellow(),
                    already_exist
                        .iter()
                        .map(|h| h.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    console::style("This operation is irreversible!").yellow(),
                ));

                if !confirmed {
                    return Err(CLIError::Aborted);
                }

                for hdl in already_exist {
                    self.local_repo.delete_dataset(&hdl.as_local_ref()).await?;
                }
            }
        };

        let mut add_results = self
            .local_repo
            .create_datasets_from_snapshots(snapshots)
            .await;

        add_results.sort_by(|(id_a, _), (id_b, _)| id_a.cmp(&id_b));

        let (mut num_added, mut num_errors) = (0, 0);

        for (id, res) in add_results {
            match res {
                Ok(_) => {
                    num_added += 1;
                    eprintln!("{}: {}", console::style("Added").green(), id);
                }
                Err(CreateDatasetFromSnapshotError::NameCollision(_)) => {
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
            Err(CLIError::PartialFailure)
        } else {
            Err(CLIError::Aborted)
        }
    }
}
