// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::{future, StreamExt, TryStreamExt};
use kamu::domain::*;
use kamu::utils::datasets_filtering::filter_datasets_by_local_pattern;
use opendatafabric::*;

use super::{common, CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteCommand {
    dataset_repo: Arc<dyn DatasetRepository>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    dataset_ref_patterns: Vec<DatasetRefPattern>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    all: bool,
    recursive: bool,
    no_confirmation: bool,
}

impl DeleteCommand {
    pub fn new<I>(
        dataset_repo: Arc<dyn DatasetRepository>,
        delete_dataset: Arc<dyn DeleteDatasetUseCase>,
        dataset_ref_patterns: I,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        all: bool,
        recursive: bool,
        no_confirmation: bool,
    ) -> Self
    where
        I: IntoIterator<Item = DatasetRefPattern>,
    {
        Self {
            dataset_repo,
            delete_dataset,
            dataset_ref_patterns: dataset_ref_patterns.into_iter().collect(),
            dependency_graph_service,
            all,
            recursive,
            no_confirmation,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for DeleteCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        if self.dataset_ref_patterns.is_empty() && !self.all {
            return Err(CLIError::usage_error("Specify a dataset or use --all flag"));
        }

        let dataset_ids: Vec<_> = if self.all {
            self.dataset_repo
                .get_all_datasets()
                .map_ok(|dataset_handle| dataset_handle.id)
                .try_collect()
                .await?
        } else {
            filter_datasets_by_local_pattern(
                self.dataset_repo.as_ref(),
                self.dataset_ref_patterns.clone(),
            )
            .map_ok(|dataset_handle| dataset_handle.id)
            .try_collect()
            .await?
        };

        let dataset_refs: Vec<_> = if self.recursive
            || self.all
            || self
                .dataset_ref_patterns
                .contains(&DatasetRefPattern::Pattern(DatasetAliasPattern::new(
                    None,
                    DatasetNamePattern::new_unchecked("%"),
                ))) {
            self.dependency_graph_service
                .get_recursive_downstream_dependencies(dataset_ids)
                .await
                .int_err()?
                .map(DatasetRef::ID)
                .collect()
                .await
        } else {
            dataset_ids
                .iter()
                .map(|dataset_id| DatasetRef::ID(dataset_id.clone()))
                .collect()
        };

        if dataset_refs.is_empty() {
            eprintln!(
                "{}",
                console::style("There are no datasets matching the pattern").yellow()
            );
            return Ok(());
        }

        let confirmed = if self.no_confirmation {
            true
        } else {
            let dataset_aliases = future::join_all(dataset_refs.iter().map(|dataset_id| async {
                let dataset_hdl = self
                    .dataset_repo
                    .resolve_dataset_ref(dataset_id)
                    .await
                    .unwrap();
                dataset_hdl.alias.to_string()
            }))
            .await;

            common::prompt_yes_no(&format!(
                "{}\n  {}\n{}\nDo you wish to continue? [y/N]: ",
                console::style("You are about to delete following dataset(s):").yellow(),
                dataset_aliases.join("\n  "),
                console::style("This operation is irreversible!").yellow(),
            ))
        };

        if !confirmed {
            return Err(CLIError::Aborted);
        }

        for dataset_ref in &dataset_refs {
            match self.delete_dataset.execute(dataset_ref).await {
                Ok(_) => Ok(()),
                Err(DeleteDatasetError::DanglingReference(e)) => Err(CLIError::failure(e)),
                Err(DeleteDatasetError::Access(e)) => Err(CLIError::failure(e)),
                Err(e) => Err(CLIError::critical(e)),
            }?;
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} dataset(s)", dataset_refs.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
