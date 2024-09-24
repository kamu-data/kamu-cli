// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use internal_error::ResultIntoInternal;
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

        let dataset_handles: Vec<DatasetHandle> = if self.all {
            self.dataset_repo.get_all_datasets().try_collect().await?
        } else {
            filter_datasets_by_local_pattern(
                self.dataset_repo.as_ref(),
                self.dataset_ref_patterns.clone(),
            )
            .try_collect()
            .await?
        };

        let dataset_handles: Vec<DatasetHandle> = if !self.recursive {
            dataset_handles
        } else {
            self.dependency_graph_service
                .get_recursive_downstream_dependencies(
                    dataset_handles.into_iter().map(|h| h.id).collect(),
                )
                .await
                .int_err()?
                .map(DatasetID::into_local_ref)
                .then(|hdl| {
                    let repo = self.dataset_repo.clone();
                    async move { repo.resolve_dataset_ref(&hdl).await }
                })
                .try_collect()
                .await?
        };

        if dataset_handles.is_empty() {
            eprintln!(
                "{}",
                console::style("There are no datasets matching the pattern").yellow()
            );
            return Ok(());
        }

        let confirmed = if self.no_confirmation {
            true
        } else {
            let dataset_aliases: Vec<String> = dataset_handles
                .iter()
                .map(|h| h.alias.to_string())
                .collect();

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

        // TODO: Multiple rounds of resolving IDs to handles
        let dataset_ids = self
            .dependency_graph_service
            .in_dependency_order(
                dataset_handles.into_iter().map(|h| h.id).collect(),
                DependencyOrder::DepthFirst,
            )
            .await
            .map_err(CLIError::critical)?;

        for id in &dataset_ids {
            match self
                .delete_dataset
                .execute_via_ref(&id.as_local_ref())
                .await
            {
                Ok(_) => Ok(()),
                Err(DeleteDatasetError::DanglingReference(e)) => Err(CLIError::failure(e)),
                Err(DeleteDatasetError::Access(e)) => Err(CLIError::failure(e)),
                Err(e) => Err(CLIError::critical(e)),
            }?;
        }

        eprintln!(
            "{}",
            console::style(format!("Deleted {} dataset(s)", dataset_ids.len()))
                .green()
                .bold()
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
