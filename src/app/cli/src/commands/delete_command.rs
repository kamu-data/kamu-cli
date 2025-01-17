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

use super::{CLIError, Command};
use crate::ConfirmDeleteService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteCommand {
    dataset_registry: Arc<dyn DatasetRegistry>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    dataset_ref_patterns: Vec<odf::DatasetRefPattern>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    confirm_delete_service: Arc<ConfirmDeleteService>,
    all: bool,
    recursive: bool,
}

impl DeleteCommand {
    pub fn new<I>(
        dataset_registry: Arc<dyn DatasetRegistry>,
        delete_dataset: Arc<dyn DeleteDatasetUseCase>,
        dataset_ref_patterns: I,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        confirm_delete_service: Arc<ConfirmDeleteService>,
        all: bool,
        recursive: bool,
    ) -> Self
    where
        I: IntoIterator<Item = odf::DatasetRefPattern>,
    {
        Self {
            dataset_registry,
            delete_dataset,
            dataset_ref_patterns: dataset_ref_patterns.into_iter().collect(),
            dependency_graph_service,
            confirm_delete_service,
            all,
            recursive,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for DeleteCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        match (self.dataset_ref_patterns.as_slice(), self.all) {
            ([], false) => Err(CLIError::usage_error("Specify dataset(s) or pass --all")),
            ([], true) => Ok(()),
            ([_head, ..], false) => Ok(()),
            ([_head, ..], true) => Err(CLIError::usage_error(
                "You can either specify dataset(s) or pass --all",
            )),
        }
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let dataset_handles: Vec<odf::DatasetHandle> = if self.all {
            self.dataset_registry
                .all_dataset_handles()
                .try_collect()
                .await?
        } else {
            filter_datasets_by_local_pattern(
                self.dataset_registry.as_ref(),
                self.dataset_ref_patterns.clone(),
            )
            .try_collect()
            .await?
        };

        let dataset_handles: Vec<odf::DatasetHandle> = if !self.recursive {
            dataset_handles
        } else {
            self.dependency_graph_service
                .get_recursive_downstream_dependencies(
                    dataset_handles.into_iter().map(|h| h.id).collect(),
                )
                .await
                .int_err()?
                .map(odf::DatasetID::into_local_ref)
                .then(|hdl| {
                    let registry = self.dataset_registry.clone();
                    async move { registry.resolve_dataset_handle_by_ref(&hdl).await }
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

        self.confirm_delete_service
            .confirm_delete(&dataset_handles)
            .await?;

        tracing::info!(?dataset_handles, "Trying to define delete order");

        // TODO: Multiple rounds of resolving IDs to handles
        let dataset_ids = self
            .dependency_graph_service
            .in_dependency_order(
                dataset_handles.into_iter().map(|h| h.id).collect(),
                DependencyOrder::DepthFirst,
            )
            .await
            .map_err(CLIError::critical)?;

        tracing::info!(?dataset_ids, "Delete order defined");

        for id in &dataset_ids {
            match self
                .delete_dataset
                .execute_via_ref(&id.as_local_ref())
                .await
            {
                Ok(_) => Ok(()),
                Err(odf::dataset::DeleteDatasetError::DanglingReference(e)) => {
                    Err(CLIError::failure(e))
                }
                Err(odf::dataset::DeleteDatasetError::Access(e)) => Err(CLIError::failure(e)),
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
