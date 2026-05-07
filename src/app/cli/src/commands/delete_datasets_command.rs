// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use internal_error::ResultIntoInternal;
use kamu::domain::TenancyConfig;
use kamu::utils::datasets_filtering::filter_datasets_by_local_pattern;
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac::RebacServiceExt;
use kamu_datasets::{
    DatasetRegistry,
    DeleteDatasetError,
    DeleteDatasetUseCase,
    DependencyGraphService,
    DependencyOrder,
};

use super::{CLIError, Command};
use crate::ConfirmDeleteService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteDatasetsCommand {
    tenancy_config: TenancyConfig,
    dataset_registry: Arc<dyn DatasetRegistry>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    confirm_delete_service: Arc<ConfirmDeleteService>,
    current_account_subject: Arc<CurrentAccountSubject>,
    rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,

    dataset_ref_patterns: Vec<odf::DatasetRefPattern>,
    all: bool,
    recursive: bool,
    force: bool,
    ignore_not_found: bool,
    dry_run: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeleteDatasetsCommand {
    pub fn new(
        tenancy_config: TenancyConfig,
        dataset_registry: Arc<dyn DatasetRegistry>,
        delete_dataset: Arc<dyn DeleteDatasetUseCase>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        confirm_delete_service: Arc<ConfirmDeleteService>,
        current_account_subject: Arc<CurrentAccountSubject>,
        rebac_service: Arc<dyn kamu_auth_rebac::RebacService>,
        dataset_ref_patterns: Vec<odf::DatasetRefPattern>,
        all: bool,
        recursive: bool,
        force: bool,
        ignore_not_found: bool,
        dry_run: bool,
    ) -> Self {
        Self {
            tenancy_config,
            dataset_registry,
            delete_dataset,
            dependency_graph_service,
            confirm_delete_service,
            current_account_subject,
            rebac_service,
            dataset_ref_patterns,
            all,
            recursive,
            force,
            ignore_not_found,
            dry_run,
        }
    }

    async fn prepare_targets(&self) -> Result<PreparedDeleteDatasets, CLIError> {
        let PreparedDeleteInputs {
            dataset_handles,
            ignored_patterns,
        } = self.prepare_input_handles().await?;

        if dataset_handles.is_empty() {
            return Ok(PreparedDeleteDatasets {
                targets: Vec::new(),
                ignored_patterns,
            });
        }

        let dataset_handles = self.expand_recursive_handles(dataset_handles).await?;
        let targets = self.order_delete_targets(dataset_handles).await?;

        Ok(PreparedDeleteDatasets {
            targets,
            ignored_patterns,
        })
    }

    async fn prepare_input_handles(&self) -> Result<PreparedDeleteInputs, CLIError> {
        if self.all {
            let dataset_handles = match self.tenancy_config {
                TenancyConfig::SingleTenant => {
                    self.dataset_registry
                        .all_dataset_handles()
                        .try_collect()
                        .await?
                }
                TenancyConfig::MultiTenant => {
                    let is_admin = match self.current_account_subject.as_ref() {
                        CurrentAccountSubject::Logged(account) => self
                            .rebac_service
                            .is_account_admin(&account.account_id)
                            .await
                            .int_err()?,
                        CurrentAccountSubject::Anonymous(_) => false,
                    };

                    if is_admin {
                        self.dataset_registry
                            .all_dataset_handles()
                            .try_collect()
                            .await?
                    } else if let Some(account_name) =
                        self.current_account_subject.maybe_account_name()
                    {
                        self.dataset_registry
                            .all_dataset_handles_by_owner_name(account_name)
                            .try_collect()
                            .await?
                    } else {
                        Vec::new()
                    }
                }
            };

            return Ok(PreparedDeleteInputs {
                dataset_handles,
                ignored_patterns: Vec::new(),
            });
        }

        if !self.ignore_not_found {
            return Ok(PreparedDeleteInputs {
                dataset_handles: filter_datasets_by_local_pattern(
                    self.dataset_registry.as_ref(),
                    self.dataset_ref_patterns.clone(),
                )
                .try_collect()
                .await?,
                ignored_patterns: Vec::new(),
            });
        }

        let all_dataset_handles: Vec<_> = self
            .dataset_registry
            .all_dataset_handles()
            .try_collect()
            .await?;

        let mut dataset_handles = Vec::new();
        let mut ignored_patterns = Vec::new();

        for pattern in &self.dataset_ref_patterns {
            let matched_handles = all_dataset_handles
                .iter()
                .filter(|dataset_handle| pattern.matches(dataset_handle))
                .cloned()
                .collect::<Vec<_>>();

            if pattern.is_pattern() {
                dataset_handles.extend(matched_handles);
            } else if let Some(dataset_handle) = matched_handles.into_iter().next() {
                dataset_handles.push(dataset_handle);
            } else {
                ignored_patterns.push(pattern.to_string());
            }
        }

        Ok(PreparedDeleteInputs {
            dataset_handles,
            ignored_patterns,
        })
    }

    async fn expand_recursive_handles(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
    ) -> Result<Vec<odf::DatasetHandle>, CLIError> {
        if !self.recursive {
            return Ok(dataset_handles);
        }

        let dataset_ids = self
            .dependency_graph_service
            .get_recursive_downstream_dependencies(
                dataset_handles.into_iter().map(|h| h.id).collect(),
            )
            .await
            .int_err()?
            .map(Cow::Owned)
            .collect::<Vec<_>>()
            .await;

        let resolution = self
            .dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&dataset_ids)
            .await
            .int_err()?;

        if let Some((_, error)) = resolution.unresolved_datasets.into_iter().next() {
            return Err(error.into());
        }

        Ok(resolution.resolved_handles)
    }

    async fn order_delete_targets(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
    ) -> Result<Vec<DeleteDatasetTarget>, CLIError> {
        tracing::info!(?dataset_handles, "Trying to define delete order");

        let dataset_ids = self
            .dependency_graph_service
            .in_dependency_order(
                dataset_handles.iter().map(|h| h.id.clone()).collect(),
                DependencyOrder::DepthFirst,
            )
            .await
            .map_err(CLIError::critical)?;

        tracing::info!(?dataset_ids, "Delete order defined");

        let mut handles_by_id: BTreeMap<odf::DatasetID, odf::DatasetHandle> = dataset_handles
            .into_iter()
            .map(|dataset_handle| (dataset_handle.id.clone(), dataset_handle))
            .collect();

        Ok(dataset_ids
            .into_iter()
            .map(|dataset_id| DeleteDatasetTarget {
                handle: handles_by_id
                    .remove(&dataset_id)
                    .expect("Dependency ordering must return only prepared dataset IDs"),
            })
            .collect())
    }

    async fn execute_delete(
        &self,
        targets: Vec<DeleteDatasetTarget>,
        summary: &mut DeleteDatasetsSummary,
    ) -> Result<(), CLIError> {
        for target in targets {
            if self.dry_run {
                summary.record_deleted();
                DeleteDatasetsPrinter::print_success(&target, true);
                continue;
            }

            match self
                .delete_dataset
                .execute_via_ref(&target.handle.as_local_ref())
                .await
            {
                Ok(()) => {
                    summary.record_deleted();
                    DeleteDatasetsPrinter::print_success(&target, false);
                }
                Err(DeleteDatasetError::DanglingReference(error)) => {
                    return Err(CLIError::failure(error));
                }
                Err(DeleteDatasetError::Access(error)) => {
                    return Err(CLIError::failure(error));
                }
                Err(DeleteDatasetError::NotFound(error)) => {
                    return Err(CLIError::failure(error));
                }
                Err(DeleteDatasetError::Internal(error)) => {
                    return Err(CLIError::critical(error));
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for DeleteDatasetsCommand {
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

    async fn run(&self) -> Result<(), CLIError> {
        let PreparedDeleteDatasets {
            targets,
            ignored_patterns,
        } = self.prepare_targets().await?;

        let mut summary = DeleteDatasetsSummary::default();

        for ignored_pattern in ignored_patterns {
            summary.record_ignored();
            DeleteDatasetsPrinter::print_ignored(&ignored_pattern, self.dry_run);
        }

        if targets.is_empty() {
            if summary.total_items == 0 {
                eprintln!(
                    "{}",
                    console::style("There are no datasets matching the pattern").yellow()
                );
                return Ok(());
            }

            DeleteDatasetsPrinter::print_summary(&summary, self.dry_run);
            return Ok(());
        }

        if !self.dry_run && !self.force {
            let dataset_handles: Vec<_> =
                targets.iter().map(|target| target.handle.clone()).collect();
            self.confirm_delete_service
                .confirm_delete(&dataset_handles)
                .await?;
        }

        self.execute_delete(targets, &mut summary).await?;

        DeleteDatasetsPrinter::print_summary(&summary, self.dry_run);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct PreparedDeleteInputs {
    dataset_handles: Vec<odf::DatasetHandle>,
    ignored_patterns: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct PreparedDeleteDatasets {
    targets: Vec<DeleteDatasetTarget>,
    ignored_patterns: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct DeleteDatasetTarget {
    handle: odf::DatasetHandle,
}

impl DeleteDatasetTarget {
    fn display_name(&self) -> String {
        self.handle.alias.to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct DeleteDatasetsSummary {
    total_items: usize,
    deleted: usize,
    ignored: usize,
}

impl DeleteDatasetsSummary {
    fn record_deleted(&mut self) {
        self.total_items += 1;
        self.deleted += 1;
    }

    fn record_ignored(&mut self) {
        self.total_items += 1;
        self.ignored += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteDatasetsPrinter;

impl DeleteDatasetsPrinter {
    fn print_success(target: &DeleteDatasetTarget, dry_run: bool) {
        let label = if dry_run {
            console::style("Would delete").cyan().bold()
        } else {
            console::style("Deleted").green().bold()
        };

        eprintln!("{label}: {}", target.display_name());
    }

    fn print_ignored(selector: &str, dry_run: bool) {
        let label = if dry_run {
            "Ignored (dry-run)"
        } else {
            "Ignored"
        };

        eprintln!("{}: {selector}", console::style(label).yellow().bold());
    }

    fn print_summary(summary: &DeleteDatasetsSummary, dry_run: bool) {
        let deleted_label = if dry_run { "would delete" } else { "deleted" };

        eprintln!(
            "{} {} item(s): {} {}, {} ignored, 0 failed",
            console::style("Summary").bold(),
            summary.total_items,
            summary.deleted,
            deleted_label,
            summary.ignored,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
