// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use futures::TryStreamExt;
use internal_error::root_source_message;
use kamu::domain::TenancyConfig;
use kamu::utils::datasets_filtering::filter_datasets_by_local_pattern_with_unmatched_handler;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::{
    DatasetRegistry,
    DeleteDatasetError,
    DeleteDatasetPlan,
    DeleteDatasetPlanningResult,
    DeleteDatasetUseCase,
};

use super::{BatchError, CLIError, Command};
use crate::ConfirmDeleteService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteDatasetsCommand {
    tenancy_config: TenancyConfig,
    dataset_registry: Arc<dyn DatasetRegistry>,
    delete_dataset: Arc<dyn DeleteDatasetUseCase>,
    confirm_delete_service: Arc<ConfirmDeleteService>,
    current_account_subject: Arc<CurrentAccountSubject>,

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
        confirm_delete_service: Arc<ConfirmDeleteService>,
        current_account_subject: Arc<CurrentAccountSubject>,
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
            confirm_delete_service,
            current_account_subject,
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
                planning_result: DeleteDatasetPlanningResult::default(),
                ignored_patterns,
            });
        }

        let planning_result = self
            .delete_dataset
            .plan_delete(dataset_handles, self.recursive)
            .await?;

        Ok(PreparedDeleteDatasets {
            planning_result,
            ignored_patterns,
        })
    }

    pub(super) async fn validate_and_prepare(&self) -> Result<PreparedDeleteDatasets, CLIError> {
        self.validate_args().await?;
        self.prepare_targets().await
    }

    pub(super) async fn run_prepared(
        &self,
        prepared: PreparedDeleteDatasets,
    ) -> Result<(), CLIError> {
        let PreparedDeleteDatasets {
            planning_result,
            ignored_patterns,
        } = prepared;

        let mut summary = DeleteDatasetsSummary::default();

        for ignored_pattern in ignored_patterns {
            summary.record_ignored();
            DeleteDatasetsPrinter::print_ignored(&ignored_pattern, self.dry_run);
        }

        self.report_force_allowed_orphans(&planning_result, &mut summary);

        self.ensure_plan_is_executable(&planning_result)?;

        let plan = planning_result.into_executable_plan(self.force)?;

        if plan.authorized_targets.is_empty() {
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
            let dataset_handles: Vec<_> = plan
                .authorized_targets
                .iter()
                .map(|target| target.dataset_handle.clone())
                .collect();
            self.confirm_delete_service
                .confirm_delete(&dataset_handles)
                .await?;
        }

        self.execute_delete(plan, &mut summary).await?;

        DeleteDatasetsPrinter::print_summary(&summary, self.dry_run);

        Ok(())
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
                    if let Some(account_name) = self.current_account_subject.maybe_account_name() {
                        // Narrow down to datasets owned by the current account.
                        // Note: this will not include datasets shared with the account.
                        // The user would have to delete those not owned, but accessible,
                        // with an explicit command.
                        self.dataset_registry
                            .all_dataset_handles_by_owner_name(account_name)
                            .try_collect()
                            .await?
                    } else {
                        unreachable!("In a CLI setup, we should always have an account name")
                    }
                }
            };

            Ok(PreparedDeleteInputs {
                dataset_handles,
                ignored_patterns: Vec::new(),
            })
        } else {
            // Evaluate patterns/refs and track all unmatched patterns.
            // Exact refs that don't match anything are treated as errors unless
            // `--ignore-not-found` is set.
            let mut ignored_patterns = Vec::new();
            let dataset_handles = filter_datasets_by_local_pattern_with_unmatched_handler(
                self.dataset_registry.as_ref(),
                self.dataset_ref_patterns.clone(),
                |pattern, maybe_error| {
                    ignored_patterns.push(pattern.to_string());

                    if !self.ignore_not_found
                        && let Some(error) = maybe_error
                    {
                        return Err(error);
                    }

                    Ok(())
                },
            )
            .try_collect()
            .await?;

            Ok(PreparedDeleteInputs {
                dataset_handles,
                ignored_patterns,
            })
        }
    }

    fn report_force_allowed_orphans(
        &self,
        planning_result: &DeleteDatasetPlanningResult,
        summary: &mut DeleteDatasetsSummary,
    ) {
        if !self.force {
            return;
        }

        if self.recursive {
            let count = planning_result.issues.inaccessible_downstream_handles.len();
            if count > 0 {
                summary.record_ignored_count(count);
                DeleteDatasetsPrinter::print_inaccessible_downstream_count(count, self.dry_run);
            }
        }

        let mut seen_dataset_ids = HashSet::new();
        for dangling_reference in &planning_result.issues.directly_dangling_references {
            for dataset_handle in &dangling_reference.children {
                if seen_dataset_ids.insert(dataset_handle.id.clone()) {
                    summary.record_ignored();
                    DeleteDatasetsPrinter::print_left_behind(dataset_handle, self.dry_run);
                }
            }
        }
    }

    fn ensure_plan_is_executable(
        &self,
        planning_result: &DeleteDatasetPlanningResult,
    ) -> Result<(), CLIError> {
        if !planning_result
            .issues
            .unauthorized_selected_handles
            .is_empty()
        {
            return Err(BatchError::new(
                "Some selected dataset(s) cannot be deleted",
                planning_result
                    .issues
                    .unauthorized_selected_handles
                    .iter()
                    .map(|(dataset_handle, error)| {
                        (
                            std::io::Error::other(root_source_message(error)),
                            dataset_handle.alias.to_string(),
                        )
                    }),
            )
            .into());
        }

        if !self.force
            && !planning_result
                .issues
                .directly_dangling_references
                .is_empty()
        {
            return Err(BatchError::new(
                "Some dataset(s) cannot be deleted due to dangling references",
                planning_result
                    .issues
                    .directly_dangling_references
                    .iter()
                    .map(|error| {
                        (
                            error.clone(),
                            format!("Dataset {}", error.dataset_handle.alias),
                        )
                    }),
            )
            .into());
        }

        if !self.force {
            let count = planning_result.issues.inaccessible_downstream_handles.len();
            if count > 0 {
                return Err(CLIError::usage_error(format!(
                    "Recursive delete would affect {count} downstream dataset(s) you cannot \
                     delete. Re-run with --force to proceed and leave them behind."
                )));
            }
        }

        Ok(())
    }

    async fn execute_delete(
        &self,
        plan: DeleteDatasetPlan,
        summary: &mut DeleteDatasetsSummary,
    ) -> Result<(), CLIError> {
        if self.dry_run {
            for target in &plan.authorized_targets {
                summary.record_deleted();
                DeleteDatasetsPrinter::print_success(&target.dataset_handle, true);
            }
            return Ok(());
        }

        let execution_summary =
            self.delete_dataset
                .execute_plan(plan)
                .await
                .map_err(|e| match e {
                    DeleteDatasetError::Internal(e) => CLIError::critical(e),
                })?;

        for dataset_handle in &execution_summary.deleted_dataset_handles {
            summary.record_deleted();
            DeleteDatasetsPrinter::print_success(dataset_handle, false);
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
        let prepared = self.prepare_targets().await?;
        self.run_prepared(prepared).await
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
pub(super) struct PreparedDeleteDatasets {
    planning_result: DeleteDatasetPlanningResult,
    ignored_patterns: Vec<String>,
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

    fn record_ignored_count(&mut self, count: usize) {
        self.total_items += count;
        self.ignored += count;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteDatasetsPrinter;

impl DeleteDatasetsPrinter {
    fn print_success(dataset_handle: &odf::DatasetHandle, dry_run: bool) {
        let label = if dry_run {
            console::style("Would delete").cyan().bold()
        } else {
            console::style("Deleted").green().bold()
        };

        eprintln!("{label}: {}", dataset_handle.alias);
    }

    fn print_inaccessible_downstream_count(count: usize, dry_run: bool) {
        let label = if dry_run {
            "Would leave behind"
        } else {
            "Left behind"
        };

        eprintln!(
            "{}: {count} inaccessible downstream dataset(s)",
            console::style(label).yellow().bold(),
        );
    }

    fn print_left_behind(dataset_handle: &odf::DatasetHandle, dry_run: bool) {
        let label = if dry_run {
            "Would leave behind"
        } else {
            "Left behind"
        };

        eprintln!(
            "{}: {}",
            console::style(label).yellow().bold(),
            dataset_handle.alias
        );
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
