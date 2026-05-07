// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use internal_error::{BoxedError, InternalError};
use kamu_resources::ResourceUID;
use kamu_resources_facade::{
    BatchResourceError,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceRef,
};

use super::{BatchError, CLIError, Command};
use crate::Interact;
use crate::output::OutputConfig;
use crate::resource_context::{ResolvedResourceContext, ResourceContextReporter};
use crate::resources::{
    ResourceSelectionResolutionOptions,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
    ResourceTarget,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteResourcesCommand {
    resource_facade: Arc<dyn ResourceFacade>,
    resource_selection_resolution_service: Arc<dyn ResourceSelectionResolutionService>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    interact: Arc<Interact>,
    output_config: Arc<OutputConfig>,

    resolved_context: ResolvedResourceContext,
    syntax: ResourceSelectionSyntax,
    force: bool,
    ignore_not_found: bool,
    dry_run: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeleteResourcesCommand {
    pub fn new(
        resource_facade: Arc<dyn ResourceFacade>,
        resource_selection_resolution_service: Arc<dyn ResourceSelectionResolutionService>,
        resource_context_reporter: Arc<ResourceContextReporter>,
        interact: Arc<Interact>,
        output_config: Arc<OutputConfig>,
        resolved_context: ResolvedResourceContext,
        syntax: ResourceSelectionSyntax,
        force: bool,
        ignore_not_found: bool,
        dry_run: bool,
    ) -> Self {
        Self {
            resource_facade,
            resource_selection_resolution_service,
            resource_context_reporter,
            interact,
            output_config,
            resolved_context,
            syntax,
            force,
            ignore_not_found,
            dry_run,
        }
    }

    async fn prepare_targets(&self) -> Result<PreparedDeleteTargets, CLIError> {
        // Resolve once up front so dry-run, confirmation, and delete execution all
        // operate on the same canonical target set.
        let resolved = self
            .resource_selection_resolution_service
            .resolve(
                self.syntax.clone(),
                self.resource_facade.as_ref(),
                ResourceSelectionResolutionOptions {
                    ignore_not_found: self.ignore_not_found,
                    max_expanded_results: None,
                },
            )
            .await?;

        Ok(PreparedDeleteTargets {
            targets: resolved
                .targets
                .into_iter()
                .map(DeleteResourceTarget::from_resource_target)
                .collect(),
            ignored_selectors: resolved
                .ignored_selectors
                .into_iter()
                .map(|selector| selector.selector_input)
                .collect(),
        })
    }

    pub(super) async fn validate_and_prepare(&self) -> Result<PreparedDeleteTargets, CLIError> {
        self.validate_args().await?;
        self.prepare_targets().await
    }

    pub(super) async fn run_prepared(
        &self,
        prepared: PreparedDeleteTargets,
    ) -> Result<(), CLIError> {
        if !self.output_config.quiet {
            self.resource_context_reporter.report_usage(
                if self.dry_run {
                    "Previewing resource deletion in context"
                } else {
                    "Deleting resources from context"
                },
                &self.resolved_context,
            );
        }

        let PreparedDeleteTargets {
            targets,
            ignored_selectors,
        } = prepared;

        let mut summary = DeleteResourcesSummary::default();
        let mut errors = Vec::new();

        for ignored_selector in ignored_selectors {
            summary.record_ignored();
            DeleteResourcesPrinter::print_ignored(
                &ignored_selector,
                self.dry_run,
                self.output_config.as_ref(),
            );
        }

        if targets.is_empty() {
            if summary.total_items == 0 {
                eprintln!(
                    "{}",
                    console::style("There are no matching resources to delete").yellow()
                );
            }
            DeleteResourcesPrinter::print_summary(&summary, self.dry_run);

            if errors.is_empty() {
                return Ok(());
            }

            return Err(BatchError {
                summary: DeleteResourcesPrinter::batch_failure_summary(errors.len(), self.dry_run),
                errors_with_context: errors,
            }
            .into());
        }

        if !self.dry_run {
            self.confirm_delete(&targets)?;
        }

        self.execute_delete_phase(targets, &mut summary, &mut errors)
            .await;

        DeleteResourcesPrinter::print_summary(&summary, self.dry_run);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(BatchError {
                summary: DeleteResourcesPrinter::batch_failure_summary(errors.len(), self.dry_run),
                errors_with_context: errors,
            }
            .into())
        }
    }

    fn confirm_delete(&self, targets: &[DeleteResourceTarget]) -> Result<(), CLIError> {
        if self.force {
            return Ok(());
        }

        let listed_resources = targets
            .iter()
            .map(|target| format!("  {}", target.display_name()))
            .collect::<Vec<_>>()
            .join("\n");

        self.interact.require_confirmation(format!(
            "{}\n{}\n{}",
            console::style("You are about to delete following resource(s):").yellow(),
            listed_resources,
            console::style("This operation is irreversible!").yellow(),
        ))
    }

    async fn execute_delete_phase(
        &self,
        targets: Vec<DeleteResourceTarget>,
        summary: &mut DeleteResourcesSummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) {
        if self.dry_run {
            for target in targets {
                summary.record_deleted();
                DeleteResourcesPrinter::print_success(&target, true, self.output_config.as_ref());
            }
            return;
        }

        // The facade batches by descriptor, so cross-kind selections are split into one
        // `delete_many(...)` call per `(kind, api_version)` group.
        for ((kind, api_version), entries) in Self::group_targets_by_descriptor(targets) {
            match self
                .resource_facade
                .delete_many(ResourceBatchSelector {
                    account: None,
                    kind: kind.clone(),
                    api_version: Some(api_version.clone()),
                    resource_refs: entries
                        .iter()
                        .map(|(_, target)| ResourceRef::ById(target.uid))
                        .collect(),
                })
                .await
            {
                Ok(response) => {
                    self.handle_delete_many_result(
                        &entries,
                        response,
                        summary,
                        errors,
                        self.output_config.as_ref(),
                    );
                }
                Err(error) => {
                    Self::record_delete_many_batch_error(
                        &entries,
                        &error,
                        summary,
                        errors,
                        self.output_config.as_ref(),
                    );
                }
            }

            if !errors.is_empty() {
                break;
            }
        }
    }

    fn group_targets_by_descriptor(
        targets: Vec<DeleteResourceTarget>,
    ) -> BTreeMap<(String, String), Vec<(usize, DeleteResourceTarget)>> {
        let mut groups = BTreeMap::new();

        for (index, target) in targets.into_iter().enumerate() {
            groups
                .entry((target.kind.clone(), target.api_version.clone()))
                .or_insert_with(Vec::new)
                .push((index, target));
        }

        groups
    }

    fn handle_delete_many_result(
        &self,
        entries: &[(usize, DeleteResourceTarget)],
        response: kamu_resources_facade::BatchResourceResponse<ResourceUID, ResourceLookupProblem>,
        summary: &mut DeleteResourcesSummary,
        errors: &mut Vec<(BoxedError, String)>,
        output_config: &OutputConfig,
    ) {
        // Batch responses preserve request indexes, so we can report results in terms
        // of the original resolved targets even though deletion happens per
        // descriptor group.
        for success in response.successes {
            let (_, target) = &entries[success.request_index];
            summary.record_deleted();
            DeleteResourcesPrinter::print_success(target, false, output_config);
        }

        for problem in response.problems {
            let (_, target) = &entries[problem.request_index];
            let error = problem.error;

            if self.ignore_not_found
                && matches!(
                    &error,
                    ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::UIDNotFound(_)
                )
            {
                summary.record_ignored();
                DeleteResourcesPrinter::print_ignored(&target.display_name(), false, output_config);
                continue;
            }

            summary.record_failed();
            let message = error.to_string();
            DeleteResourcesPrinter::print_failed(&target.display_name(), &message, false);

            errors.push((
                Box::<dyn std::error::Error + Send + Sync>::from(error),
                format!("Failed to delete resource {}", target.display_name()),
            ));
        }
    }

    fn record_delete_many_batch_error(
        entries: &[(usize, DeleteResourceTarget)],
        error: &BatchResourceError,
        summary: &mut DeleteResourcesSummary,
        errors: &mut Vec<(BoxedError, String)>,
        _output_config: &OutputConfig,
    ) {
        // A transport- or facade-level batch failure applies to every item in the group
        // because the backend did not return per-item outcomes.
        let error_message = error.to_string();

        for (_, target) in entries {
            summary.record_failed();
            DeleteResourcesPrinter::print_failed(&target.display_name(), &error_message, false);

            errors.push((
                Box::<dyn std::error::Error + Send + Sync>::from(InternalError::new(
                    error_message.clone(),
                )),
                format!("Failed to delete resource {}", target.display_name()),
            ));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for DeleteResourcesCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let prepared = self.prepare_targets().await?;
        self.run_prepared(prepared).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(super) struct PreparedDeleteTargets {
    targets: Vec<DeleteResourceTarget>,
    ignored_selectors: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
struct DeleteResourceTarget {
    kind: String,
    api_version: String,
    canonical_kind_name: String,
    uid: ResourceUID,
    name: String,
}

impl DeleteResourceTarget {
    fn from_resource_target(target: ResourceTarget) -> Self {
        Self {
            kind: target.kind,
            api_version: target.api_version,
            canonical_kind_name: target.canonical_kind_name,
            uid: target.uid,
            name: target.name,
        }
    }

    fn display_name(&self) -> String {
        format!("{}/{}", self.canonical_kind_name, self.name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct DeleteResourcesSummary {
    total_items: usize,
    deleted: usize,
    ignored: usize,
    failed: usize,
}

impl DeleteResourcesSummary {
    fn record_deleted(&mut self) {
        self.total_items += 1;
        self.deleted += 1;
    }

    fn record_ignored(&mut self) {
        self.total_items += 1;
        self.ignored += 1;
    }

    fn record_failed(&mut self) {
        self.total_items += 1;
        self.failed += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteResourcesPrinter;

impl DeleteResourcesPrinter {
    fn print_success(target: &DeleteResourceTarget, dry_run: bool, output_config: &OutputConfig) {
        if output_config.quiet {
            return;
        }

        let label = if dry_run {
            console::style("Would delete").cyan().bold()
        } else {
            console::style("Deleted").green().bold()
        };

        eprintln!("{label}: {}", target.display_name());
    }

    fn print_ignored(selector: &str, dry_run: bool, output_config: &OutputConfig) {
        if output_config.quiet {
            return;
        }

        let label = if dry_run {
            "Ignored (dry-run)"
        } else {
            "Ignored"
        };

        eprintln!("{}: {}", console::style(label).yellow().bold(), selector);
    }

    fn print_failed(selector: &str, message: &str, dry_run: bool) {
        let label = if dry_run {
            "Failed (dry-run)"
        } else {
            "Failed"
        };

        eprintln!("{}: {selector}", console::style(label).red().bold());
        eprintln!("  {} {}", console::style("error").red().bold(), message);
    }

    fn print_summary(summary: &DeleteResourcesSummary, dry_run: bool) {
        let deleted_label = if dry_run { "would delete" } else { "deleted" };

        eprintln!(
            "{} {} item(s): {} {}, {} ignored, {} failed",
            console::style("Summary").bold(),
            summary.total_items,
            summary.deleted,
            deleted_label,
            summary.ignored,
            summary.failed,
        );
    }

    fn batch_failure_summary(error_count: usize, dry_run: bool) -> String {
        format!(
            "Failed to {} {} resource(s)",
            if dry_run {
                "preview deletion of"
            } else {
                "delete"
            },
            error_count
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
