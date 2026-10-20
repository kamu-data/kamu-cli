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

use internal_error::{BoxedError, InternalError, ResultIntoInternal};
use kamu_resources::{
    ResourceID,
    ResourceLabelFilterInput,
    ResourcePhase,
    ResourceRef,
    ResourceSelectorName,
    ResourceVisibility,
    TypeUri,
};
use kamu_resources_facade::{
    BatchResourceError,
    ResourceDeletion,
    ResourceFacade,
    ResourceLookupProblem,
};

use super::{BatchError, CLIError, Command};
use crate::output::OutputConfig;
use crate::resource_context::{ResolvedResourceContext, ResourceContextReporter};
use crate::resources::{
    ResourceSelectionResolutionOptions,
    ResourceSelectionResolutionService,
    ResourceSelectionSyntax,
    ResourceTarget,
};
use crate::{Interact, PostCommandAction, PostCommandActions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteResourcesCommand {
    resource_facade: Arc<dyn ResourceFacade>,
    resource_selection_resolution_service: Arc<dyn ResourceSelectionResolutionService>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    interact: Arc<Interact>,
    output_config: Arc<OutputConfig>,
    post_command_actions: Arc<PostCommandActions>,

    resolved_context: ResolvedResourceContext,
    syntax: ResourceSelectionSyntax,
    label_filter: Option<ResourceLabelFilterInput>,
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
        post_command_actions: Arc<PostCommandActions>,
        resolved_context: ResolvedResourceContext,
        syntax: ResourceSelectionSyntax,
        label_filter: Option<ResourceLabelFilterInput>,
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
            post_command_actions,
            resolved_context,
            syntax,
            label_filter,
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
                &ResourceSelectionResolutionOptions {
                    ignore_not_found: self.ignore_not_found,
                    max_expanded_results: None,
                    label_filter: self.label_filter.clone(),
                    // A resource being deleted is selected by its id only
                    visibility: ResourceVisibility::Active,
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

        let deletions = self
            .execute_delete_phase(targets, &mut summary, &mut errors)
            .await;

        if errors.is_empty() && !deletions.is_empty() && self.phases_known_after_command() {
            self.post_command_actions.add(ReportDeletionPhases {
                deletions,
                summary,
                output_config: self.output_config.clone(),
            });
            return Ok(());
        }

        let rolled_back = !errors.is_empty() && self.errors_roll_back_deletions();
        for (target, phase) in &deletions {
            if rolled_back {
                summary.record_rolled_back();
                DeleteResourcesPrinter::print_rolled_back(target, self.output_config.as_ref());
            } else {
                summary.record_success(*phase);
                DeleteResourcesPrinter::print_success(target, *phase, self.output_config.as_ref());
            }
        }
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

    /// A local workspace processes the messages of a command once its
    /// transaction has committed, so controllers clean up only then. A server
    /// reports the phase at the time of its response.
    fn phases_known_after_command(&self) -> bool {
        match self.resolved_context {
            ResolvedResourceContext::LocalWorkspace => true,
            ResolvedResourceContext::RemoteWorkspace { .. } => false,
        }
    }

    /// A local workspace runs the whole command in one transaction, which an
    /// error rolls back together with the deletions requested before it. A
    /// server commits each request on its own.
    fn errors_roll_back_deletions(&self) -> bool {
        match self.resolved_context {
            ResolvedResourceContext::LocalWorkspace => true,
            ResolvedResourceContext::RemoteWorkspace { .. } => false,
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

    /// Returns the deleted targets with the phase each one reached
    async fn execute_delete_phase(
        &self,
        targets: Vec<DeleteResourceTarget>,
        summary: &mut DeleteResourcesSummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) -> Vec<(DeleteResourceTarget, ResourcePhase)> {
        let mut deletions = Vec::new();

        if self.dry_run {
            for target in targets {
                summary.record_deleted();
                DeleteResourcesPrinter::print_dry_run_success(&target, self.output_config.as_ref());
            }
            return deletions;
        }

        // The facade batches by descriptor, so cross-type selections are split into one
        // `delete(...)` call per resolved schema.
        for (_schema, entries) in Self::group_targets_by_schema(targets) {
            match self
                .resource_facade
                .delete(Self::entry_resource_refs(&entries))
                .await
            {
                Ok(response) => {
                    self.handle_delete_result(
                        &entries,
                        response,
                        &mut deletions,
                        summary,
                        errors,
                        self.output_config.as_ref(),
                    );
                }
                Err(error) => {
                    Self::record_delete_batch_error(
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

        deletions
    }

    /// Builds one ref per already-resolved target, naming the type by its
    /// resolved schema rather than by the selector the user typed.
    fn entry_resource_refs(entries: &[(usize, DeleteResourceTarget)]) -> Vec<ResourceRef> {
        entries
            .iter()
            .map(|(_, target)| target.resource_ref())
            .collect()
    }

    fn group_targets_by_schema(
        targets: Vec<DeleteResourceTarget>,
    ) -> BTreeMap<TypeUri, Vec<(usize, DeleteResourceTarget)>> {
        let mut groups = BTreeMap::new();

        for (index, target) in targets.into_iter().enumerate() {
            groups
                .entry(target.schema.clone())
                .or_insert_with(Vec::new)
                .push((index, target));
        }

        groups
    }

    fn handle_delete_result(
        &self,
        entries: &[(usize, DeleteResourceTarget)],
        response: kamu_resources_facade::BatchResourceResponse<
            ResourceDeletion,
            ResourceLookupProblem,
        >,
        deletions: &mut Vec<(DeleteResourceTarget, ResourcePhase)>,
        summary: &mut DeleteResourcesSummary,
        errors: &mut Vec<(BoxedError, String)>,
        output_config: &OutputConfig,
    ) {
        // Batch responses preserve request indexes, so we can report results in terms
        // of the original resolved targets even though deletion happens per
        // descriptor group.
        for success in response.successes {
            let (_, target) = &entries[success.request_index];
            deletions.push((target.clone(), success.item.phase));
        }

        for problem in response.problems {
            let (_, target) = &entries[problem.request_index];
            let error = problem.error;

            if self.ignore_not_found
                && matches!(
                    &error,
                    ResourceLookupProblem::NameNotFound(_) | ResourceLookupProblem::IDNotFound(_)
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

    fn record_delete_batch_error(
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

/// Reports the phase each resource reached once its controllers had the chance
/// to clean up: still found by its id means `Deleting`, gone means `Deleted`
struct ReportDeletionPhases {
    deletions: Vec<(DeleteResourceTarget, ResourcePhase)>,
    summary: DeleteResourcesSummary,
    output_config: Arc<OutputConfig>,
}

impl ReportDeletionPhases {
    /// A resource that cannot be read again keeps the phase its deletion
    /// request returned: the deletion has already committed.
    async fn reached_phases(&self, resource_facade: &dyn ResourceFacade) -> Vec<ResourcePhase> {
        let requested_phases = self.deletions.iter().map(|(_, phase)| *phase);
        let resource_refs = self
            .deletions
            .iter()
            .map(|(target, _)| target.resource_ref())
            .collect();

        let response = match resource_facade
            .get_handles(resource_refs, ResourceVisibility::Active)
            .await
        {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(error = ?err, "Failed to read deleted resources again");
                return requested_phases.collect();
            }
        };

        let mut reached_phases = requested_phases.collect::<Vec<_>>();
        for success in response.successes {
            reached_phases[success.request_index] = ResourcePhase::Deleting;
        }
        for problem in response.problems {
            if let Some(phase) = phase_when_not_found(&problem.error) {
                reached_phases[problem.request_index] = phase;
            }
        }
        reached_phases
    }
}

#[async_trait::async_trait]
impl PostCommandAction for ReportDeletionPhases {
    async fn run(self: Box<Self>, catalog: dill::Catalog) -> Result<(), CLIError> {
        let resource_facade = catalog.get_one::<dyn ResourceFacade>().int_err()?;
        let reached_phases = self.reached_phases(resource_facade.as_ref()).await;

        let mut summary = self.summary;
        for ((target, _), phase) in self.deletions.iter().zip(reached_phases) {
            summary.record_success(phase);
            DeleteResourcesPrinter::print_success(target, phase, self.output_config.as_ref());
            // Controllers had their chance to clean up, so a resource still
            // being deleted is waiting for a retry
            if phase == ResourcePhase::Deleting {
                DeleteResourcesPrinter::print_retry_hint(target, self.output_config.as_ref());
            }
        }
        DeleteResourcesPrinter::print_summary(&summary, false);

        Ok(())
    }
}

/// Only a resource its id no longer finds has completed its deletion
fn phase_when_not_found(problem: &ResourceLookupProblem) -> Option<ResourcePhase> {
    match problem {
        ResourceLookupProblem::IDNotFound(_) => Some(ResourcePhase::Deleted),
        ResourceLookupProblem::NameNotFound(_)
        | ResourceLookupProblem::SchemaMismatch(_)
        | ResourceLookupProblem::NameMismatch(_)
        | ResourceLookupProblem::EmptyRef
        | ResourceLookupProblem::UntypedName => None,
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
    canonical_selector: ResourceSelectorName,
    schema: TypeUri,
    id: ResourceID,
    name: String,
}

impl DeleteResourceTarget {
    fn from_resource_target(target: ResourceTarget) -> Self {
        Self {
            canonical_selector: target.canonical_selector,
            schema: target.schema,
            id: target.id,
            name: target.name.to_string(),
        }
    }

    fn display_name(&self) -> String {
        format!("{}/{}", self.canonical_selector, self.name)
    }

    fn resource_ref(&self) -> ResourceRef {
        ResourceRef {
            account: None,
            r#type: Some(self.schema.clone().into()),
            id: Some(self.id),
            did: None,
            name: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct DeleteResourcesSummary {
    total_items: usize,
    deleted: usize,
    deleting: usize,
    ignored: usize,
    failed: usize,
    rolled_back: usize,
}

impl DeleteResourcesSummary {
    fn record_deleted(&mut self) {
        self.total_items += 1;
        self.deleted += 1;
    }

    /// Controllers may still be cleaning up after the deletion was requested.
    fn record_success(&mut self, phase: ResourcePhase) {
        match phase {
            ResourcePhase::Deleting => {
                self.total_items += 1;
                self.deleting += 1;
            }
            ResourcePhase::Pending
            | ResourcePhase::Reconciling
            | ResourcePhase::Ready
            | ResourcePhase::Degraded
            | ResourcePhase::Failed
            | ResourcePhase::Deleted => self.record_deleted(),
        }
    }

    fn record_ignored(&mut self) {
        self.total_items += 1;
        self.ignored += 1;
    }

    fn record_failed(&mut self) {
        self.total_items += 1;
        self.failed += 1;
    }

    fn record_rolled_back(&mut self) {
        self.total_items += 1;
        self.rolled_back += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteResourcesPrinter;

impl DeleteResourcesPrinter {
    fn print_success(
        target: &DeleteResourceTarget,
        phase: ResourcePhase,
        output_config: &OutputConfig,
    ) {
        if output_config.quiet {
            return;
        }

        let label = match phase {
            ResourcePhase::Deleting => console::style("Deleting").yellow().bold(),
            ResourcePhase::Pending
            | ResourcePhase::Reconciling
            | ResourcePhase::Ready
            | ResourcePhase::Degraded
            | ResourcePhase::Failed
            | ResourcePhase::Deleted => console::style("Deleted").green().bold(),
        };

        eprintln!("{label}: {}", target.display_name());
    }

    fn print_retry_hint(target: &DeleteResourceTarget, output_config: &OutputConfig) {
        if output_config.quiet {
            return;
        }

        eprintln!(
            "  {} its cleanup has not finished; `kamu delete {}` retries it",
            console::style("hint").yellow().bold(),
            target.id
        );
    }

    fn print_rolled_back(target: &DeleteResourceTarget, output_config: &OutputConfig) {
        if output_config.quiet {
            return;
        }

        eprintln!(
            "{}: {}",
            console::style("Not deleted (rolled back)").yellow().bold(),
            target.display_name()
        );
    }

    fn print_dry_run_success(target: &DeleteResourceTarget, output_config: &OutputConfig) {
        if output_config.quiet {
            return;
        }

        eprintln!(
            "{}: {}",
            console::style("Deleted (dry-run)").cyan().bold(),
            target.display_name()
        );
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
        let deleted_label = if dry_run {
            "deleted (dry-run)"
        } else {
            "deleted"
        };

        let deleting = if summary.deleting > 0 {
            format!(", {} deleting", summary.deleting)
        } else {
            String::new()
        };
        let rolled_back = if summary.rolled_back > 0 {
            format!(", {} rolled back", summary.rolled_back)
        } else {
            String::new()
        };

        eprintln!(
            "{} {} item(s): {} {}{deleting}, {} ignored, {} failed{rolled_back}",
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
