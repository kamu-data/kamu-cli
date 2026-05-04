// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::collect_all_pages;
use internal_error::BoxedError;
use kamu_resources::{ResourceKindDescriptor, ResourceSummaryView, ResourceUID, ResourceView};
use kamu_resources_facade::{
    DeleteResourceError,
    DeleteResourceRequest,
    GetResourceError,
    GetResourceRequest,
    ListAllResourcesRequest,
    ListResourcesRequest,
    ResourceFacade,
    ResourceRef,
};

use super::{BatchError, CLIError, Command};
use crate::Interact;
use crate::output::OutputConfig;
use crate::resource_context::{ResolvedResourceContext, ResourceContextReporter};
use crate::resources::ResourceSelectorResolutionService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DeleteResourcesCommand {
    resource_facade: Arc<dyn ResourceFacade>,
    resource_selector_resolution_service: Arc<dyn ResourceSelectorResolutionService>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    interact: Arc<Interact>,
    output_config: Arc<OutputConfig>,

    resolved_context: ResolvedResourceContext,
    scope: DeleteResourcesScope,
    selector: Option<String>,
    all: bool,
    force: bool,
    ignore_not_found: bool,
    dry_run: bool,
    continue_on_error: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeleteResourcesCommand {
    pub fn validate_scope_and_selector(
        scope: &DeleteResourcesScope,
        selector: Option<&str>,
        all: bool,
    ) -> Result<(), CLIError> {
        match (scope, selector) {
            (DeleteResourcesScope::All, Some(_)) => {
                return Err(CLIError::usage_error(
                    "Deleting all resources does not accept explicit selectors",
                ));
            }
            (DeleteResourcesScope::All, None) | (DeleteResourcesScope::ByKind(_), Some(_)) => {}
            (DeleteResourcesScope::ByKind(_), None) if all => {}
            (DeleteResourcesScope::ByKind(_), None) => {
                return Err(CLIError::usage_error(
                    "Specify a resource selector or pass --all",
                ));
            }
        }

        Ok(())
    }

    pub fn new(
        resource_facade: Arc<dyn ResourceFacade>,
        resource_selector_resolution_service: Arc<dyn ResourceSelectorResolutionService>,
        resource_context_reporter: Arc<ResourceContextReporter>,
        interact: Arc<Interact>,
        output_config: Arc<OutputConfig>,
        resolved_context: ResolvedResourceContext,
        scope: DeleteResourcesScope,
        selector: Option<String>,
        all: bool,
        force: bool,
        ignore_not_found: bool,
        dry_run: bool,
        continue_on_error: bool,
    ) -> Self {
        Self {
            resource_facade,
            resource_selector_resolution_service,
            resource_context_reporter,
            interact,
            output_config,
            resolved_context,
            scope,
            selector,
            all,
            force,
            ignore_not_found,
            dry_run,
            continue_on_error,
        }
    }

    async fn get_single_target(&self) -> Result<PreparedDeleteTargets, CLIError> {
        let DeleteResourcesScope::ByKind(kind_descriptor) = &self.scope else {
            unreachable!();
        };

        let selector = self.selector.as_ref().unwrap();
        let resolved_selector = self
            .resource_selector_resolution_service
            .resolve_single_selector(selector)
            .await?;

        let resource = self
            .resource_facade
            .get(GetResourceRequest {
                kind: kind_descriptor.kind.clone(),
                api_version: Some(kind_descriptor.api_version.clone()),
                account: None,
                resource_ref: resolved_selector.resource_ref,
            })
            .await;

        match resource {
            Ok(resource) => Ok(PreparedDeleteTargets {
                targets: vec![DeleteResourceTarget::from_resource_view(resource)],
                ignored_selectors: Vec::new(),
            }),
            Err(GetResourceError::NameNotFound(_) | GetResourceError::UIDNotFound(_))
                if self.ignore_not_found =>
            {
                Ok(PreparedDeleteTargets {
                    targets: Vec::new(),
                    ignored_selectors: vec![selector.clone()],
                })
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list_targets_by_kind(
        &self,
        kind_descriptor: &ResourceKindDescriptor,
    ) -> Result<Vec<DeleteResourceTarget>, CLIError> {
        let resources = collect_all_pages(RESOURCE_PAGE_SIZE, |pagination| async move {
            self.resource_facade
                .list(ListResourcesRequest {
                    kind: kind_descriptor.kind.clone(),
                    account: None,
                    pagination,
                })
                .await
                .map_err(CLIError::from)
        })
        .await?;

        Ok(resources
            .into_iter()
            .map(DeleteResourceTarget::from_resource_summary_view)
            .collect())
    }

    async fn list_all_targets(&self) -> Result<Vec<DeleteResourceTarget>, CLIError> {
        let resources = collect_all_pages(RESOURCE_PAGE_SIZE, |pagination| async move {
            self.resource_facade
                .list_all(ListAllResourcesRequest {
                    account: None,
                    pagination,
                })
                .await
                .map_err(CLIError::from)
        })
        .await?;

        Ok(resources
            .into_iter()
            .map(DeleteResourceTarget::from_resource_summary_view)
            .collect())
    }

    async fn prepare_targets(&self) -> Result<PreparedDeleteTargets, CLIError> {
        match &self.scope {
            DeleteResourcesScope::ByKind(kind_descriptor) => {
                if self.selector.is_some() {
                    self.get_single_target().await
                } else {
                    Ok(PreparedDeleteTargets {
                        targets: self.list_targets_by_kind(kind_descriptor).await?,
                        ignored_selectors: Vec::new(),
                    })
                }
            }
            DeleteResourcesScope::All => Ok(PreparedDeleteTargets {
                targets: self.list_all_targets().await?,
                ignored_selectors: Vec::new(),
            }),
        }
    }

    fn no_targets_message(&self) -> &'static str {
        match &self.scope {
            DeleteResourcesScope::ByKind(_) => "There are no matching resources to delete",
            DeleteResourcesScope::All => "There are no resources to delete",
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
        for target in targets {
            if self.dry_run {
                summary.record_deleted();
                DeleteResourcesPrinter::print_success(&target, true, self.output_config.as_ref());
                continue;
            }

            match self
                .resource_facade
                .delete(DeleteResourceRequest {
                    kind: target.kind.clone(),
                    account: None,
                    resource_ref: ResourceRef::ById(target.uid),
                })
                .await
            {
                Ok(_) => {
                    summary.record_deleted();
                    DeleteResourcesPrinter::print_success(
                        &target,
                        false,
                        self.output_config.as_ref(),
                    );
                }
                Err(DeleteResourceError::NameNotFound(_) | DeleteResourceError::UIDNotFound(_))
                    if self.ignore_not_found =>
                {
                    summary.record_ignored();
                    DeleteResourcesPrinter::print_ignored(
                        &target.display_name(),
                        false,
                        self.output_config.as_ref(),
                    );
                }
                Err(error) => {
                    summary.record_failed();
                    let message = error.to_string();
                    DeleteResourcesPrinter::print_failed(&target.display_name(), &message, false);

                    errors.push((
                        Box::<dyn std::error::Error + Send + Sync>::from(error),
                        format!("Failed to delete resource {}", target.display_name()),
                    ));

                    if !self.continue_on_error {
                        break;
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for DeleteResourcesCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        Self::validate_scope_and_selector(&self.scope, self.selector.as_deref(), self.all)
    }

    async fn run(&self) -> Result<(), CLIError> {
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
        } = self.prepare_targets().await?;

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
                eprintln!("{}", console::style(self.no_targets_message()).yellow());
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum DeleteResourcesScope {
    ByKind(ResourceKindDescriptor),
    All,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct PreparedDeleteTargets {
    targets: Vec<DeleteResourceTarget>,
    ignored_selectors: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
struct DeleteResourceTarget {
    kind: String,
    uid: ResourceUID,
    name: String,
}

impl DeleteResourceTarget {
    fn from_resource_summary_view(resource: ResourceSummaryView) -> Self {
        Self {
            kind: resource.kind,
            uid: resource.uid,
            name: resource.name,
        }
    }

    fn from_resource_view(resource: ResourceView) -> Self {
        Self {
            kind: resource.kind,
            uid: resource.metadata.uid,
            name: resource.metadata.name,
        }
    }

    fn display_name(&self) -> String {
        format!("{}/{}", self.kind, self.name)
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
