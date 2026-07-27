// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Read as _;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common_macros::{transactional_method1, transactional_method2};
use internal_error::{BoxedError, ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    ApplyManifestChange,
    ApplyManifestRejection,
    ApplyResourceOutcome,
    Resource,
    ResourceWarning,
    resource_type_name,
};
use kamu_resources_facade::{
    ApplyManifestBatchItemResult,
    ApplyManifestBatchRequest,
    ApplyManifestBatchResponse,
    ApplyManifestRequest,
    ResourceManifestFormat as FacadeResourceManifestFormat,
};
use thiserror::Error;

use super::{BatchError, CLIError, Command, common};
use crate::cli::ResourceManifestFormat;
use crate::output::{ApplyMultiProgress, OutputConfig};
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{
    DiscoverResourceManifestsResult,
    DiscoveredResourceManifest,
    DiscoveredResourceManifestSource,
    ExecuteResourceManifestError,
    ExecuteResourceManifestOutcome,
    ExecutedResourceManifestResult,
    ResourceFacadeFactory,
    ResourceManifestDiscoveryService,
    ResourceManifestExecutionService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ApplyCommand {
    // The facade factory and execution service are resolved per-manifest from a
    // transactional catalog instead (this command runs without an outer
    // transaction), so they are not injected as fields here.
    catalog: dill::Catalog,
    resource_manifest_discovery_service: Arc<dyn ResourceManifestDiscoveryService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    files: Vec<PathBuf>,

    #[dill::component(explicit)]
    recursive: bool,

    #[dill::component(explicit)]
    dry_run: bool,

    #[dill::component(explicit)]
    stdin: bool,

    #[dill::component(explicit)]
    continue_on_error: bool,

    #[dill::component(explicit)]
    format: Option<ResourceManifestFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ApplyCommand {
    fn stdin_manifest_format(&self) -> FacadeResourceManifestFormat {
        match self.format.unwrap_or(ResourceManifestFormat::Yaml) {
            ResourceManifestFormat::Json => FacadeResourceManifestFormat::Json,
            ResourceManifestFormat::Yaml => FacadeResourceManifestFormat::Yaml,
        }
    }

    fn read_stdin_manifest(&self) -> Result<Vec<DiscoveredResourceManifest>, CLIError> {
        let mut content = String::new();
        std::io::stdin().read_to_string(&mut content).int_err()?;

        Ok(vec![DiscoveredResourceManifest {
            source: DiscoveredResourceManifestSource::Stdin(content),
            format: self.stdin_manifest_format(),
        }])
    }

    fn discover_manifests_phase(
        &self,
        printer: &ApplyPrinter,
        summary: &mut ApplySummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) -> Result<Vec<DiscoveredResourceManifest>, CLIError> {
        if self.stdin {
            return self.read_stdin_manifest();
        }

        let DiscoverResourceManifestsResult {
            manifests,
            errors: discovery_errors,
        } = self.resource_manifest_discovery_service.discover(
            self.files.clone(),
            self.recursive,
            self.format,
        );

        for (input, err) in discovery_errors {
            let input = input.display().to_string();
            summary.record_failed();
            printer.print_discovery_error(&input, err.to_string());

            errors.push((
                Box::<dyn std::error::Error + Send + Sync>::from(err),
                format!("Failed to process input {input}"),
            ));

            if !self.continue_on_error {
                return Err(BatchError {
                    summary: printer.batch_failure_summary(errors.len()),
                    errors_with_context: std::mem::take(errors),
                }
                .into());
            }
        }

        Ok(manifests)
    }

    /// Applies a single manifest in its own transaction, so previously-applied
    /// items survive a later rejection or failure. Committed on `Accepted`;
    /// rolled back on rejection/error, carrying the outcome out for reporting.
    #[transactional_method2(
        resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
        execution_service: Arc<dyn ResourceManifestExecutionService>
    )]
    async fn apply_single_manifest(
        &self,
        manifest: &DiscoveredResourceManifest,
    ) -> Result<ExecuteResourceManifestOutcome, ExecuteSingleManifestError> {
        let resource_facade = resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())
            .map_err(|e| ExecuteSingleManifestError::Internal(e.int_err()))?;

        let outcome = execution_service
            .execute(resource_facade.as_ref(), manifest, self.dry_run)
            .await
            .map_err(ExecuteSingleManifestError::Execute)?;

        // Roll back rejections (nothing to commit), carrying the outcome out.
        match outcome {
            outcome @ ExecuteResourceManifestOutcome::Accepted(_) => Ok(outcome),
            rejected @ ExecuteResourceManifestOutcome::Rejected(_) => {
                Err(ExecuteSingleManifestError::Rejected(rejected))
            }
        }
    }

    async fn apply_manifests_phase(
        &self,
        printer: &ApplyPrinter<'_>,
        manifests: Vec<DiscoveredResourceManifest>,
        maybe_progress: Option<&ApplyMultiProgress>,
        summary: &mut ApplySummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) -> Result<(), CLIError> {
        if self.continue_on_error {
            self.apply_manifests_per_item(printer, manifests, maybe_progress, summary, errors)
                .await
        } else {
            self.apply_manifests_as_one_batch(printer, manifests, maybe_progress, summary, errors)
                .await
        }
    }

    /// Existing per-item transactional loop: each manifest is independently
    /// transacted (see `apply_single_manifest`), so earlier successes survive
    /// a later rejection/failure and the loop keeps going past failures.
    async fn apply_manifests_per_item(
        &self,
        printer: &ApplyPrinter<'_>,
        manifests: Vec<DiscoveredResourceManifest>,
        maybe_progress: Option<&ApplyMultiProgress>,
        summary: &mut ApplySummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) -> Result<(), CLIError> {
        for manifest in manifests {
            let source = manifest.source.to_string();
            let item_progress = printer.start_item(maybe_progress, &source);

            // Flatten the rollback control-flow back into the outcome/error
            // shape the reporting below expects.
            let outcome: Result<ExecuteResourceManifestOutcome, BoxedError> =
                match self.apply_single_manifest(&manifest).await {
                    Ok(outcome) | Err(ExecuteSingleManifestError::Rejected(outcome)) => Ok(outcome),
                    Err(ExecuteSingleManifestError::Execute(err)) => Err(Box::new(err)),
                    Err(ExecuteSingleManifestError::Internal(err)) => Err(Box::new(err)),
                };

            match outcome {
                Ok(ExecuteResourceManifestOutcome::Accepted(result)) => {
                    summary.record_accepted(&result);
                    printer.print_accepted(item_progress, &source, &result, self.dry_run)?;
                }
                Ok(ExecuteResourceManifestOutcome::Rejected(rejection)) => {
                    summary.record_rejected();
                    printer.print_rejected(item_progress, &source, rejection.message.clone());

                    errors.push((
                        Box::<dyn std::error::Error + Send + Sync>::from(ApplyRejectedError::from(
                            rejection,
                        )),
                        format!("Manifest {source} was rejected"),
                    ));

                    if !self.continue_on_error {
                        break;
                    }
                }
                Err(err) => {
                    summary.record_failed();
                    printer.print_failed(item_progress, &source, err.to_string());

                    errors.push((
                        err,
                        format!(
                            "Failed to {} manifest {}",
                            if self.dry_run { "plan" } else { "apply" },
                            source
                        ),
                    ));

                    if !self.continue_on_error {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Opens exactly one transaction for the whole batch: the first
    /// rejection/failure rolls back everything, including earlier items in
    /// the same invocation that would otherwise have succeeded.
    #[transactional_method1(resource_facade_factory: Arc<dyn ResourceFacadeFactory>)]
    async fn apply_manifest_batch(
        &self,
        manifests: &[DiscoveredResourceManifest],
    ) -> Result<ApplyManifestBatchResponse<ExecuteResourceManifestOutcome>, ExecuteBatchError> {
        let resource_facade = resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())
            .map_err(|e| ExecuteBatchError::Internal(e.int_err()))?;

        let mut items = Vec::with_capacity(manifests.len());
        for manifest in manifests {
            let content = manifest
                .source
                .read_content()
                .map_err(ExecuteBatchError::ReadManifest)?;
            items.push(ApplyManifestRequest {
                format: manifest.format,
                manifest: content,
            });
        }

        let request = ApplyManifestBatchRequest { items };

        let response = if self.dry_run {
            let response = resource_facade
                .plan_apply_manifests(request)
                .await
                .map_err(ExecuteBatchError::Batch)?;
            map_batch_response(response)
        } else {
            let response = resource_facade
                .apply_manifests(request)
                .await
                .map_err(ExecuteBatchError::Batch)?;
            map_batch_response(response)
        };

        let fully_succeeded = response.items.len() == manifests.len()
            && response.items.iter().all(|item| {
                matches!(
                    item.outcome,
                    Ok(ExecuteResourceManifestOutcome::Accepted(_))
                )
            });

        if fully_succeeded {
            Ok(response)
        } else {
            Err(ExecuteBatchError::Rejected(response))
        }
    }

    /// New atomic path: a single call to the batch facade method wraps the
    /// *entire* batch in one transaction.
    async fn apply_manifests_as_one_batch(
        &self,
        printer: &ApplyPrinter<'_>,
        manifests: Vec<DiscoveredResourceManifest>,
        maybe_progress: Option<&ApplyMultiProgress>,
        summary: &mut ApplySummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) -> Result<(), CLIError> {
        let (response, committed) = match self.apply_manifest_batch(&manifests).await {
            Ok(response) => (response, true),
            Err(ExecuteBatchError::Rejected(response)) => (response, false),
            Err(ExecuteBatchError::ReadManifest(err)) => {
                summary.record_failed();
                errors.push((Box::new(err), "Failed to read manifest".to_string()));
                return Ok(());
            }
            Err(ExecuteBatchError::Batch(err)) => {
                summary.record_failed();
                errors.push((Box::new(err), "Batch apply failed".to_string()));
                return Ok(());
            }
            Err(ExecuteBatchError::Internal(err)) => {
                summary.record_failed();
                errors.push((Box::new(err), "Batch apply failed".to_string()));
                return Ok(());
            }
        };

        let mut results_by_index = response
            .items
            .into_iter()
            .map(|item| (item.request_index, item.outcome))
            .collect::<std::collections::HashMap<_, _>>();
        let rolled_back_successes = response
            .rolled_back_successes
            .into_iter()
            .collect::<std::collections::HashSet<_>>();

        for (request_index, manifest) in manifests.iter().enumerate() {
            let source = manifest.source.to_string();
            let item_progress = printer.start_item(maybe_progress, &source);

            match results_by_index.remove(&request_index) {
                None => {
                    if rolled_back_successes.contains(&request_index) {
                        summary.record_rolled_back();
                        printer.print_rolled_back(item_progress, &source, None, self.dry_run)?;
                    } else {
                        // Never attempted: the batch stopped before reaching this item.
                        summary.record_not_attempted();
                        printer.print_not_attempted(item_progress, &source);
                    }
                }
                Some(Ok(ExecuteResourceManifestOutcome::Accepted(result))) => {
                    if committed {
                        summary.record_accepted(&result);
                        printer.print_accepted(item_progress, &source, &result, self.dry_run)?;
                    } else {
                        summary.record_rolled_back();
                        printer.print_rolled_back(
                            item_progress,
                            &source,
                            Some(&result.resource),
                            self.dry_run,
                        )?;
                    }
                }
                Some(Ok(ExecuteResourceManifestOutcome::Rejected(rejection))) => {
                    summary.record_rejected();
                    printer.print_rejected(item_progress, &source, rejection.message.clone());

                    errors.push((
                        Box::<dyn std::error::Error + Send + Sync>::from(ApplyRejectedError::from(
                            rejection,
                        )),
                        format!("Manifest {source} was rejected"),
                    ));
                }
                Some(Err(err)) => {
                    summary.record_failed();
                    printer.print_failed(item_progress, &source, err.to_string());

                    errors.push((
                        Box::new(err),
                        format!(
                            "Failed to {} manifest {}",
                            if self.dry_run { "plan" } else { "apply" },
                            source
                        ),
                    ));
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ApplyCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.stdin && !self.files.is_empty() {
            return Err(CLIError::usage_error(
                "Cannot specify --stdin and positional arguments at the same time",
            ));
        }

        if !self.stdin && self.files.is_empty() {
            return Err(CLIError::usage_error("No manifest paths were provided"));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;

        if !self.output_config.quiet {
            self.resource_context_reporter.report_usage(
                if self.dry_run {
                    "Previewing resource manifests in context"
                } else {
                    "Applying resource manifests to context"
                },
                &resolved_context,
            );
        }

        let printer = ApplyPrinter::new(&self.output_config, self.dry_run);

        let mut summary = ApplySummary::default();
        let mut errors: Vec<(BoxedError, String)> = Vec::new();

        let manifests = match self.discover_manifests_phase(&printer, &mut summary, &mut errors) {
            Ok(manifests) => manifests,
            Err(err) => {
                printer.print_summary(None, &summary);
                return Err(err);
            }
        };

        let maybe_progress = printer.create_progress(manifests.len());

        let draw_thread = maybe_progress.as_ref().map(|progress| {
            let progress = progress.clone();
            std::thread::spawn(move || {
                progress.draw();
            })
        });

        self.apply_manifests_phase(
            &printer,
            manifests,
            maybe_progress.as_ref(),
            &mut summary,
            &mut errors,
        )
        .await?;

        printer.print_summary(maybe_progress.as_ref(), &summary);
        ApplyPrinter::finish_progress(maybe_progress.as_ref(), draw_thread);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(BatchError {
                summary: printer.batch_failure_summary(errors.len()),
                errors_with_context: errors,
            }
            .into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct ApplySummary {
    total_items: usize,
    created: usize,
    updated: usize,
    unchanged: usize,
    rejected: usize,
    failed: usize,
    warnings: usize,
    rolled_back: usize,
    not_attempted: usize,
}

impl ApplySummary {
    fn record_accepted(&mut self, result: &ExecutedResourceManifestResult) {
        self.total_items += 1;
        self.warnings += result.warnings.len();

        match result.outcome {
            ApplyResourceOutcome::Created => self.created += 1,
            ApplyResourceOutcome::Updated => self.updated += 1,
            ApplyResourceOutcome::Untouched => self.unchanged += 1,
        }
    }

    fn record_rejected(&mut self) {
        self.total_items += 1;
        self.rejected += 1;
    }

    fn record_failed(&mut self) {
        self.total_items += 1;
        self.failed += 1;
    }

    /// An item that individually succeeded but was thrown away because the
    /// enclosing single-transaction batch rolled back — never counted into
    /// `created`/`updated`, since nothing was actually persisted.
    fn record_rolled_back(&mut self) {
        self.total_items += 1;
        self.rolled_back += 1;
    }

    /// An item the batch never reached because it stopped at an earlier
    /// rejection/failure.
    fn record_not_attempted(&mut self) {
        self.total_items += 1;
        self.not_attempted += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Resource apply rejected ({category}): {message}")]
struct ApplyRejectedError {
    category: &'static str,
    message: String,
}

impl From<ApplyManifestRejection> for ApplyRejectedError {
    fn from(value: ApplyManifestRejection) -> Self {
        Self {
            category: value.category.label(),
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `Rejected` carries the outcome out of a rolled-back transaction for
/// reporting; `Execute`/`Internal` are genuine failures.
enum ExecuteSingleManifestError {
    Rejected(ExecuteResourceManifestOutcome),
    Execute(ExecuteResourceManifestError),
    Internal(InternalError),
}

// Required by `transactional_method2`.
impl From<InternalError> for ExecuteSingleManifestError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `Rejected` carries the partial batch response out of a rolled-back
/// transaction for reporting; `ReadManifest`/`Batch`/`Internal` are genuine
/// failures that abort the whole call before any item-level response exists.
enum ExecuteBatchError {
    Rejected(ApplyManifestBatchResponse<ExecuteResourceManifestOutcome>),
    ReadManifest(std::io::Error),
    Batch(kamu_resources_facade::BatchResourceError),
    Internal(InternalError),
}

// Required by `transactional_method1`.
impl From<InternalError> for ExecuteBatchError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}

fn map_batch_response<D>(
    response: ApplyManifestBatchResponse<D>,
) -> ApplyManifestBatchResponse<ExecuteResourceManifestOutcome>
where
    ExecuteResourceManifestOutcome: From<D>,
{
    ApplyManifestBatchResponse {
        items: response
            .items
            .into_iter()
            .map(|item| ApplyManifestBatchItemResult {
                request_index: item.request_index,
                outcome: item.outcome.map(ExecuteResourceManifestOutcome::from),
            })
            .collect(),
        rolled_back_successes: response.rolled_back_successes,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ApplyPrinter<'a> {
    output_config: &'a OutputConfig,
    dry_run: bool,
}

struct ApplyItemProgress<'a> {
    progress: Option<&'a ApplyMultiProgress>,
    item: Option<crate::output::ApplyProgress>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a> ApplyPrinter<'a> {
    // Command-facing API

    fn new(output_config: &'a OutputConfig, dry_run: bool) -> Self {
        Self {
            output_config,
            dry_run,
        }
    }

    fn create_progress(&self, total_items: usize) -> Option<ApplyMultiProgress> {
        if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            Some(ApplyMultiProgress::new(total_items))
        } else {
            None
        }
    }

    fn start_item<'b>(
        &self,
        maybe_progress: Option<&'b ApplyMultiProgress>,
        source: &str,
    ) -> ApplyItemProgress<'b> {
        self.begin_item(maybe_progress, source)
    }

    fn print_summary(&self, progress: Option<&ApplyMultiProgress>, summary: &ApplySummary) {
        let mut line = format!(
            "{} {} item(s): {} created, {} updated, {} unchanged, {} rejected, {} failed, {} \
             warning(s)",
            console::style("Summary").bold(),
            summary.total_items,
            summary.created,
            summary.updated,
            summary.unchanged,
            summary.rejected,
            summary.failed,
            summary.warnings
        );

        if summary.rolled_back > 0 || summary.not_attempted > 0 {
            use std::fmt::Write as _;
            let _ = write!(
                line,
                ", {} rolled back, {} not attempted",
                summary.rolled_back, summary.not_attempted
            );
        }

        self.print_line(progress, line);
    }

    fn batch_failure_summary(&self, error_count: usize) -> String {
        format!(
            "Failed to {} {} item(s)",
            if self.dry_run { "plan" } else { "apply" },
            error_count
        )
    }

    fn print_discovery_error(&self, source: &str, message: String) {
        self.print_line(None, self.error_status_line(source, "Failed"));
        self.print_error_detail(None, source, message);
    }

    fn print_accepted(
        &self,
        item_progress: ApplyItemProgress<'_>,
        source: &str,
        result: &ExecutedResourceManifestResult,
        dry_run: bool,
    ) -> Result<(), CLIError> {
        let progress = self.finish_item(
            item_progress,
            self.accepted_status_line(source, result)?,
            self.should_print_success_line(),
        );
        self.increment_completed(progress);

        if !result.warnings.is_empty() {
            self.print_warning_lines(progress, source, &result.warnings);
        }

        if dry_run && !result.changes.is_empty() {
            self.print_change_lines(progress, source, &result.changes);
        }

        if self.should_print_verbose_resource() {
            self.print_verbose_resource(progress, &result.resource)?;
        }

        Ok(())
    }

    fn print_rejected(&self, item_progress: ApplyItemProgress<'_>, source: &str, message: String) {
        let progress = self.finish_item(
            item_progress,
            self.error_status_line(source, "Rejected"),
            true,
        );
        self.increment_completed(progress);
        self.print_error_detail(progress, source, message);
    }

    fn print_failed(&self, item_progress: ApplyItemProgress<'_>, source: &str, message: String) {
        let progress = self.finish_item(
            item_progress,
            self.error_status_line(source, "Failed"),
            true,
        );
        self.increment_completed(progress);
        self.print_error_detail(progress, source, message);
    }

    /// Prints an item that individually succeeded but was not persisted
    /// because the enclosing single-transaction batch rolled back due to a
    /// later item's rejection/failure. `resource` is only available when the
    /// facade could report the item's real post-apply state (the local
    /// path); the remote GraphQL path rolls back server-side and cannot hand
    /// back a resource that was never committed, so it prints without one.
    fn print_rolled_back(
        &self,
        item_progress: ApplyItemProgress<'_>,
        source: &str,
        resource: Option<&Resource>,
        dry_run: bool,
    ) -> Result<(), CLIError> {
        let label = if dry_run {
            "Not committed (dry-run, batch rolled back)"
        } else {
            "Not committed (batch rolled back)"
        };

        let progress = self.finish_item(
            item_progress,
            format!(
                "{}: {} -> would have been applied, but not committed (batch rolled back)",
                console::style(label).yellow().bold(),
                source
            ),
            true,
        );
        self.increment_completed(progress);

        if let Some(resource) = resource
            && self.should_print_verbose_resource()
        {
            self.print_verbose_resource(progress, resource)?;
        }

        Ok(())
    }

    /// Prints an item the batch never reached because it stopped at an
    /// earlier rejection/failure.
    fn print_not_attempted(&self, item_progress: ApplyItemProgress<'_>, source: &str) {
        let progress = self.finish_item(
            item_progress,
            format!(
                "{}: {}",
                console::style("Not attempted (batch rolled back)").dim(),
                source
            ),
            true,
        );
        self.increment_completed(progress);
    }

    fn finish_progress(
        maybe_progress: Option<&ApplyMultiProgress>,
        draw_thread: Option<std::thread::JoinHandle<()>>,
    ) {
        if let Some(progress) = maybe_progress {
            progress.finish();
        }

        if let Some(draw_thread) = draw_thread {
            draw_thread.join().unwrap();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ApplyPrinter<'_> {
    // Internal helpers

    fn print_line(&self, progress: Option<&ApplyMultiProgress>, message: impl Into<String>) {
        let message = message.into();

        if let Some(progress) = progress {
            progress.println(message);
        } else {
            eprintln!("{message}");
        }
    }

    fn begin_item<'a>(
        &self,
        progress: Option<&'a ApplyMultiProgress>,
        source: &str,
    ) -> ApplyItemProgress<'a> {
        if let Some(progress) = progress {
            return ApplyItemProgress {
                progress: Some(progress),
                item: Some(progress.begin(source.to_string())),
            };
        }

        ApplyItemProgress {
            progress: None,
            item: None,
        }
    }

    fn should_print_success_line(&self) -> bool {
        !self.output_config.quiet
    }

    fn should_print_verbose_resource(&self) -> bool {
        self.output_config.verbosity_level > 0
    }

    fn print_warning_lines(
        &self,
        progress: Option<&ApplyMultiProgress>,
        source: &str,
        warnings: &[ResourceWarning],
    ) {
        for warning in warnings {
            self.print_line(
                progress,
                format!(
                    "  {} [{}] {} {}",
                    console::style("warning").yellow().bold(),
                    warning.code,
                    Self::warning_target(source, warning),
                    warning.message
                ),
            );
        }
    }

    fn print_change_lines(
        &self,
        progress: Option<&ApplyMultiProgress>,
        source: &str,
        changes: &[ApplyManifestChange],
    ) {
        for change in changes {
            if Self::should_render_change_as_block(change.before.as_ref(), change.after.as_ref()) {
                self.print_line(
                    progress,
                    format!(
                        "  {} {} {}:",
                        console::style("change").cyan().bold(),
                        change.kind,
                        Self::change_target(source, change),
                    ),
                );
                self.print_line(
                    progress,
                    format!(
                        "    {}:\n{}",
                        console::style("before").dim(),
                        Self::indent_block(
                            &Self::format_json_value_for_block(change.before.as_ref()),
                            "      "
                        )
                    ),
                );
                self.print_line(
                    progress,
                    format!(
                        "    {}:\n{}",
                        console::style("after").dim(),
                        Self::indent_block(
                            &Self::format_json_value_for_block(change.after.as_ref()),
                            "      "
                        )
                    ),
                );
            } else {
                self.print_line(
                    progress,
                    format!(
                        "  {} {} {}: {} -> {}",
                        console::style("change").cyan().bold(),
                        change.kind,
                        Self::change_target(source, change),
                        Self::format_optional_json_value(change.before.as_ref()),
                        Self::format_optional_json_value(change.after.as_ref()),
                    ),
                );
            }
        }
    }

    fn print_verbose_resource(
        &self,
        progress: Option<&ApplyMultiProgress>,
        resource: &Resource,
    ) -> Result<(), CLIError> {
        let rendered = Self::render_verbose_resource(resource)?;

        self.print_line(
            progress,
            format!(
                "  {}:\n{}",
                console::style("resource").dim(),
                Self::indent_block(rendered.trim_end(), "    ")
            ),
        );

        Ok(())
    }

    fn render_verbose_resource(resource: &Resource) -> Result<String, CLIError> {
        #[serde_with::serde_as]
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RenderedResourceHeaders<'a> {
            id: &'a kamu_resources::ResourceID,
            #[serde_as(as = "odf::metadata::serde::yaml::auth::AccountHandle")]
            account: &'a odf::AccountHandle,
            name: &'a str,
            #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceLabels")]
            labels: &'a kamu_resources::ResourceLabels,
            #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceAnnotations")]
            annotations: &'a kamu_resources::ResourceAnnotations,
            generation: u64,
            created_at: &'a DateTime<Utc>,
            updated_at: &'a DateTime<Utc>,
            deleted_at: &'a Option<DateTime<Utc>>,
        }

        impl<'a> RenderedResourceHeaders<'a> {
            fn new(resource: &'a Resource) -> Self {
                Self {
                    id: &resource.headers.id,
                    account: &resource.headers.account,
                    name: &resource.headers.name,
                    labels: &resource.headers.labels,
                    annotations: &resource.headers.annotations,
                    generation: resource.headers.generation,
                    created_at: &resource.headers.created_at,
                    updated_at: &resource.headers.updated_at,
                    deleted_at: &resource.headers.deleted_at,
                }
            }
        }

        #[derive(serde::Serialize)]
        struct RenderedResource<'a> {
            #[serde(rename = "$schema")]
            schema: &'a str,
            headers: RenderedResourceHeaders<'a>,
            spec: serde_yaml::Value,
            status: serde_yaml::Value,
        }

        serde_yaml::to_string(&RenderedResource {
            schema: resource.schema.as_str(),
            headers: RenderedResourceHeaders::new(resource),
            spec: common::json_to_yaml_value(&resource.spec),
            status: common::json_to_yaml_value(&kamu_resources::resource_status_to_json(
                &resource.status,
            )),
        })
        .map_err(CLIError::critical)
    }

    fn accepted_status_line(
        &self,
        source: &str,
        result: &ExecutedResourceManifestResult,
    ) -> Result<String, CLIError> {
        let label = result.outcome.label(self.dry_run);
        let style = match result.outcome {
            ApplyResourceOutcome::Created | ApplyResourceOutcome::Updated => {
                console::style(label).green().bold()
            }
            ApplyResourceOutcome::Untouched => console::style(label).yellow().bold(),
        };

        let kind_name = resource_type_name(&result.resource.schema)
            .map(|type_name| type_name.to_string())
            .map_err(CLIError::critical)?;

        Ok(format!(
            "{}: {} -> {}/{}",
            style, source, kind_name, result.resource.headers.name
        ))
    }

    fn error_status_line(&self, source: impl std::fmt::Display, label: &str) -> String {
        let label = if self.dry_run {
            format!("{label} (dry-run)")
        } else {
            label.to_string()
        };

        format!("{}: {}", console::style(label).red().bold(), source)
    }

    fn print_error_detail(
        &self,
        progress: Option<&ApplyMultiProgress>,
        source: impl std::fmt::Display,
        message: impl Into<String>,
    ) {
        self.print_line(
            progress,
            format!(
                "  {} {} {}",
                console::style("error").red().bold(),
                source,
                message.into()
            ),
        );
    }

    fn finish_item<'a>(
        &self,
        progress: ApplyItemProgress<'a>,
        message: String,
        print_when_plain: bool,
    ) -> Option<&'a ApplyMultiProgress> {
        match (progress.progress, progress.item) {
            (Some(progress), Some(item)) => {
                item.finish_with_message(message);
                Some(progress)
            }
            _ => {
                if print_when_plain {
                    eprintln!("{message}");
                }
                None
            }
        }
    }

    fn increment_completed(&self, progress: Option<&ApplyMultiProgress>) {
        if let Some(progress) = progress {
            progress.increment_completed();
        }
    }

    fn format_optional_json_value(value: Option<&serde_json::Value>) -> String {
        match value {
            Some(value) => {
                serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".to_string())
            }
            None => "null".to_string(),
        }
    }

    fn format_json_value_for_block(value: Option<&serde_json::Value>) -> String {
        match value {
            Some(value) => {
                serde_json::to_string_pretty(value).unwrap_or_else(|_| "<unprintable>".to_string())
            }
            None => "null".to_string(),
        }
    }

    fn should_render_change_as_block(
        before: Option<&serde_json::Value>,
        after: Option<&serde_json::Value>,
    ) -> bool {
        Self::is_structured_json_value(before) || Self::is_structured_json_value(after)
    }

    fn is_structured_json_value(value: Option<&serde_json::Value>) -> bool {
        matches!(
            value,
            Some(serde_json::Value::Array(_) | serde_json::Value::Object(_))
        )
    }

    fn indent_block(value: &str, prefix: &str) -> String {
        value
            .lines()
            .map(|line| format!("{prefix}{line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn warning_target(source: &str, warning: &ResourceWarning) -> String {
        match warning.path.as_deref() {
            Some(path) => format!("{source} {path}"),
            None => source.to_string(),
        }
    }

    fn change_target(source: &str, change: &ApplyManifestChange) -> String {
        format!("{source} {}", change.path)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
