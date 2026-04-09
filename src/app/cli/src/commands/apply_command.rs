// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use internal_error::BoxedError;
use kamu_resources::{
    ApplyManifestChange,
    ApplyManifestRejection,
    ApplyResourceOutcome,
    ResourceView,
    ResourceWarning,
};
use kamu_resources_facade::ResourceFacade;
use thiserror::Error;

use super::{BatchError, CLIError, Command, common};
use crate::output::{ApplyMultiProgress, OutputConfig};
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{
    DiscoverResourceManifestsResult,
    DiscoveredResourceManifest,
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
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_manifest_discovery_service: Arc<dyn ResourceManifestDiscoveryService>,
    resource_manifest_execution_service: Arc<dyn ResourceManifestExecutionService>,
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
    continue_on_error: bool,

    #[dill::component(explicit)]
    format: Option<crate::cli::ResourceManifestFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ApplyCommand {
    fn discover_manifests_phase(
        &self,
        printer: &ApplyPrinter,
        summary: &mut ApplySummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) -> Result<Vec<DiscoveredResourceManifest>, CLIError> {
        let DiscoverResourceManifestsResult {
            manifests,
            errors: discovery_errors,
        } = self.resource_manifest_discovery_service.discover(
            self.files.clone(),
            self.recursive,
            self.format,
        );

        for (input, err) in discovery_errors {
            summary.record_failed();
            printer.print_discovery_error(&input, err.to_string());

            errors.push((
                Box::<dyn std::error::Error + Send + Sync>::from(err),
                format!("Failed to process input {}", input.display()),
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

    async fn apply_manifests_phase(
        &self,
        printer: &ApplyPrinter<'_>,
        resource_facade: &dyn ResourceFacade,
        manifests: Vec<DiscoveredResourceManifest>,
        maybe_progress: Option<&ApplyMultiProgress>,
        summary: &mut ApplySummary,
        errors: &mut Vec<(BoxedError, String)>,
    ) -> Result<(), CLIError> {
        for manifest in manifests {
            let item_progress = printer.start_item(maybe_progress, &manifest.source);

            match self
                .resource_manifest_execution_service
                .execute(resource_facade, &manifest, self.dry_run)
                .await
            {
                Ok(ExecuteResourceManifestOutcome::Accepted(result)) => {
                    summary.record_accepted(&result);
                    printer.print_accepted(
                        item_progress,
                        &manifest.source,
                        &result,
                        self.dry_run,
                    )?;
                }
                Ok(ExecuteResourceManifestOutcome::Rejected(rejection)) => {
                    summary.record_rejected();
                    printer.print_rejected(
                        item_progress,
                        &manifest.source,
                        rejection.message.clone(),
                    );

                    errors.push((
                        Box::<dyn std::error::Error + Send + Sync>::from(ApplyRejectedError::from(
                            rejection,
                        )),
                        format!("Manifest {} was rejected", manifest.source.display()),
                    ));

                    if !self.continue_on_error {
                        break;
                    }
                }
                Err(err) => {
                    summary.record_failed();
                    printer.print_failed(item_progress, &manifest.source, err.to_string());

                    errors.push((
                        Box::<dyn std::error::Error + Send + Sync>::from(err),
                        format!(
                            "Failed to {} manifest {}",
                            if self.dry_run { "plan" } else { "apply" },
                            manifest.source.display()
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ApplyCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.files.is_empty() {
            return Err(CLIError::usage_error(
                "Specify at least one manifest path using -f",
            ));
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

        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

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
            resource_facade.as_ref(),
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
        source: &Path,
    ) -> ApplyItemProgress<'b> {
        self.begin_item(maybe_progress, source)
    }

    fn print_summary(&self, progress: Option<&ApplyMultiProgress>, summary: &ApplySummary) {
        self.print_line(
            progress,
            format!(
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
            ),
        );
    }

    fn batch_failure_summary(&self, error_count: usize) -> String {
        format!(
            "Failed to {} {} item(s)",
            if self.dry_run { "plan" } else { "apply" },
            error_count
        )
    }

    fn print_discovery_error(&self, source: &Path, message: String) {
        self.print_line(None, self.error_status_line(source, "Failed"));
        self.print_error_detail(None, source, message);
    }

    fn print_accepted(
        &self,
        item_progress: ApplyItemProgress<'_>,
        source: &Path,
        result: &ExecutedResourceManifestResult,
        dry_run: bool,
    ) -> Result<(), CLIError> {
        let progress = self.finish_item(
            item_progress,
            self.accepted_status_line(source, result),
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

    fn print_rejected(&self, item_progress: ApplyItemProgress<'_>, source: &Path, message: String) {
        let progress = self.finish_item(
            item_progress,
            self.error_status_line(source, "Rejected"),
            true,
        );
        self.increment_completed(progress);
        self.print_error_detail(progress, source, message);
    }

    fn print_failed(&self, item_progress: ApplyItemProgress<'_>, source: &Path, message: String) {
        let progress = self.finish_item(
            item_progress,
            self.error_status_line(source, "Failed"),
            true,
        );
        self.increment_completed(progress);
        self.print_error_detail(progress, source, message);
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
        source: &Path,
    ) -> ApplyItemProgress<'a> {
        if let Some(progress) = progress {
            return ApplyItemProgress {
                progress: Some(progress),
                item: Some(progress.begin(source.display().to_string())),
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
        source: &Path,
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
        source: &Path,
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
        resource: &ResourceView,
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

    fn render_verbose_resource(resource: &ResourceView) -> Result<String, CLIError> {
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct RenderedResourceViewMetadata<'a> {
            uid: &'a kamu_resources::ResourceUID,
            name: &'a str,
            description: &'a Option<String>,
            labels: &'a std::collections::BTreeMap<String, String>,
            annotations: &'a std::collections::BTreeMap<String, String>,
            generation: u64,
            created_at: &'a chrono::DateTime<chrono::Utc>,
            updated_at: &'a chrono::DateTime<chrono::Utc>,
            deleted_at: &'a Option<chrono::DateTime<chrono::Utc>>,
        }

        impl<'a> RenderedResourceViewMetadata<'a> {
            fn new(resource: &'a ResourceView) -> Self {
                Self {
                    uid: &resource.metadata.uid,
                    name: &resource.metadata.name,
                    description: &resource.metadata.description,
                    labels: &resource.metadata.labels,
                    annotations: &resource.metadata.annotations,
                    generation: resource.metadata.generation,
                    created_at: &resource.metadata.created_at,
                    updated_at: &resource.metadata.updated_at,
                    deleted_at: &resource.metadata.deleted_at,
                }
            }
        }

        #[derive(serde::Serialize)]
        struct RenderedResourceView<'a> {
            kind: &'a str,
            #[serde(rename = "apiVersion")]
            api_version: &'a str,
            account: &'a kamu_resources::ResourceViewAccount,
            metadata: RenderedResourceViewMetadata<'a>,
            #[serde(rename = "lastReconciledAt")]
            last_reconciled_at: &'a Option<chrono::DateTime<chrono::Utc>>,
            spec: serde_yaml::Value,
            status: Option<serde_yaml::Value>,
        }

        serde_yaml::to_string(&RenderedResourceView {
            kind: &resource.kind,
            api_version: &resource.api_version,
            account: &resource.account,
            metadata: RenderedResourceViewMetadata::new(resource),
            last_reconciled_at: &resource.last_reconciled_at,
            spec: common::json_to_yaml_value(&resource.spec),
            status: resource.status.as_ref().map(common::json_to_yaml_value),
        })
        .map_err(CLIError::critical)
    }

    fn accepted_status_line(
        &self,
        source: &Path,
        result: &ExecutedResourceManifestResult,
    ) -> String {
        let label = result.outcome.label(self.dry_run);
        let style = match result.outcome {
            ApplyResourceOutcome::Created | ApplyResourceOutcome::Updated => {
                console::style(label).green().bold()
            }
            ApplyResourceOutcome::Untouched => console::style(label).yellow().bold(),
        };

        format!(
            "{}: {} -> {}/{}",
            style,
            source.display(),
            result.resource.kind,
            result.resource.metadata.name
        )
    }

    fn error_status_line(&self, source: &Path, label: &str) -> String {
        let label = if self.dry_run {
            format!("{label} (dry-run)")
        } else {
            label.to_string()
        };

        format!(
            "{}: {}",
            console::style(label).red().bold(),
            source.display(),
        )
    }

    fn print_error_detail(
        &self,
        progress: Option<&ApplyMultiProgress>,
        source: &Path,
        message: impl Into<String>,
    ) {
        self.print_line(
            progress,
            format!(
                "  {} {} {}",
                console::style("error").red().bold(),
                source.display(),
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

    fn warning_target(source: &Path, warning: &ResourceWarning) -> String {
        match warning.path.as_deref() {
            Some(path) => format!("{} {}", source.display(), path),
            None => source.display().to_string(),
        }
    }

    fn change_target(source: &Path, change: &ApplyManifestChange) -> String {
        format!("{} {}", source.display(), change.path)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
