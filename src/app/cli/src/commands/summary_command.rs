// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use kamu_resources::ResourceTypeCountSummary;

use super::{CLIError, Command};
use crate::cli::SummaryOutputFormat;
use crate::output::*;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{ResourceSummaryService, ResourceSummaryView};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct SummaryCommand {
    resource_summary_service: Arc<dyn ResourceSummaryService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    output_format: Option<SummaryOutputFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SummaryCommand {
    fn effective_output_format(&self) -> SummaryOutputFormat {
        self.output_format.unwrap_or(SummaryOutputFormat::Table)
    }

    fn render_table(&self, summary: &ResourceSummaryView) -> Result<(), CLIError> {
        println!("Context");
        for (key, value) in summary.context.rows() {
            println!("  {key:<20} {value}");
        }
        println!();
        println!("Resource Counts");

        type Count = fn(&ResourceTypeCountSummary) -> u64;

        // The count columns after `Type`: header, alignment and value
        let count_columns: [(&str, &str, Count); 8] = [
            ("Total", "l", |item| item.total_count),
            ("Pending", "r", |item| item.phase_counts.pending),
            ("Reconciling", "r", |item| item.phase_counts.reconciling),
            ("Ready", "r", |item| item.phase_counts.ready),
            ("Degraded", "r", |item| item.phase_counts.degraded),
            ("Failed", "r", |item| item.phase_counts.failed),
            ("Deleting", "r", |item| item.phase_counts.deleting),
            ("Deleted", "r", |item| item.phase_counts.deleted),
        ];

        let schema = Arc::new(Schema::new(
            std::iter::once(Field::new("Type", DataType::Utf8, false))
                .chain(
                    count_columns
                        .iter()
                        .map(|(name, _, _)| Field::new(*name, DataType::UInt64, false)),
                )
                .collect::<Vec<_>>(),
        ));

        let type_column: ArrayRef = Arc::new(StringArray::from(
            summary
                .resource_counts
                .iter()
                .map(|item| item.type_name.to_string())
                .collect::<Vec<_>>(),
        ));
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            std::iter::once(type_column)
                .chain(count_columns.iter().map(|(_, _, count)| {
                    Arc::new(UInt64Array::from(
                        summary
                            .resource_counts
                            .iter()
                            .map(count)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                }))
                .collect(),
        )
        .map_err(CLIError::critical)?;

        let output_config = OutputConfig {
            format: OutputFormat::Table,
            ..self.output_config.as_ref().clone()
        };
        let records_format = RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(
                std::iter::once(ColumnFormat::new().with_style_spec("l"))
                    .chain(
                        count_columns
                            .iter()
                            .map(|(_, style, _)| ColumnFormat::new().with_style_spec(*style)),
                    )
                    .collect(),
            );

        let mut writer = output_config.get_records_writer(&schema, records_format);
        writer
            .write_batch(&record_batch)
            .map_err(CLIError::critical)?;
        writer.finish().map_err(CLIError::critical)?;

        Ok(())
    }

    fn render_json(&self, summary: &ResourceSummaryView) -> Result<(), CLIError> {
        serde_json::to_writer_pretty(std::io::stdout(), summary).map_err(CLIError::critical)
    }

    fn render_yaml(&self, summary: &ResourceSummaryView) -> Result<(), CLIError> {
        serde_yaml::to_writer(std::io::stdout(), summary).map_err(CLIError::critical)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for SummaryCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;

        if self.effective_output_format() == SummaryOutputFormat::Table {
            self.resource_context_reporter
                .report_usage("Fetching resource summary from context", &resolved_context);
        }

        let summary = self
            .resource_summary_service
            .summary(self.explicit_context_name.as_deref())
            .await?;

        match self.effective_output_format() {
            SummaryOutputFormat::Table => self.render_table(&summary)?,
            SummaryOutputFormat::Json => self.render_json(&summary)?,
            SummaryOutputFormat::Yaml => self.render_yaml(&summary)?,
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
