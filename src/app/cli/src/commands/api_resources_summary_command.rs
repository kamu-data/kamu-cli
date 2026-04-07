// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use super::{CLIError, Command};
use crate::cli::ApiResourcesSummaryOutputFormat;
use crate::output::*;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::{ResourceSummaryService, ResourceSummaryView};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ApiResourcesSummaryCommand {
    resource_summary_service: Arc<dyn ResourceSummaryService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,

    #[dill::component(explicit)]
    output_format: Option<ApiResourcesSummaryOutputFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ApiResourcesSummaryCommand {
    fn effective_output_format(&self) -> ApiResourcesSummaryOutputFormat {
        self.output_format
            .unwrap_or(ApiResourcesSummaryOutputFormat::Table)
    }

    fn render_table(&self, summary: &ResourceSummaryView) -> Result<(), CLIError> {
        println!("Context");
        for (key, value) in summary.context.rows() {
            println!("  {key:<20} {value}");
        }
        println!();
        println!("Resource Counts");

        let schema = Arc::new(Schema::new(vec![
            Field::new("Name", DataType::Utf8, false),
            Field::new("Kind", DataType::Utf8, false),
            Field::new("API Version", DataType::Utf8, false),
            Field::new("Total", DataType::UInt64, false),
            Field::new("Pending", DataType::UInt64, false),
            Field::new("Reconciling", DataType::UInt64, false),
            Field::new("Ready", DataType::UInt64, false),
            Field::new("Degraded", DataType::UInt64, false),
            Field::new("Failed", DataType::UInt64, false),
        ]));

        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.name.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.kind.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.api_version.clone())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.total_count)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.phase_counts.pending)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.phase_counts.reconciling)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.phase_counts.ready)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.phase_counts.degraded)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    summary
                        .resource_counts
                        .iter()
                        .map(|item| item.phase_counts.failed)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(CLIError::critical)?;

        let output_config = OutputConfig {
            format: OutputFormat::Table,
            ..self.output_config.as_ref().clone()
        };
        let records_format = RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("r"),
                ColumnFormat::new().with_style_spec("r"),
                ColumnFormat::new().with_style_spec("r"),
                ColumnFormat::new().with_style_spec("r"),
                ColumnFormat::new().with_style_spec("r"),
                ColumnFormat::new().with_style_spec("r"),
            ]);

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
impl Command for ApiResourcesSummaryCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;

        if self.effective_output_format() == ApiResourcesSummaryOutputFormat::Table {
            self.resource_context_reporter
                .report_usage("Fetching resource summary from context", &resolved_context);
        }

        let summary = self
            .resource_summary_service
            .summary(self.explicit_context_name.as_deref())
            .await?;

        match self.effective_output_format() {
            ApiResourcesSummaryOutputFormat::Table => self.render_table(&summary)?,
            ApiResourcesSummaryOutputFormat::Json => self.render_json(&summary)?,
            ApiResourcesSummaryOutputFormat::Yaml => self.render_yaml(&summary)?,
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
