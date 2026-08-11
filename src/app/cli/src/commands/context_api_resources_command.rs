// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use super::{CLIError, Command};
use crate::output::*;
use crate::resource_context::{ResourceContextReporter, ResourceContextResolver};
use crate::resources::ResourceFacadeFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextApiResourcesCommand {
    resource_facade_factory: Arc<dyn ResourceFacadeFactory>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ContextApiResourcesCommand {
    async fn record_batch(&self) -> Result<RecordBatch, CLIError> {
        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        let supported_resource_types = resource_facade.list_supported_resource_types().await?;

        let col_name: Vec<_> = supported_resource_types
            .iter()
            .map(|item| item.canonical_selector.to_string())
            .collect();
        let col_aliases: Vec<_> = supported_resource_types
            .iter()
            .map(|item| {
                item.selector_aliases
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();
        let col_schema: Vec<_> = supported_resource_types
            .iter()
            .map(|item| item.schema.to_string())
            .collect();

        self.records(vec![
            Arc::new(StringArray::from(col_name)),
            Arc::new(StringArray::from(col_aliases)),
            Arc::new(StringArray::from(col_schema)),
        ])
        .map_err(CLIError::critical)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextApiResourcesCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;
        if self.output_config.format == OutputFormat::Table {
            self.resource_context_reporter.report_usage(
                "Fetching supported resource types from context",
                &resolved_context,
            );
        }

        let mut writer = self
            .output_config
            .get_records_writer(&self.schema(), self.records_format());

        writer.write_batch(&self.record_batch().await?)?;
        writer.finish()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl OutputWriter for ContextApiResourcesCommand {
    fn records_format(&self) -> RecordsFormat {
        RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
            ])
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("Name", DataType::Utf8, false),
            Field::new("Aliases", DataType::Utf8, false),
            Field::new("Schema", DataType::Utf8, false),
        ]))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
