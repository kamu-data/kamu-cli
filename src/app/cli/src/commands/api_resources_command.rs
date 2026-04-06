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
pub struct ApiResourcesCommand {
    resource_facade_factory: Arc<ResourceFacadeFactory>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    output_config: Arc<OutputConfig>,

    #[dill::component(explicit)]
    explicit_context_name: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ApiResourcesCommand {
    async fn record_batch(&self) -> Result<RecordBatch, CLIError> {
        let resource_facade = self
            .resource_facade_factory
            .get_resource_facade(self.explicit_context_name.as_deref())?;

        let supported_kinds = resource_facade
            .list_supported_kinds()
            .await
            .map_err(CLIError::critical)?;

        let col_name: Vec<_> = supported_kinds
            .iter()
            .map(|item| item.name.clone())
            .collect();
        let col_short_names: Vec<_> = supported_kinds
            .iter()
            .map(|item| item.short_names.join(","))
            .collect();
        let col_kind: Vec<_> = supported_kinds
            .iter()
            .map(|item| item.kind.clone())
            .collect();
        let col_api_version: Vec<_> = supported_kinds
            .iter()
            .map(|item| item.api_version.clone())
            .collect();

        self.records(vec![
            Arc::new(StringArray::from(col_name)),
            Arc::new(StringArray::from(col_short_names)),
            Arc::new(StringArray::from(col_kind)),
            Arc::new(StringArray::from(col_api_version)),
        ])
        .map_err(CLIError::critical)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ApiResourcesCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(self.explicit_context_name.as_deref())?;
        if self.output_config.format == OutputFormat::Table {
            self.resource_context_reporter.report_usage(
                "Fetching supported resource kinds from context",
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

impl OutputWriter for ApiResourcesCommand {
    fn records_format(&self) -> RecordsFormat {
        RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
            ])
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("Name", DataType::Utf8, false),
            Field::new("Short Names", DataType::Utf8, false),
            Field::new("Kind", DataType::Utf8, false),
            Field::new("API Version", DataType::Utf8, false),
        ]))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
