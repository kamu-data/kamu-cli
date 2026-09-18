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
use crate::resource_context::{
    ResolvedResourceContext,
    ResourceContextReporter,
    ResourceContextResolver,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextCommand {
    resource_context_resolver: Arc<ResourceContextResolver>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    output_config: Arc<OutputConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ContextCommand {
    fn record_batch(&self) -> Result<RecordBatch, CLIError> {
        let resolved_context = self.resource_context_resolver.resolve(None)?;
        let (name, kind, scope) = self.resource_context_reporter.describe(&resolved_context);

        let url = match resolved_context {
            ResolvedResourceContext::LocalWorkspace => "-".to_string(),
            ResolvedResourceContext::RemoteWorkspace { backend_url, .. } => backend_url.to_string(),
        };

        self.records(vec![
            Arc::new(StringArray::from(vec![name])),
            Arc::new(StringArray::from(vec![kind])),
            Arc::new(StringArray::from(vec![scope])),
            Arc::new(StringArray::from(vec![url])),
        ])
        .map_err(CLIError::critical)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let mut writer = self
            .output_config
            .get_records_writer(&self.schema(), self.records_format());

        writer.write_batch(&self.record_batch()?)?;
        writer.finish()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl OutputWriter for ContextCommand {
    fn records_format(&self) -> RecordsFormat {
        RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("c"),
                ColumnFormat::new().with_style_spec("c"),
                ColumnFormat::new().with_style_spec("l"),
            ])
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("Name", DataType::Utf8, false),
            Field::new("Kind", DataType::Utf8, false),
            Field::new("Scope", DataType::Utf8, false),
            Field::new("Url", DataType::Utf8, false),
        ]))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
