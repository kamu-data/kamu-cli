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
use internal_error::InternalError;

use super::{CLIError, Command};
use crate::WorkspaceService;
use crate::output::*;
use crate::resource_context::{
    LOCAL_CONTEXT_NAME,
    ResolvedResourceContext,
    ResourceContextLastTestResult,
    ResourceContextRegistryService,
    ResourceContextResolver,
    ResourceContextStoreScope,
    ScopedResourceContextRecord,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct ContextListCommand {
    resource_context_registry_service: Arc<ResourceContextRegistryService>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    workspace_service: Arc<WorkspaceService>,
    output_config: Arc<OutputConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ContextListCommand {
    fn record_batch(&self) -> Result<RecordBatch, InternalError> {
        let resolved_current_context = self.resource_context_resolver.resolve(None).ok();

        let mut contexts = self
            .resource_context_registry_service
            .list_effective_contexts_with_scope();
        contexts.sort_by(|a, b| a.context.name.cmp(&b.context.name));

        let mut col_current = Vec::new();
        let mut col_name = Vec::new();
        let mut col_kind = Vec::new();
        let mut col_scope = Vec::new();
        let mut col_status = Vec::new();
        let mut col_url = Vec::new();

        if self.workspace_service.is_in_workspace() {
            col_current.push(Self::current_marker(
                resolved_current_context.as_ref(),
                &ResolvedResourceContext::LocalWorkspace,
            ));
            col_name.push(LOCAL_CONTEXT_NAME.to_string());
            col_kind.push("Local".to_string());
            col_scope.push(Self::scope_label(ResourceContextStoreScope::Workspace).to_string());
            col_status.push("Local".to_string());
            col_url.push("-".to_string());
        }

        for scoped_context in contexts {
            let status = Self::status_label(&scoped_context).to_string();
            let resolved_context = ResolvedResourceContext::RemoteWorkspace {
                name: scoped_context.context.name.clone(),
                backend_url: scoped_context.context.backend_url.clone(),
            };

            col_current.push(Self::current_marker(
                resolved_current_context.as_ref(),
                &resolved_context,
            ));
            col_name.push(scoped_context.context.name);
            col_kind.push("Remote".to_string());
            col_scope.push(Self::scope_label(scoped_context.scope).to_string());
            col_status.push(status);
            col_url.push(scoped_context.context.backend_url.to_string());
        }

        self.records(vec![
            Arc::new(StringArray::from(col_current)),
            Arc::new(StringArray::from(col_name)),
            Arc::new(StringArray::from(col_kind)),
            Arc::new(StringArray::from(col_scope)),
            Arc::new(StringArray::from(col_status)),
            Arc::new(StringArray::from(col_url)),
        ])
    }

    fn current_marker(
        resolved_current_context: Option<&ResolvedResourceContext>,
        row_context: &ResolvedResourceContext,
    ) -> &'static str {
        if resolved_current_context == Some(row_context) {
            "*"
        } else {
            ""
        }
    }

    pub(crate) fn scope_label(scope: ResourceContextStoreScope) -> &'static str {
        match scope {
            ResourceContextStoreScope::Workspace => "Workspace",
            ResourceContextStoreScope::User => "User",
        }
    }

    fn status_label(context: &ScopedResourceContextRecord) -> &str {
        context
            .last_test_result
            .as_ref()
            .map(ResourceContextLastTestResult::label)
            .unwrap_or("Unknown")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ContextListCommand {
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

impl OutputWriter for ContextListCommand {
    fn records_format(&self) -> RecordsFormat {
        RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default())
            .with_column_formats(vec![
                ColumnFormat::new().with_style_spec("c"),
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("c"),
                ColumnFormat::new().with_style_spec("c"),
                ColumnFormat::new().with_style_spec("l"),
                ColumnFormat::new().with_style_spec("l"),
            ])
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("Current", DataType::Utf8, false),
            Field::new("Name", DataType::Utf8, false),
            Field::new("Kind", DataType::Utf8, false),
            Field::new("Scope", DataType::Utf8, false),
            Field::new("Status", DataType::Utf8, false),
            Field::new("Url", DataType::Utf8, false),
        ]))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
