// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use datafusion::arrow::array::{
    ArrayRef,
    BooleanArray,
    StringArray,
    TimestampMicrosecondArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use internal_error::InternalError;
use kamu_resources::{
    ResourceKindDescriptor,
    ResourceListColumnDataType,
    ResourceListColumnDescriptor,
    ResourceListColumnValue,
    ResourceListColumnVisibility,
    ResourceSummaryView,
};
use kamu_resources_facade::ResourceFacade;
use odf::utils::data::format::RecordsWriter;

use super::{CLIError, Command, common};
use crate::accounts;
use crate::output::*;
use crate::resource_context::{ResolvedResourceContext, ResourceContextReporter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ListResourcesCommand {
    resource_facade: Arc<dyn ResourceFacade>,
    resource_context_reporter: Arc<ResourceContextReporter>,
    resolved_context: ResolvedResourceContext,
    related_account: accounts::RelatedAccountIndication,
    kind_descriptor: ResourceKindDescriptor,
    output_config: Arc<OutputConfig>,
    detail_level: u8,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ListResourcesCommand {
    pub fn new(
        resource_facade: Arc<dyn ResourceFacade>,
        resource_context_reporter: Arc<ResourceContextReporter>,
        resolved_context: ResolvedResourceContext,
        related_account: accounts::RelatedAccountIndication,
        kind_descriptor: ResourceKindDescriptor,
        output_config: Arc<OutputConfig>,
        detail_level: u8,
    ) -> Self {
        Self {
            resource_facade,
            resource_context_reporter,
            resolved_context,
            related_account,
            kind_descriptor,
            output_config,
            detail_level,
        }
    }

    fn generic_resource_columns(&self) -> Vec<ResourceGenericColumn> {
        let mut columns = vec![
            ResourceGenericColumn::Name,
            ResourceGenericColumn::Phase,
            ResourceGenericColumn::Readiness,
            ResourceGenericColumn::Updated,
        ];

        if self.detail_level > 0 {
            columns.extend([
                ResourceGenericColumn::Description,
                ResourceGenericColumn::Generation,
                ResourceGenericColumn::ObservedGeneration,
                ResourceGenericColumn::Age,
            ]);
        }

        columns
    }

    fn selected_resource_columns(
        &self,
        kind_descriptor: &ResourceKindDescriptor,
    ) -> Vec<ResourceListColumnDescriptor> {
        kind_descriptor
            .list_columns
            .iter()
            .filter(|column| {
                column.visibility == ResourceListColumnVisibility::Default
                    || (self.detail_level > 0
                        && column.visibility == ResourceListColumnVisibility::WideOnly)
            })
            .cloned()
            .collect()
    }

    fn resource_schema(&self, extra_columns: &[ResourceListColumnDescriptor]) -> Arc<Schema> {
        let tz = Some(Arc::from("+00:00"));
        let mut fields = Vec::new();

        for column in self.generic_resource_columns() {
            fields.push(match column {
                ResourceGenericColumn::Name => Field::new("Name", DataType::Utf8, false),
                ResourceGenericColumn::Phase => Field::new("Phase", DataType::Utf8, true),
                ResourceGenericColumn::Readiness => {
                    Field::new("Readiness", DataType::Boolean, true)
                }
                ResourceGenericColumn::Updated => Field::new(
                    "Updated",
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    false,
                ),
                ResourceGenericColumn::Description => {
                    Field::new("Description", DataType::Utf8, true)
                }
                ResourceGenericColumn::Generation => {
                    Field::new("Generation", DataType::UInt64, false)
                }
                ResourceGenericColumn::ObservedGeneration => {
                    Field::new("Observed Generation", DataType::UInt64, true)
                }
                ResourceGenericColumn::Age => Field::new(
                    "Age",
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                    false,
                ),
            });
        }

        for column in extra_columns {
            let data_type = match column.data_type {
                ResourceListColumnDataType::String => DataType::Utf8,
                ResourceListColumnDataType::UInt64 => DataType::UInt64,
                ResourceListColumnDataType::Bool => DataType::Boolean,
            };
            fields.push(Field::new(column.header.clone(), data_type, true));
        }

        Arc::new(Schema::new(fields))
    }

    fn resource_records_format(
        &self,
        extra_columns: &[ResourceListColumnDescriptor],
    ) -> RecordsFormat {
        let mut column_formats = Vec::new();

        for column in self.generic_resource_columns() {
            column_formats.push(match column {
                ResourceGenericColumn::Name | ResourceGenericColumn::Description => {
                    ColumnFormat::new().with_style_spec("l")
                }
                ResourceGenericColumn::Phase | ResourceGenericColumn::Readiness => {
                    ColumnFormat::new().with_style_spec("c")
                }
                ResourceGenericColumn::Updated | ResourceGenericColumn::Age => ColumnFormat::new()
                    .with_style_spec("c")
                    .with_value_fmt_t(common::humanize_relative_date),
                ResourceGenericColumn::Generation | ResourceGenericColumn::ObservedGeneration => {
                    ColumnFormat::new()
                        .with_style_spec("r")
                        .with_value_fmt_t(common::humanize_quantity)
                }
            });
        }

        for column in extra_columns {
            column_formats.push(match column.data_type {
                ResourceListColumnDataType::String => ColumnFormat::new().with_style_spec("l"),
                ResourceListColumnDataType::UInt64 => ColumnFormat::new()
                    .with_style_spec("r")
                    .with_value_fmt_t(common::humanize_quantity),
                ResourceListColumnDataType::Bool => ColumnFormat::new().with_style_spec("c"),
            });
        }

        RecordsFormat::new()
            .with_default_column_format(ColumnFormat::default().with_null_value("-"))
            .with_column_formats(column_formats)
    }

    fn resource_column_value<'a>(
        resource: &'a ResourceSummaryView,
        key: &str,
    ) -> Option<&'a ResourceListColumnValue> {
        resource
            .list_values
            .iter()
            .find(|value| value.key == key)
            .map(|value| &value.value)
    }

    fn resource_column_string(
        resource: &ResourceSummaryView,
        column: &ResourceListColumnDescriptor,
    ) -> Result<Option<String>, CLIError> {
        match Self::resource_column_value(resource, &column.key) {
            None => Ok(None),
            Some(ResourceListColumnValue::String(value)) => Ok(Some(value.clone())),
            Some(other) => Err(CLIError::critical(InternalError::new(format!(
                "List column '{}' expected string value but got {:?}",
                column.key, other
            )))),
        }
    }

    fn resource_column_u64(
        resource: &ResourceSummaryView,
        column: &ResourceListColumnDescriptor,
    ) -> Result<Option<u64>, CLIError> {
        match Self::resource_column_value(resource, &column.key) {
            None => Ok(None),
            Some(ResourceListColumnValue::UInt64(value)) => Ok(Some(*value)),
            Some(other) => Err(CLIError::critical(InternalError::new(format!(
                "List column '{}' expected uint64 value but got {:?}",
                column.key, other
            )))),
        }
    }

    fn resource_column_bool(
        resource: &ResourceSummaryView,
        column: &ResourceListColumnDescriptor,
    ) -> Result<Option<bool>, CLIError> {
        match Self::resource_column_value(resource, &column.key) {
            None => Ok(None),
            Some(ResourceListColumnValue::Bool(value)) => Ok(Some(*value)),
            Some(other) => Err(CLIError::critical(InternalError::new(format!(
                "List column '{}' expected bool value but got {:?}",
                column.key, other
            )))),
        }
    }

    async fn list_all_resources(&self, kind: &str) -> Result<Vec<ResourceSummaryView>, CLIError> {
        let mut page = 0;
        let mut items = Vec::new();

        loop {
            let page_items = self
                .resource_facade
                .list(kamu_resources_facade::ListResourcesRequest {
                    kind: kind.to_string(),
                    account: None,
                    pagination: PaginationOpts::from_page(page, RESOURCE_PAGE_SIZE),
                })
                .await?;

            let fetched = page_items.len();
            items.extend(page_items);

            if fetched < RESOURCE_PAGE_SIZE {
                break;
            }
            page += 1;
        }

        Ok(items)
    }

    async fn load_resources(&self) -> Result<Vec<ResourceSummaryView>, CLIError> {
        let mut resources = self.list_all_resources(&self.kind_descriptor.kind).await?;

        resources.sort_by(|lhs, rhs| lhs.name.cmp(&rhs.name));

        Ok(resources)
    }

    fn make_writer(
        &self,
        extra_columns: &[ResourceListColumnDescriptor],
    ) -> (Arc<Schema>, Box<dyn RecordsWriter>) {
        let schema = self.resource_schema(extra_columns);
        let records_format = self.resource_records_format(extra_columns);
        let writer = self
            .output_config
            .get_records_writer(&schema, records_format);

        (schema, writer)
    }

    fn generic_resource_arrays(&self, resources: &[ResourceSummaryView]) -> Vec<ArrayRef> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for column in self.generic_resource_columns() {
            columns.push(match column {
                ResourceGenericColumn::Name => Arc::new(StringArray::from(
                    resources
                        .iter()
                        .map(|resource| resource.name.clone())
                        .collect::<Vec<_>>(),
                )),
                ResourceGenericColumn::Phase => Arc::new(StringArray::from(
                    resources
                        .iter()
                        .map(|resource| {
                            resource
                                .status
                                .as_ref()
                                .and_then(|status| status.phase.map(|phase| phase.to_string()))
                        })
                        .collect::<Vec<_>>(),
                )),
                ResourceGenericColumn::Readiness => Arc::new(BooleanArray::from(
                    resources
                        .iter()
                        .map(|resource| resource.status.as_ref().and_then(|status| status.ready))
                        .collect::<Vec<_>>(),
                )),
                ResourceGenericColumn::Updated => Arc::new(
                    TimestampMicrosecondArray::from(
                        resources
                            .iter()
                            .map(|resource| Some(resource.updated_at.timestamp_micros()))
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone_utc(),
                ),
                ResourceGenericColumn::Description => Arc::new(StringArray::from(
                    resources
                        .iter()
                        .map(|resource| resource.description.clone())
                        .collect::<Vec<_>>(),
                )),
                ResourceGenericColumn::Generation => Arc::new(UInt64Array::from(
                    resources
                        .iter()
                        .map(|resource| resource.generation)
                        .collect::<Vec<_>>(),
                )),
                ResourceGenericColumn::ObservedGeneration => Arc::new(UInt64Array::from(
                    resources
                        .iter()
                        .map(|resource| {
                            resource
                                .status
                                .as_ref()
                                .and_then(|status| status.observed_generation)
                        })
                        .collect::<Vec<_>>(),
                )),
                ResourceGenericColumn::Age => Arc::new(
                    TimestampMicrosecondArray::from(
                        resources
                            .iter()
                            .map(|resource| Some(resource.created_at.timestamp_micros()))
                            .collect::<Vec<_>>(),
                    )
                    .with_timezone_utc(),
                ),
            });
        }

        columns
    }

    fn extra_resource_arrays(
        &self,
        resources: &[ResourceSummaryView],
        extra_columns: &[ResourceListColumnDescriptor],
    ) -> Result<Vec<ArrayRef>, CLIError> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for column in extra_columns {
            columns.push(match column.data_type {
                ResourceListColumnDataType::String => Arc::new(StringArray::from(
                    resources
                        .iter()
                        .map(|resource| Self::resource_column_string(resource, column))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ResourceListColumnDataType::UInt64 => Arc::new(UInt64Array::from(
                    resources
                        .iter()
                        .map(|resource| Self::resource_column_u64(resource, column))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                ResourceListColumnDataType::Bool => Arc::new(BooleanArray::from(
                    resources
                        .iter()
                        .map(|resource| Self::resource_column_bool(resource, column))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
            });
        }

        Ok(columns)
    }

    fn make_record_batch(
        &self,
        schema: Arc<Schema>,
        resources: &[ResourceSummaryView],
        extra_columns: &[ResourceListColumnDescriptor],
    ) -> Result<RecordBatch, CLIError> {
        let mut columns = self.generic_resource_arrays(resources);
        columns.extend(self.extra_resource_arrays(resources, extra_columns)?);

        Ok(RecordBatch::try_new(schema, columns).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl Command for ListResourcesCommand {
    async fn validate_args(&self) -> Result<(), CLIError> {
        if self.related_account.is_explicit() {
            return Err(CLIError::usage_error(
                "Listing resources does not support --target-account or --all-accounts",
            ));
        }

        Ok(())
    }

    async fn run(&self) -> Result<(), CLIError> {
        let resources = self.load_resources().await?;
        let extra_columns = self.selected_resource_columns(&self.kind_descriptor);
        let (schema, mut writer) = self.make_writer(&extra_columns);

        if self.output_config.format == OutputFormat::Table {
            self.resource_context_reporter
                .report_usage("Fetching resources from context", &self.resolved_context);
        }

        let records = self.make_record_batch(schema, &resources, &extra_columns)?;
        writer.write_batch(&records)?;
        writer.finish()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceGenericColumn {
    Name,
    Phase,
    Readiness,
    Updated,
    Description,
    Generation,
    ObservedGeneration,
    Age,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
