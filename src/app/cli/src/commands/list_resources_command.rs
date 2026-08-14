// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use database_common::{PaginationOpts, collect_all_pages};
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
    ResourceListColumnDataType,
    ResourceListColumnDescriptor,
    ResourceListColumnValue,
    ResourceListColumnVisibility,
    ResourceQuery,
    ResourceSummaryView,
    ResourceTypeDescriptor,
    resource_type_name,
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
    scope: ListResourcesScope,
    output_config: Arc<OutputConfig>,
    detail_level: u8,
    max_results: Option<NonZeroUsize>,
    label_filter: Option<kamu_resources::ResourceLabelFilterInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ListResourcesCommand {
    pub fn new(
        resource_facade: Arc<dyn ResourceFacade>,
        resource_context_reporter: Arc<ResourceContextReporter>,
        resolved_context: ResolvedResourceContext,
        related_account: accounts::RelatedAccountIndication,
        scope: ListResourcesScope,
        output_config: Arc<OutputConfig>,
        detail_level: u8,
        max_results: Option<NonZeroUsize>,
        label_filter: Option<kamu_resources::ResourceLabelFilterInput>,
    ) -> Self {
        Self {
            resource_facade,
            resource_context_reporter,
            resolved_context,
            related_account,
            scope,
            output_config,
            detail_level,
            max_results,
            label_filter,
        }
    }

    fn generic_resource_columns(&self) -> Vec<ResourceGenericColumn> {
        let mut columns = vec![ResourceGenericColumn::Name];

        if self.is_all_scope() {
            columns.push(ResourceGenericColumn::Type);
        }

        if self.detail_level > 0 {
            columns.push(ResourceGenericColumn::Id);
        }

        columns.extend([
            ResourceGenericColumn::Phase,
            ResourceGenericColumn::Readiness,
            ResourceGenericColumn::Updated,
        ]);

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

    fn selected_resource_columns(&self) -> Vec<ResourceListColumnDescriptor> {
        match &self.scope {
            ListResourcesScope::All(_) | ListResourcesScope::Types(_) => Vec::new(),
            ListResourcesScope::ByType(type_descriptor, _) => type_descriptor
                .list_columns
                .iter()
                .filter(|column| {
                    column.visibility == ResourceListColumnVisibility::Default
                        || (self.detail_level > 0
                            && column.visibility == ResourceListColumnVisibility::WideOnly)
                })
                .cloned()
                .collect(),
        }
    }

    fn is_all_scope(&self) -> bool {
        self.scope.is_generic()
    }

    fn resource_schema(&self, extra_columns: &[ResourceListColumnDescriptor]) -> Arc<Schema> {
        let tz = Some(Arc::from("+00:00"));
        let mut fields = Vec::new();

        for column in self.generic_resource_columns() {
            fields.push(match column {
                ResourceGenericColumn::Name => Field::new("Name", DataType::Utf8, false),
                ResourceGenericColumn::Type => Field::new("Type", DataType::Utf8, false),
                ResourceGenericColumn::Id => Field::new("ID", DataType::Utf8, false),
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
                ResourceGenericColumn::Name
                | ResourceGenericColumn::Type
                | ResourceGenericColumn::Id
                | ResourceGenericColumn::Description => ColumnFormat::new().with_style_spec("l"),
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

    async fn list_resources_by_selector(
        &self,
        schema: &kamu_resources::TypeUri,
        query: Option<&ResourceQuery>,
    ) -> Result<Vec<ResourceSummaryView>, CLIError> {
        if let Some(max_results) = self.max_results {
            self.resource_facade
                .list(kamu_resources_facade::ListResourcesRequest {
                    selectors: selectors_for_type(schema, query),
                    account: None,
                    pagination: PaginationOpts::from_max_results(max_results.get()),
                    label_filter: self.label_filter.clone(),
                })
                .await
                .map_err(Into::into)
        } else {
            collect_all_pages(RESOURCE_PAGE_SIZE, |pagination| async move {
                self.resource_facade
                    .list(kamu_resources_facade::ListResourcesRequest {
                        selectors: selectors_for_type(schema, query),
                        account: None,
                        pagination,
                        label_filter: self.label_filter.clone(),
                    })
                    .await
                    .map_err(Into::into)
            })
            .await
        }
    }

    async fn list_all_resources(
        &self,
        selectors: &[kamu_resources::ResourceSelector],
    ) -> Result<Vec<ResourceSummaryView>, CLIError> {
        if let Some(max_results) = self.max_results {
            self.resource_facade
                .list_all(kamu_resources_facade::ListAllResourcesRequest {
                    account: None,
                    label_filter: self.label_filter.clone(),
                    pagination: PaginationOpts::from_max_results(max_results.get()),
                    selectors: selectors.to_vec(),
                })
                .await
                .map_err(Into::into)
        } else {
            collect_all_pages(RESOURCE_PAGE_SIZE, |pagination| async move {
                self.resource_facade
                    .list_all(kamu_resources_facade::ListAllResourcesRequest {
                        account: None,
                        label_filter: self.label_filter.clone(),
                        pagination,
                        selectors: selectors.to_vec(),
                    })
                    .await
                    .map_err(Into::into)
            })
            .await
        }
    }

    async fn load_resources(&self) -> Result<Vec<ResourceSummaryView>, CLIError> {
        match &self.scope {
            ListResourcesScope::ByType(type_descriptor, query) => {
                let mut resources = self
                    .list_resources_by_selector(&type_descriptor.schema, query.as_ref())
                    .await?;
                resources.sort_by(|lhs, rhs| lhs.name.cmp(&rhs.name));
                Ok(resources)
            }
            ListResourcesScope::Types(type_queries) => {
                let selectors = type_queries
                    .iter()
                    .flat_map(|(type_descriptor, query)| {
                        selectors_for_type(&type_descriptor.schema, query.as_ref())
                    })
                    .collect::<Vec<_>>();
                self.load_generic_resources(&selectors).await
            }
            ListResourcesScope::All(query) => {
                self.load_generic_resources(&any_type_selectors(query.as_ref()))
                    .await
            }
        }
    }

    async fn load_generic_resources(
        &self,
        selectors: &[kamu_resources::ResourceSelector],
    ) -> Result<Vec<ResourceSummaryView>, CLIError> {
        let mut resources = self.list_all_resources(selectors).await?;
        resources.sort_by(|lhs, rhs| {
            lhs.name
                .cmp(&rhs.name)
                .then_with(|| lhs.schema.cmp(&rhs.schema))
                .then_with(|| lhs.id.to_string().cmp(&rhs.id.to_string()))
        });
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

    fn generic_resource_arrays(
        &self,
        resources: &[ResourceSummaryView],
    ) -> Result<Vec<ArrayRef>, CLIError> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for column in self.generic_resource_columns() {
            columns.push(match column {
                ResourceGenericColumn::Name => Arc::new(StringArray::from(
                    resources
                        .iter()
                        .map(|resource| resource.name.to_string())
                        .collect::<Vec<_>>(),
                )),
                ResourceGenericColumn::Type => Arc::new(StringArray::from(
                    resources
                        .iter()
                        .map(|resource| {
                            resource_type_name(&resource.schema)
                                .map(|type_name| type_name.to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(CLIError::critical)?,
                )),
                ResourceGenericColumn::Id => Arc::new(StringArray::from(
                    resources
                        .iter()
                        .map(|resource| resource.id.to_string())
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

        Ok(columns)
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
        let mut columns = self.generic_resource_arrays(resources)?;
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
        let extra_columns = self.selected_resource_columns();
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

/// What `kamu list` was asked to show.
///
/// Column shape follows the *arity* of the request, not its results: one type
/// keeps that type's own columns, more than one falls back to generic columns
/// plus `Type`. Letting the results decide would make the output schema depend
/// on the data, so the same command could emit different columns on different
/// days.
#[derive(Debug, Clone)]
pub enum ListResourcesScope {
    /// One type, optionally narrowed by a name pattern or exact ID.
    ByType(ResourceTypeDescriptor, Option<ResourceQuery>),
    /// Two or more named types, each with its own query.
    Types(Vec<(ResourceTypeDescriptor, Option<ResourceQuery>)>),
    /// Every type, optionally narrowed by one query applying to all.
    All(Option<ResourceQuery>),
}

impl ListResourcesScope {
    /// Whether the generic column set applies (`Type` column, no per-type
    /// columns).
    pub(crate) fn is_generic(&self) -> bool {
        !matches!(self, Self::ByType(..))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceGenericColumn {
    Name,
    Type,
    Id,
    Phase,
    Readiness,
    Updated,
    Description,
    Generation,
    ObservedGeneration,
    Age,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A selector for one type, carrying whatever narrows it.
///
/// `ResourceQuery`'s exact-name and exact-id list forms cannot appear here: the
/// CLI only reaches this path for `all`-style and pattern selectors, exact refs
/// having gone through the ref API instead.
fn selectors_for_type(
    schema: &kamu_resources::TypeUri,
    query: Option<&kamu_resources::ResourceQuery>,
) -> Vec<kamu_resources::ResourceSelector> {
    selectors_for_query(Some(schema), query)
}

/// The same, spanning every type.
fn any_type_selectors(
    query: Option<&kamu_resources::ResourceQuery>,
) -> Vec<kamu_resources::ResourceSelector> {
    selectors_for_query(None, query)
}

/// Fans a query out into scalar, ODF-shaped selectors — one per exact id or
/// name, since a selector carries a single `id` and a single `name` pattern.
/// The facade's coalescer folds them back into one repository row per type.
///
/// The match is exhaustive on purpose: a variant falling through would widen
/// the selector to the whole type rather than narrowing it.
fn selectors_for_query(
    schema: Option<&kamu_resources::TypeUri>,
    query: Option<&kamu_resources::ResourceQuery>,
) -> Vec<kamu_resources::ResourceSelector> {
    let base = || kamu_resources::ResourceSelector {
        r#type: schema.map(|schema| schema.clone().into()),
        ..Default::default()
    };

    let Some(query) = query else {
        return vec![base()];
    };

    match query {
        kamu_resources::ResourceQuery::NamePattern(pattern) => {
            vec![kamu_resources::ResourceSelector {
                name: Some(pattern.clone()),
                ..base()
            }]
        }
        kamu_resources::ResourceQuery::ExactIds(ids) => ids
            .iter()
            .map(|id| kamu_resources::ResourceSelector {
                id: Some(*id),
                ..base()
            })
            .collect(),
        // An exact name is a wildcard-free `LIKE` pattern: a selector's `name`
        // is a pattern by ODF definition, so the literal must be escaped rather
        // than passed through.
        kamu_resources::ResourceQuery::ExactNames(names) => names
            .iter()
            .map(|name| kamu_resources::ResourceSelector {
                name: Some(sql_like_escape_literal(name.as_str())),
                ..base()
            })
            .collect(),
    }
}

/// Escapes a literal so it matches only itself when used as a `LIKE` pattern.
fn sql_like_escape_literal(literal: &str) -> String {
    let mut escaped = String::with_capacity(literal.len());
    for ch in literal.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
