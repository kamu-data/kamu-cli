// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use futures::StreamExt;

///////////////////////////////////////////////////////////////////////////////
// TODO: Ability to create a table from a list of parquet files
// should in the future be supported directly by datafusion
// See: https://github.com/apache/arrow-datafusion/issues/1384
///////////////////////////////////////////////////////////////////////////////

pub struct ListingTableOfFiles {
    object_store_url: url::Url,
    object_store: Arc<dyn object_store::ObjectStore>,
    format: Arc<ParquetFormat>,
    files: Vec<object_store::path::Path>,
    /// File fields only
    file_schema: SchemaRef,
    /// File fields + partition columns
    table_schema: SchemaRef,
}

impl ListingTableOfFiles {
    pub async fn try_new(
        object_store_url: url::Url,
        object_store: Arc<dyn object_store::ObjectStore>,
        ctx: &SessionState,
        files: Vec<object_store::path::Path>,
    ) -> Result<Self> {
        let format = Arc::new(ParquetFormat::new());

        // Infer schema
        let file_path = files.first().unwrap();
        let file_meta = object_store.head(&file_path).await?;

        let file_schema = format
            .infer_schema(ctx, &object_store, &[file_meta])
            .await?;

        Ok(Self {
            object_store_url,
            object_store,
            format,
            files,
            table_schema: Arc::new(Schema::new(file_schema.fields().clone())),
            file_schema,
        })
    }
}

#[async_trait]
impl TableProvider for ListingTableOfFiles {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (partitioned_file_lists, statistics) =
            self.list_files_for_scan(ctx, filters, limit).await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let schema = self.schema();
            let projected_schema = datafusion::physical_plan::project_schema(&schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        // create an execution plan
        self.format
            .create_physical_plan(
                ctx,
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::parse(self.object_store_url.as_str())
                        .unwrap(),
                    file_schema: Arc::clone(&self.file_schema),
                    file_groups: partitioned_file_lists,
                    statistics,
                    projection: projection.cloned(),
                    limit,
                    output_ordering: self.try_create_output_ordering()?,
                    table_partition_cols: Vec::new(),
                    infinite_source: false,
                },
                None,
            )
            .await
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        // NOTE: We don't have access to datafusion::datasource::listing::helpers, so
        // have to remove pushdown
        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }
}

impl ListingTableOfFiles {
    /// Get the list of files for a scan as well as the file level statistics.
    /// The list is grouped to let the execution plan know how the files should
    /// be distributed to different threads / executors.
    async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a SessionState,
        _filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        // NOTE: We don't have access to datafusion::datasource::listing::helpers, so
        // have to remove parititioning
        let mut file_list = Vec::new();

        for file_path in &self.files {
            let object_meta = self.object_store.head(file_path).await?;
            file_list.push(PartitionedFile {
                object_meta,
                partition_values: Vec::new(),
                range: None,
                extensions: None,
            })
        }

        let files = futures::stream::iter(file_list);

        let (files, statistics) = datafusion::datasource::get_statistics_with_limit(
            files.map(|f| Ok((f, Statistics::default()))),
            self.schema(),
            limit,
        )
        .await?;

        Ok((
            Self::split_files(files, ctx.config().target_partitions()),
            statistics,
        ))
    }

    fn try_create_output_ordering(&self) -> Result<Option<Vec<PhysicalSortExpr>>> {
        Ok(None)
    }

    // NOTE: inlined from datafusion::datasource::listing::helpers
    fn split_files(partitioned_files: Vec<PartitionedFile>, n: usize) -> Vec<Vec<PartitionedFile>> {
        if partitioned_files.is_empty() {
            return vec![];
        }
        // effectively this is div with rounding up instead of truncating
        let chunk_size = (partitioned_files.len() + n - 1) / n;
        partitioned_files
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect()
    }
}
