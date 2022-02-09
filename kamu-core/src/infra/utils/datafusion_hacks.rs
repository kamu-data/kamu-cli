// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    datasource::{
        datasource::TableProviderFilterPushDown,
        get_statistics_with_limit,
        listing::ListingOptions,
        object_store::{FileMeta, ObjectStore, SizedFile},
        PartitionedFile, TableProvider,
    },
    error::{DataFusionError, Result},
    logical_plan::Expr,
    physical_plan::{empty::EmptyExec, file_format::PhysicalPlanConfig, ExecutionPlan, Statistics},
};
use futures::StreamExt;

///////////////////////////////////////////////////////////////////////////////
// TODO: Ability to create a table from a list of parquet files
// should in the future be supported directly by datafusion
// See: https://github.com/apache/arrow-datafusion/issues/1384
///////////////////////////////////////////////////////////////////////////////

pub struct ListingTableOfFiles {
    object_store: Arc<dyn ObjectStore>,
    files: Vec<String>,
    /// File fields only
    file_schema: SchemaRef,
    /// File fields + partition columns
    table_schema: SchemaRef,
    options: ListingOptions,
}

impl ListingTableOfFiles {
    /// Create new table that lists the FS to get the files to scan.
    /// The provided `schema` must be resolved before creating the table
    /// and should contain the fields of the file without the table
    /// partitioning columns.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        files: Vec<String>,
        file_schema: SchemaRef,
        options: ListingOptions,
    ) -> Self {
        // Add the partition columns to the file schema
        let mut table_fields = file_schema.fields().clone();
        for part in &options.table_partition_cols {
            table_fields.push(Field::new(
                part,
                datafusion::physical_plan::file_format::DEFAULT_PARTITION_COLUMN_DATATYPE.clone(),
                false,
            ));
        }

        Self {
            object_store,
            files,
            file_schema,
            table_schema: Arc::new(Schema::new(table_fields)),
            options,
        }
    }

    pub async fn new_with_defaults(files: Vec<String>) -> Result<Self> {
        let (target_partitions, enable_pruning) = (1, true);

        let file_format = Arc::new(
            datafusion::datasource::file_format::parquet::ParquetFormat::default()
                .with_enable_pruning(enable_pruning),
        );

        let listing_options = datafusion::datasource::listing::ListingOptions {
            format: file_format.clone(),
            collect_stat: true,
            file_extension: "".to_owned(),
            target_partitions,
            table_partition_cols: vec![],
        };

        let object_store: Arc<dyn datafusion::datasource::object_store::ObjectStore> =
            Arc::new(datafusion::datasource::object_store::local::LocalFileSystem);

        let resolved_schema = listing_options
            .infer_schema(object_store.clone(), files.first().unwrap())
            .await?;

        Ok(ListingTableOfFiles::new(
            object_store,
            files,
            resolved_schema,
            listing_options,
        ))
    }

    // /// Get object store ref
    // pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
    //     &self.object_store
    // }
    // /// Get path ref
    // pub fn table_path(&self) -> &str {
    //     &self.table_path
    // }
    // /// Get options ref
    // pub fn options(&self) -> &ListingOptions {
    //     &self.options
    // }
}

#[async_trait]
impl TableProvider for ListingTableOfFiles {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (partitioned_file_lists, statistics) = self.list_files_for_scan(filters, limit).await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let schema = self.schema();
            let projected_schema = match &projection {
                None => schema,
                Some(p) => Arc::new(Schema::new(
                    p.iter().map(|i| schema.field(*i).clone()).collect(),
                )),
            };
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        // create the execution plan
        self.options
            .format
            .create_physical_plan(
                PhysicalPlanConfig {
                    object_store: Arc::clone(&self.object_store),
                    file_schema: Arc::clone(&self.file_schema),
                    file_groups: partitioned_file_lists,
                    statistics,
                    projection: projection.clone(),
                    batch_size,
                    limit,
                    table_partition_cols: self.options.table_partition_cols.clone(),
                },
                filters,
            )
            .await
    }

    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        // NOTE: We don't have access to datafusion::datasource::listing::helpers, so have to remote pushdown
        Ok(TableProviderFilterPushDown::Inexact)
        // if expr_applicable_for_cols(&self.options.table_partition_cols, filter) {
        //     // if filter can be handled by partiton pruning, it is exact
        //     Ok(TableProviderFilterPushDown::Exact)
        // } else {
        //     // otherwise, we still might be able to handle the filter with file
        //     // level mechanisms such as Parquet row group pruning.
        //     Ok(TableProviderFilterPushDown::Inexact)
        // }
    }
}

impl ListingTableOfFiles {
    /// Get the list of files for a scan as well as the file level statistics.
    /// The list is grouped to let the execution plan know how the files should
    /// be distributed to different threads / executors.
    async fn list_files_for_scan<'a>(
        &'a self,
        _filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        // NOTE: We don't have access to datafusion::datasource::listing::helpers, so have to remote parititioning

        // // list files (with partitions)
        // let file_list = pruned_partition_list(
        //     self.object_store.as_ref(),
        //     &self.table_path,
        //     filters,
        //     &self.options.file_extension,
        //     &self.options.table_partition_cols,
        // )
        // .await?;

        fn get_meta(path: String, metadata: std::fs::Metadata) -> FileMeta {
            FileMeta {
                sized_file: SizedFile {
                    path,
                    size: metadata.len(),
                },
                last_modified: metadata.modified().map(chrono::DateTime::from).ok(),
            }
        }

        let partitionef_files: Vec<_> = self
            .files
            .iter()
            .map(|path| {
                std::fs::metadata(&path)
                    .map_err(|e| Into::<DataFusionError>::into(e))
                    .map(|meta| get_meta(path.clone(), meta))
            })
            .map(|meta| {
                Ok(PartitionedFile {
                    partition_values: vec![],
                    file_meta: meta?,
                })
            })
            .collect::<Result<_>>()?;

        let file_list = futures::stream::iter(partitionef_files);

        ////////

        // collect the statistics if required by the config
        let object_store = Arc::clone(&self.object_store);
        let files = file_list.then(move |part_file| {
            let object_store = object_store.clone();
            async move {
                // let part_file = part_file?;
                let statistics = if self.options.collect_stat {
                    let object_reader =
                        object_store.file_reader(part_file.file_meta.sized_file.clone())?;
                    self.options.format.infer_stats(object_reader).await?
                } else {
                    Statistics::default()
                };
                Ok((part_file, statistics)) as Result<(PartitionedFile, Statistics)>
            }
        });

        let (files, statistics) = get_statistics_with_limit(files, self.schema(), limit).await?;

        Ok((
            Self::split_files(files, self.options.target_partitions),
            statistics,
        ))
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
