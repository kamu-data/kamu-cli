// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::parquet::{
    basic::LogicalType,
    file::reader::{FileReader, SerializedFileReader},
    schema::types::Type,
};
use datafusion::{
    arrow::datatypes::Schema,
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::{
        datasource::{Statistics, TableProviderFilterPushDown},
        TableProvider, TableType,
    },
    error::DataFusionError,
    logical_plan::combine_filters,
    physical_plan::{parquet::ParquetExec, ExecutionPlan},
    prelude::*,
};
use dill::*;
use opendatafabric::DatasetID;
use std::{path::PathBuf, sync::Arc};
use tracing::info_span;

use crate::domain::{
    DatasetQueryOptions, MetadataRepository, QueryError, QueryOptions, QueryService,
};

use super::{DatasetLayout, VolumeLayout};

pub struct QueryServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    volume_layout: Arc<VolumeLayout>,
}

#[component(pub)]
impl QueryServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        volume_layout: Arc<VolumeLayout>,
    ) -> Self {
        Self {
            metadata_repo,
            volume_layout,
        }
    }

    // Unfortunately there are some deficiencies in datafusion/arrow that we have to work around in this nasty way
    fn catch_panic<F: FnOnce() -> R + std::panic::UnwindSafe, R>(
        f: F,
    ) -> Result<R, Box<dyn std::any::Any + Send + 'static>> {
        let old_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| ()));

        // TODO: This is necessary because datafusion currently panics on schemas with nesting
        let res = std::panic::catch_unwind(f);

        std::panic::set_hook(old_hook);
        res
    }
}

impl QueryService for QueryServiceImpl {
    fn tail(
        &self,
        dataset_id: &DatasetID,
        num_records: u64,
    ) -> Result<Arc<dyn DataFrame>, QueryError> {
        let vocab = self
            .metadata_repo
            .get_metadata_chain(dataset_id)?
            .iter_blocks()
            .filter_map(|b| b.vocab)
            .next()
            .unwrap_or_default();

        // TODO: This is a workaround for Arrow not supporting any operations on Decimals yet
        // See: https://github.com/apache/arrow-rs/issues/272
        let mut has_decimal = false;

        // TODO: This is a workaround for Arrow not handling timestamps with explicit timezones.
        // We basically have to re-cast all timestamp fields into timestamps after querying.
        // See:
        // - https://github.com/apache/arrow-datafusion/issues/959
        // - https://github.com/apache/arrow-rs/issues/393
        let schema = self.get_schema(dataset_id)?;
        let fields: Vec<String> = match schema {
            Type::GroupType { fields, .. } => fields
                .iter()
                .map(|f| match f.as_ref() {
                    pt @ Type::PrimitiveType { .. } => {
                        if let Some(LogicalType::TIMESTAMP(ts)) = pt.get_basic_info().logical_type()
                        {
                            if ts.is_adjusted_to_u_t_c {
                                return format!(
                                    "CAST(\"{name}\" as TIMESTAMP) as \"{name}\"",
                                    name = pt.get_basic_info().name()
                                );
                            }
                        } else if pt.get_precision() > 0 {
                            has_decimal = true;
                        }
                        format!("\"{}\"", pt.get_basic_info().name())
                    }
                    Type::GroupType { basic_info, .. } => format!("\"{}\"", basic_info.name()),
                })
                .collect(),
            Type::PrimitiveType { .. } => unreachable!(),
        };

        let query = if !has_decimal {
            format!(
                r#"SELECT {fields} FROM "{dataset}" ORDER BY {event_time_col} DESC LIMIT {num_records}"#,
                fields = fields.join(", "),
                dataset = dataset_id,
                event_time_col = vocab.event_time_column.unwrap_or("event_time".to_owned()),
                num_records = num_records
            )
        } else {
            format!(
                r#"SELECT {fields} FROM "{dataset}" DESC LIMIT {num_records}"#,
                fields = fields.join(", "),
                dataset = dataset_id,
                num_records = num_records
            )
        };

        self.sql_statement(
            &query,
            QueryOptions {
                datasets: vec![DatasetQueryOptions {
                    dataset_id: dataset_id.to_owned(),
                    limit: Some(num_records),
                }],
            },
        )
    }

    fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<Arc<dyn DataFrame>, QueryError> {
        let span = info_span!("Executing SQL query", statement = statement);
        let _span_guard = span.enter();

        let cfg = ExecutionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("kamu", "kamu");

        let mut ctx = ExecutionContext::with_config(cfg);

        ctx.register_catalog(
            "kamu",
            Arc::new(KamuCatalog::new(
                self.metadata_repo.clone(),
                self.volume_layout.clone(),
                options,
            )),
        );

        Ok(ctx.sql(statement)?)
    }

    fn get_schema(&self, dataset_id: &DatasetID) -> Result<Type, QueryError> {
        let metadata_chain = self.metadata_repo.get_metadata_chain(dataset_id)?;
        let dataset_layout = DatasetLayout::new(&self.volume_layout, dataset_id);

        let last_data_file = metadata_chain
            .iter_blocks()
            .filter(|b| b.output_slice.is_some())
            .map(|b| dataset_layout.data_dir.join(b.block_hash.to_string()))
            .next()
            .expect("Obtaining schema from datasets with no data is not yet supported");

        let file = std::fs::File::open(&last_data_file)?;
        let reader = SerializedFileReader::new(file).map_err(|e| QueryError::internal(e))?;
        Ok(reader.metadata().file_metadata().schema().clone())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct KamuCatalog {
    metadata_repo: Arc<dyn MetadataRepository>,
    volume_layout: Arc<VolumeLayout>,
    options: QueryOptions,
}

impl KamuCatalog {
    fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        volume_layout: Arc<VolumeLayout>,
        options: QueryOptions,
    ) -> Self {
        Self {
            metadata_repo,
            volume_layout,
            options,
        }
    }
}

impl CatalogProvider for KamuCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["kamu".to_owned()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn datafusion::catalog::schema::SchemaProvider>> {
        if name == "kamu" {
            Some(Arc::new(KamuSchema::new(
                self.metadata_repo.clone(),
                self.volume_layout.clone(),
                self.options.clone(),
            )))
        } else {
            None
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Performance is poor as it essentially reads all data files in the workspace
// and in some cases (like 'show tables') even twice
struct KamuSchema {
    metadata_repo: Arc<dyn MetadataRepository>,
    volume_layout: Arc<VolumeLayout>,
    options: QueryOptions,
}

impl KamuSchema {
    fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        volume_layout: Arc<VolumeLayout>,
        options: QueryOptions,
    ) -> Self {
        Self {
            metadata_repo,
            volume_layout,
            options,
        }
    }

    fn has_data(&self, dataset_id: &DatasetID) -> bool {
        let limit = self.options_for(dataset_id).and_then(|o| o.limit);
        let files = self.collect_data_files(dataset_id, limit);
        if files.is_empty() {
            return false;
        }

        // TODO: DataFusion currently panics on schemas with nesting
        QueryServiceImpl::catch_panic(|| DatasetTable::try_new(files).is_ok())
            .ok()
            .unwrap_or(false)
    }

    fn collect_data_files(&self, dataset_id: &DatasetID, limit: Option<u64>) -> Vec<PathBuf> {
        let dataset_layout = DatasetLayout::new(&self.volume_layout, dataset_id);

        if let Ok(metadata_chain) = self.metadata_repo.get_metadata_chain(dataset_id) {
            let mut files = Vec::new();
            let mut num_records = 0;

            for block in metadata_chain
                .iter_blocks()
                .filter(|b| b.output_slice.is_some())
            {
                num_records += block.output_slice.unwrap().num_records;
                files.push(dataset_layout.data_dir.join(block.block_hash.to_string()));
                if limit.is_some() && limit.unwrap() <= num_records as u64 {
                    break;
                }
            }

            files
        } else {
            Vec::new()
        }
    }

    fn options_for(&self, dataset_id: &DatasetID) -> Option<&DatasetQueryOptions> {
        for opt in &self.options.datasets {
            if &opt.dataset_id == dataset_id {
                return Some(opt);
            }
        }
        None
    }
}

impl SchemaProvider for KamuSchema {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        if self.options.datasets.is_empty() {
            self.metadata_repo
                .get_all_datasets()
                .filter(|id| self.has_data(id))
                .map(|id| id.into())
                .collect()
        } else {
            self.options
                .datasets
                .iter()
                .map(|d| d.dataset_id.to_string())
                .collect()
        }
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let dataset_id = DatasetID::try_from(name).unwrap();
        let limit = self.options_for(dataset_id).and_then(|o| o.limit);
        let files = self.collect_data_files(dataset_id, limit);

        if files.is_empty() {
            None
        } else {
            // TODO: Have to unwrap as no way to return error, should we read schema lazily?
            Some(Arc::new(DatasetTable::try_new(files).unwrap()))
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// Based heavily on datafusion::ParquetTable
// TODO: Migrate to ParquetTable once it supports explicit filenames
struct DatasetTable {
    files: Vec<PathBuf>,
    schema: Arc<Schema>,
    statistics: Statistics,
    max_concurrency: usize,
    enable_pruning: bool,
}

impl DatasetTable {
    fn try_new(files: Vec<PathBuf>) -> Result<Self, DataFusionError> {
        let max_concurrency = 1;
        let files_ref: Vec<&str> = files.iter().map(|p| p.to_str().unwrap()).collect();
        let parquet_exec =
            ParquetExec::try_from_files(&files_ref, None, None, 0, max_concurrency, None)?;
        let schema = parquet_exec.schema();
        let statistics = parquet_exec.statistics().clone();
        Ok(Self {
            files,
            schema,
            statistics,
            max_concurrency,
            enable_pruning: true,
        })
    }
}

impl TableProvider for DatasetTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[datafusion::logical_plan::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        let files_ref: Vec<&str> = self.files.iter().map(|s| s.to_str().unwrap()).collect();

        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        let predicate = if self.enable_pruning {
            combine_filters(filters)
        } else {
            None
        };
        Ok(Arc::new(ParquetExec::try_from_files(
            &files_ref,
            projection.clone(),
            predicate,
            limit
                .map(|l| std::cmp::min(l, batch_size))
                .unwrap_or(batch_size),
            self.max_concurrency,
            limit,
        )?))
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn has_exact_statistics(&self) -> bool {
        true
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &datafusion::logical_plan::Expr,
    ) -> datafusion::error::Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}
