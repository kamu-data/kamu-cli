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
use slog::{info, Logger};
use std::{path::PathBuf, sync::Arc};

use crate::domain::{
    DatasetQueryOptions, MetadataRepository, QueryError, QueryOptions, QueryService,
};

use super::{DatasetLayout, VolumeLayout};

pub struct QueryServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    volume_layout: Arc<VolumeLayout>,
    logger: Logger,
}

#[component(pub)]
impl QueryServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        volume_layout: Arc<VolumeLayout>,
        logger: Logger,
    ) -> Self {
        Self {
            metadata_repo,
            volume_layout,
            logger,
        }
    }
}

impl QueryService for QueryServiceImpl {
    fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<Arc<dyn DataFrame>, QueryError> {
        info!(self.logger, "Executing SQL query"; "statement" => statement);

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

        let old_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| ()));

        // TODO: This is necessary because datafusion currently panics on schemas with nesting
        let ok = std::panic::catch_unwind(|| DatasetTable::try_new(files).is_ok())
            .ok()
            .unwrap_or(false);

        std::panic::set_hook(old_hook);
        ok
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
