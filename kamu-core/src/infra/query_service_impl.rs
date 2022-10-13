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
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
    prelude::*,
};
use dill::*;
use futures::stream::TryStreamExt;
use opendatafabric::*;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::info_span;

use crate::domain::*;
use crate::infra::utils::datafusion_hacks::ListingTableOfFiles;
use crate::infra::*;

pub struct QueryServiceImpl {
    local_repo: Arc<dyn LocalDatasetRepository>,
    workspace_layout: Arc<WorkspaceLayout>,
}

#[component(pub)]
impl QueryServiceImpl {
    pub fn new(
        local_repo: Arc<dyn LocalDatasetRepository>,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Self {
        Self {
            local_repo,
            workspace_layout,
        }
    }
}

#[async_trait::async_trait]
impl QueryService for QueryServiceImpl {
    async fn tail(
        &self,
        dataset_ref: &DatasetRefLocal,
        num_records: u64,
    ) -> Result<Arc<dyn DataFrame>, QueryError> {
        let dataset_handle = self.local_repo.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let vocab: DatasetVocabulary = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<SetVocab>())
            .try_first()
            .await
            .int_err()?
            .map(|sv| sv.into())
            .unwrap_or_default();

        // TODO: This is a workaround for Arrow not supporting any operations on Decimals yet
        // See: https://github.com/apache/arrow-rs/issues/272
        let mut has_decimal = false;

        // TODO: This is a workaround for Arrow not handling timestamps with explicit timezones.
        // We basically have to re-cast all timestamp fields into timestamps after querying.
        // See:
        // - https://github.com/apache/arrow-datafusion/issues/959
        // - https://github.com/apache/arrow-rs/issues/393
        let schema = self.get_schema(&dataset_handle.as_local_ref()).await?;
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
                r#"SELECT {fields} FROM "{dataset}" ORDER BY {offset_col} DESC LIMIT {num_records}"#,
                fields = fields.join(", "),
                dataset = dataset_handle.name,
                offset_col = vocab.offset_column.unwrap_or("offset".to_owned()),
                num_records = num_records
            )
        } else {
            format!(
                r#"SELECT {fields} FROM "{dataset}" DESC LIMIT {num_records}"#,
                fields = fields.join(", "),
                dataset = dataset_handle.name,
                num_records = num_records
            )
        };

        self.sql_statement(
            &query,
            QueryOptions {
                datasets: vec![DatasetQueryOptions {
                    dataset_ref: dataset_handle.as_local_ref(),
                    limit: Some(num_records),
                }],
            },
        )
        .await
    }

    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<Arc<dyn DataFrame>, QueryError> {
        let span = info_span!("Executing SQL query", statement);
        let _span_guard = span.enter();

        let cfg = ExecutionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("kamu", "kamu");

        let mut ctx = ExecutionContext::with_config(cfg);

        ctx.register_catalog(
            "kamu",
            Arc::new(KamuCatalog::new(Arc::new(KamuSchema::new(
                self.local_repo.clone(),
                self.workspace_layout.clone(),
                options,
            )))),
        );

        Ok(ctx.sql(statement).await?)
    }

    async fn get_schema(&self, dataset_ref: &DatasetRefLocal) -> Result<Type, QueryError> {
        let dataset_handle = self.local_repo.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        // TODO: This service shouldn't know the specifics of dataset layouts
        let dataset_layout = self.workspace_layout.dataset_layout(&dataset_handle.name);

        let last_data_file = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.output_data)
            .map_ok(|slice| dataset_layout.data_slice_path(&slice))
            .try_first()
            .await
            .int_err()?
            .ok_or_else(|| {
                "Obtaining schema from datasets with no data is not yet supported".int_err()
            })?;

        let file = std::fs::File::open(&last_data_file).int_err()?;
        let reader = SerializedFileReader::new(file).int_err()?;
        Ok(reader.metadata().file_metadata().schema().clone())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct KamuCatalog {
    schema: Arc<KamuSchema>,
}

impl KamuCatalog {
    fn new(schema: Arc<KamuSchema>) -> Self {
        Self { schema }
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
            Some(self.schema.clone())
        } else {
            None
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Performance is poor as it essentially reads all data files in the workspace
// and in some cases (like 'show tables') even twice
#[derive(Clone)]
struct KamuSchema {
    local_repo: Arc<dyn LocalDatasetRepository>,
    workspace_layout: Arc<WorkspaceLayout>,
    options: QueryOptions,
}

impl KamuSchema {
    fn new(
        local_repo: Arc<dyn LocalDatasetRepository>,
        workspace_layout: Arc<WorkspaceLayout>,
        options: QueryOptions,
    ) -> Self {
        Self {
            local_repo,
            workspace_layout,
            options,
        }
    }

    async fn has_data(&self, dataset_handle: &DatasetHandle) -> Result<bool, InternalError> {
        let limit = self.options_for(dataset_handle).and_then(|o| o.limit);
        let files = self.collect_data_files(dataset_handle, limit).await?;

        if files.is_empty() {
            return Ok(false);
        }

        // TODO: Datafusion does not yet support nested types
        // See: https://github.com/apache/arrow-datafusion/issues/2326
        let nested = Self::is_nested(files.first().unwrap())?;
        Ok(!nested)
    }

    fn is_nested(file: &Path) -> Result<bool, InternalError> {
        let reader = SerializedFileReader::new(std::fs::File::open(file).int_err()?).int_err()?;
        let schema = reader.metadata().file_metadata().schema();
        Ok(schema.get_fields().iter().any(|f| f.is_group()))
    }

    async fn collect_data_files(
        &self,
        dataset_handle: &DatasetHandle,
        limit: Option<u64>,
    ) -> Result<Vec<PathBuf>, InternalError> {
        let dataset_layout = self.workspace_layout.dataset_layout(&dataset_handle.name);

        if let Ok(dataset) = self
            .local_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
        {
            let mut files = Vec::new();
            let mut num_records = 0;

            let mut slices = dataset
                .as_metadata_chain()
                .iter_blocks()
                .filter_data_stream_blocks()
                .filter_map_ok(|(_, b)| b.event.output_data);

            while let Some(slice) = slices.try_next().await.int_err()? {
                num_records += slice.interval.end - slice.interval.start + 1;
                files.push(dataset_layout.data_slice_path(&slice));
                if limit.is_some() && limit.unwrap() <= num_records as u64 {
                    break;
                }
            }

            Ok(files)
        } else {
            Ok(Vec::new())
        }
    }

    fn options_for(&self, dataset_handle: &DatasetHandle) -> Option<&DatasetQueryOptions> {
        for opt in &self.options.datasets {
            let same = match &opt.dataset_ref {
                DatasetRefLocal::ID(id) => *id == dataset_handle.id,
                DatasetRefLocal::Name(name) => *name == dataset_handle.name,
                DatasetRefLocal::Handle(h) => h.id == dataset_handle.id,
            };
            if same {
                return Some(opt);
            }
        }
        None
    }

    async fn table_names_impl(&self) -> Vec<String> {
        if self.options.datasets.is_empty() {
            let mut res = Vec::new();
            let mut dataset_handles = self.local_repo.get_all_datasets();

            while let Some(hdl) = dataset_handles.try_next().await.unwrap() {
                if self.has_data(&hdl).await.unwrap() {
                    res.push(hdl.name.to_string())
                }
            }

            res
        } else {
            self.options
                .datasets
                .iter()
                .map(|d| d.dataset_ref.to_string())
                .collect()
        }
    }

    async fn table_exist_impl(&self, name: &str) -> bool {
        let dataset_name = match DatasetName::try_from(name) {
            Ok(name) => name,
            Err(_) => return false,
        };

        self.local_repo
            .try_resolve_dataset_ref(&dataset_name.into())
            .await
            .unwrap()
            .is_some()
    }

    async fn table_impl(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let dataset_resolution = self
            .local_repo
            .resolve_dataset_ref(&DatasetName::try_from(name).unwrap().into())
            .await;

        match dataset_resolution {
            Err(_) => None,
            Ok(dataset_handle) => {
                let limit = self.options_for(&dataset_handle).and_then(|o| o.limit);
                let files = self
                    .collect_data_files(&dataset_handle, limit)
                    .await
                    .unwrap();

                if files.is_empty() {
                    None
                } else {
                    // TODO: Datafusion made it difficult to lazily create table providers,
                    // so we have to resort to spawning a thread to call an async function
                    // See: https://github.com/apache/arrow-datafusion/issues/1792
                    let table = ListingTableOfFiles::new_with_defaults(
                        files
                            .into_iter()
                            .map(|p| p.to_string_lossy().into())
                            .collect(),
                    )
                    .await
                    .unwrap();

                    Some(Arc::new(table))
                }
            }
        }
    }
}

impl SchemaProvider for KamuSchema {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    // TODO: Datafusion should make this function async
    fn table_names(&self) -> Vec<String> {
        let this = self.clone();

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(this.table_names_impl())
        })
        .join()
        .unwrap()
    }

    fn table_exist(&self, name: &str) -> bool {
        let this = self.clone();
        let name = name.to_owned();

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(this.table_exist_impl(&name))
        })
        .join()
        .unwrap()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let this = self.clone();
        let name = name.to_owned();

        // TODO: Datafusion made it difficult to lazily create table providers,
        // so we have to resort to spawning a thread to call an async function
        // See: https://github.com/apache/arrow-datafusion/issues/1792
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(this.table_impl(&name))
        })
        .join()
        .unwrap()
    }
}
