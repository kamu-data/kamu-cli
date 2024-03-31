// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::common::{Constraints, Statistics};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{DataFilePaths, SessionState};
use datafusion::execution::options::ReadOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use futures::stream::{self, StreamExt, TryStreamExt};
use kamu_core::*;
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////
// Catalog
/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct KamuCatalog {
    schema: Arc<KamuSchema>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl KamuCatalog {
    pub fn new(schema: Arc<KamuSchema>) -> Self {
        Self { schema }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl CatalogProvider for KamuCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["kamu".to_owned()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "kamu" {
            Some(self.schema.clone())
        } else {
            None
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Schema
/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Needs to be Clone only for non-async hacks below
#[derive(Clone)]
pub(crate) struct KamuSchema {
    inner: Arc<KamuSchemaImpl>,
}

struct KamuSchemaImpl {
    session_context: SessionContext,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    options: QueryOptions,
    cache: Mutex<SchemaCache>,
}

#[derive(Default)]
struct SchemaCache {
    tables: Option<HashMap<DatasetAlias, Arc<KamuTable>>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl KamuSchema {
    pub fn new(
        session_context: SessionContext,
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        options: QueryOptions,
    ) -> Self {
        Self {
            inner: Arc::new(KamuSchemaImpl {
                session_context,
                dataset_repo,
                dataset_action_authorizer,
                options,
                cache: Mutex::new(SchemaCache::default()),
            }),
        }
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn init_schema_cache(&self) -> Result<SchemaCache, InternalError> {
        let mut tables = HashMap::new();

        if self.inner.options.datasets.is_empty() {
            let mut dataset_handles = self.inner.dataset_repo.get_all_datasets();

            while let Some(hdl) = dataset_handles.try_next().await.int_err()? {
                if self
                    .inner
                    .dataset_action_authorizer
                    .check_action_allowed(&hdl, auth::DatasetAction::Read)
                    .await
                    .is_ok()
                {
                    let dataset = self
                        .inner
                        .dataset_repo
                        .get_dataset(&hdl.as_local_ref())
                        .await
                        .unwrap();

                    tables.insert(
                        hdl.alias.clone(),
                        Arc::new(KamuTable::new(
                            self.inner.session_context.clone(),
                            hdl,
                            dataset,
                            None,
                        )),
                    );
                }
            }
        } else {
            for options in &self.inner.options.datasets {
                let hdl = self
                    .inner
                    .dataset_repo
                    .resolve_dataset_ref(&options.dataset_ref)
                    .await
                    .int_err()?;

                let dataset = self
                    .inner
                    .dataset_repo
                    .get_dataset(&hdl.as_local_ref())
                    .await
                    .unwrap();

                tables.insert(
                    hdl.alias.clone(),
                    Arc::new(KamuTable::new(
                        self.inner.session_context.clone(),
                        hdl,
                        dataset,
                        Some(options.clone()),
                    )),
                );
            }
        }

        Ok(SchemaCache {
            tables: Some(tables),
        })
    }

    async fn ensure_cache(&self) -> Result<std::sync::MutexGuard<SchemaCache>, InternalError> {
        {
            let cache = self.inner.cache.lock().unwrap();
            if cache.tables.is_some() {
                return Ok(cache);
            }
        }

        let new_cache = self.init_schema_cache().await?;

        {
            let mut cache = self.inner.cache.lock().unwrap();
            *cache = new_cache;
            Ok(cache)
        }
    }

    async fn table_names_impl(&self) -> Result<Vec<String>, InternalError> {
        let cache = self.ensure_cache().await?;
        Ok(cache
            .tables
            .as_ref()
            .unwrap()
            .keys()
            .map(DatasetAlias::to_string)
            .collect())
    }

    async fn table_exist_impl(&self, name: &str) -> Result<bool, InternalError> {
        let alias = DatasetAlias::from_str(name).int_err()?;
        let cache = self.ensure_cache().await?;
        Ok(cache.tables.as_ref().unwrap().contains_key(&alias))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
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
        .unwrap_or(false)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let Ok(alias) = DatasetAlias::try_from(name) else {
            return Ok(None);
        };

        let table = {
            let cache = self
                .ensure_cache()
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;

            cache.tables.as_ref().unwrap().get(&alias).cloned()
        };

        if let Some(table) = table {
            // HACK: We pre-initialize the schema here because `TableProvider::schema()` is
            // not async
            table
                .get_table_schema()
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;

            Ok(Some(table))
        } else {
            Ok(None)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Table
/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct KamuTable {
    session_context: SessionContext,
    dataset_handle: DatasetHandle,
    dataset: Arc<dyn Dataset>,
    options: Option<DatasetQueryOptions>,
    cache: Mutex<TableCache>,
}

#[derive(Default)]
struct TableCache {
    schema: Option<SchemaRef>,
    table: Option<Arc<dyn TableProvider>>,
}

impl KamuTable {
    pub(crate) fn new(
        session_context: SessionContext,
        dataset_handle: DatasetHandle,
        dataset: Arc<dyn Dataset>,
        options: Option<DatasetQueryOptions>,
    ) -> Self {
        Self {
            session_context,
            dataset_handle,
            dataset,
            options,
            cache: Mutex::new(TableCache::default()),
        }
    }

    #[tracing::instrument(level="info", skip_all, fields(dataset_handle = ?self.dataset_handle))]
    async fn init_table_schema(&self) -> Result<SchemaRef, InternalError> {
        let maybe_set_data_schema = self
            .dataset
            .as_metadata_chain()
            .accept_one(SearchSetDataSchemaVisitor::new())
            .await
            .int_err()?
            .into_event();

        if let Some(set_data_schema) = maybe_set_data_schema {
            set_data_schema.schema_as_arrow().int_err()
        } else {
            Ok(Arc::new(Schema::empty()))
        }
    }

    async fn get_table_schema(&self) -> Result<SchemaRef, InternalError> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(schema) = &cache.schema {
                return Ok(Arc::clone(schema));
            }
        }

        let schema = self.init_table_schema().await?;

        {
            let mut cache = self.cache.lock().unwrap();
            cache.schema = Some(Arc::clone(&schema));
            Ok(schema)
        }
    }

    // TODO: A lot of duplication from `SessionContext::read_parquet` - code is
    // copied as we need table provider and not the `DataFrame`
    #[tracing::instrument(level="info", skip_all, fields(dataset_handle = ?self.dataset_handle))]
    async fn init_table_provider(
        &self,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableProvider>, InternalError> {
        let files = self.collect_data_file_hashes().await?;

        if files.is_empty() {
            return Ok(Arc::new(EmptyTable::new(schema)));
        }

        let object_repo = self.dataset.as_data_repo();
        let file_urls: Vec<String> = stream::iter(files)
            .then(|h| async move { object_repo.get_internal_url(&h).await })
            .map(Into::into)
            .collect()
            .await;

        let options = ParquetReadOptions {
            schema: None,
            // TODO: PERF: potential speedup if we specify `offset`?
            file_sort_order: Vec::new(),
            file_extension: "",
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
        };

        let table_paths = file_urls.to_urls().int_err()?;
        let session_config = self.session_context.copied_config();
        let listing_options = options
            .to_listing_options(&session_config, self.session_context.copied_table_options());

        let resolved_schema = options
            .get_resolved_schema(
                &session_config,
                self.session_context.state(),
                table_paths[0].clone(),
            )
            .await
            .int_err()?;

        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let provider = ListingTable::try_new(config).int_err()?;

        Ok(Arc::new(provider))
    }

    async fn get_table_provider(&self) -> Result<Arc<dyn TableProvider>, InternalError> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(table) = &cache.table {
                return Ok(Arc::clone(table));
            }
        }

        let schema = self.get_table_schema().await?;
        let table = self.init_table_provider(schema).await?;

        {
            let mut cache = self.cache.lock().unwrap();
            cache.table = Some(Arc::clone(&table));
            Ok(table)
        }
    }

    async fn collect_data_file_hashes(&self) -> Result<Vec<Multihash>, InternalError> {
        let last_records_to_consider = self
            .options
            .as_ref()
            .and_then(|o| o.last_records_to_consider);

        type Flag = MetadataEventTypeFlags;
        type Decision = MetadataVisitorDecision;

        struct DataSliceCollectorVisitorState {
            files: Vec<Multihash>,
            num_records: u64,
            last_records_to_consider: Option<u64>,
        }

        let final_state = self
            .dataset
            .as_metadata_chain()
            .reduce(
                DataSliceCollectorVisitorState {
                    files: Vec::new(),
                    num_records: 0,
                    last_records_to_consider,
                },
                Decision::NextOfType(Flag::DATA_BLOCK),
                |state, _hash, block| {
                    let new_data = match &block.event {
                        MetadataEvent::AddData(e) => e.new_data.as_ref(),
                        MetadataEvent::ExecuteTransform(e) => e.new_data.as_ref(),
                        _ => unreachable!(),
                    };
                    let Some(slice) = new_data else {
                        return Decision::NextOfType(Flag::DATA_BLOCK);
                    };

                    state.num_records += slice.num_records();
                    state.files.push(slice.physical_hash.clone());

                    if let Some(last_records_to_consider) = &state.last_records_to_consider
                        && *last_records_to_consider <= state.num_records
                    {
                        return Decision::Stop;
                    }

                    Decision::NextOfType(Flag::DATA_BLOCK)
                },
            )
            .await
            .int_err()?;

        Ok(final_state.files)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TableProvider for KamuTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // SAFETY: We rely on `KamuScheme` to pre-initialize schemas in async context
        // before handing down references to `KamuTable`
        self.cache.lock().unwrap().schema.clone().unwrap()
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let p = self
            .get_table_provider()
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        p.scan(state, projection, filters, limit).await
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
