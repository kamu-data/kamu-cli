// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session};
use datafusion::common::{Constraints, Statistics};
use datafusion::config::TableOptions;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::context::DataFilePaths;
use datafusion::execution::options::ReadOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use futures::stream::{self, StreamExt, TryStreamExt};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Catalog
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct KamuCatalog {
    schema: Arc<KamuSchema>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl KamuCatalog {
    pub fn new(schema: Arc<KamuSchema>) -> Self {
        Self { schema }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Schema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Needs to be Clone only for non-async hacks below
#[derive(Clone)]
pub(crate) struct KamuSchema {
    inner: Arc<KamuSchemaImpl>,
}

struct KamuSchemaImpl {
    // Note: Never include `SessionContext` or `SessionState` into this structure.
    // Be mindful that this will cause a circular reference and thus prevent context
    // from ever being released.
    session_config: Arc<SessionConfig>,
    table_options: Arc<TableOptions>,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    options: QueryOptions,
    cache: Mutex<SchemaCache>,
}

#[derive(Default)]
struct SchemaCache {
    tables: Option<HashMap<String, Arc<KamuTable>>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl KamuSchema {
    pub fn new(
        session_context: &SessionContext,
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        options: QueryOptions,
    ) -> Self {
        Self {
            inner: Arc::new(KamuSchemaImpl {
                session_config: Arc::new(session_context.copied_config()),
                table_options: Arc::new(session_context.copied_table_options()),
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

        if let Some(aliases) = &self.inner.options.aliases {
            for (alias, id) in aliases {
                let hdl = self
                    .inner
                    .dataset_repo
                    .resolve_dataset_ref(&id.into())
                    .await
                    .int_err()?;

                self.inner
                    .dataset_action_authorizer
                    .check_action_allowed(&hdl, auth::DatasetAction::Read)
                    .await
                    .int_err()?;

                let dataset = self.inner.dataset_repo.get_dataset_by_handle(&hdl);

                let as_of = self
                    .inner
                    .options
                    .as_of_state
                    .as_ref()
                    .and_then(|s| s.inputs.get(id))
                    .cloned();

                let hints = self
                    .inner
                    .options
                    .hints
                    .as_ref()
                    .and_then(|h| h.get(id))
                    .cloned();

                tables.insert(
                    alias.clone(),
                    Arc::new(KamuTable::new(
                        self.inner.session_config.clone(),
                        self.inner.table_options.clone(),
                        hdl,
                        dataset,
                        as_of,
                        hints,
                    )),
                );
            }
        } else {
            // TODO: PERF: Scanning all datasets is not just super expensive - it may not be
            // possible at the public node scale. We need to patch DataFusion to support
            // unbounded catalogs.
            let mut dataset_handles = self.inner.dataset_repo.get_all_datasets();
            let inputs_state = self
                .inner
                .options
                .as_of_state
                .as_ref()
                .map(|s| &s.inputs)
                .cloned()
                .unwrap_or_default();

            while let Some(hdl) = dataset_handles.try_next().await.int_err()? {
                if self
                    .inner
                    .dataset_action_authorizer
                    .check_action_allowed(&hdl, auth::DatasetAction::Read)
                    .await
                    .is_ok()
                {
                    let dataset = self.inner.dataset_repo.get_dataset_by_handle(&hdl);

                    let as_of = inputs_state.get(&hdl.id).cloned();

                    let hints = self
                        .inner
                        .options
                        .hints
                        .as_ref()
                        .and_then(|h| h.get(&hdl.id))
                        .cloned();

                    tables.insert(
                        hdl.alias.to_string(),
                        Arc::new(KamuTable::new(
                            self.inner.session_config.clone(),
                            self.inner.table_options.clone(),
                            hdl,
                            dataset,
                            as_of,
                            hints,
                        )),
                    );
                }
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
        Ok(cache.tables.as_ref().unwrap().keys().cloned().collect())
    }

    async fn table_exist_impl(&self, name: &str) -> Result<bool, InternalError> {
        let cache = self.ensure_cache().await?;
        Ok(cache.tables.as_ref().unwrap().contains_key(name))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SchemaProvider for KamuSchema {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    // TODO: Datafusion should make this function async
    fn table_names(&self) -> Vec<String> {
        let this = self.clone();

        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

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
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            runtime.block_on(this.table_exist_impl(&name))
        })
        .join()
        .unwrap()
        .unwrap_or(false)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = {
            let cache = self
                .ensure_cache()
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;

            cache.tables.as_ref().unwrap().get(name).cloned()
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Table
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct KamuTable {
    session_config: Arc<SessionConfig>,
    table_options: Arc<TableOptions>,
    dataset_handle: DatasetHandle,
    dataset: Arc<dyn Dataset>,
    as_of: Option<Multihash>,
    hints: Option<DatasetQueryHints>,
    cache: Mutex<TableCache>,
}

#[derive(Default)]
struct TableCache {
    schema: Option<SchemaRef>,
    table: Option<Arc<dyn TableProvider>>,
}

impl KamuTable {
    pub(crate) fn new(
        session_config: Arc<SessionConfig>,
        table_options: Arc<TableOptions>,
        dataset_handle: DatasetHandle,
        dataset: Arc<dyn Dataset>,
        as_of: Option<Multihash>,
        hints: Option<DatasetQueryHints>,
    ) -> Self {
        Self {
            session_config,
            table_options,
            dataset_handle,
            dataset,
            as_of,
            hints,
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
        let files = self.collect_data_file_hashes(self.as_of.as_ref()).await?;

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
            schema: Some(&schema),
            // TODO: PERF: potential speedup if we specify `offset`?
            file_sort_order: Vec::new(),
            file_extension: "",
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
        };

        let table_paths = file_urls.to_urls().int_err()?;
        let listing_options =
            options.to_listing_options(&self.session_config, self.table_options.as_ref().clone());

        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(schema);

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

    async fn collect_data_file_hashes(
        &self,
        as_of: Option<&Multihash>,
    ) -> Result<Vec<Multihash>, InternalError> {
        let hash = if let Some(hash) = as_of {
            hash.clone()
        } else {
            self.dataset
                .as_metadata_chain()
                .resolve_ref(&BlockRef::Head)
                .await
                .int_err()?
        };

        tracing::debug!(?as_of, "Collecting data slices");

        let last_records_to_consider = self.hints.as_ref().and_then(|o| o.last_records_to_consider);

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
            .reduce_by_hash(
                &hash,
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

        tracing::debug!(num_slices = final_state.files.len(), "Slices collected");
        Ok(final_state.files)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        state: &dyn Session,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
