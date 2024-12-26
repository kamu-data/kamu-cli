// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use auth::DatasetAction;
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
use futures::stream::{self, StreamExt};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::auth::DatasetActionAuthorizerExt;
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

impl std::fmt::Debug for KamuCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KamuCatalog").finish_non_exhaustive()
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
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    options: QueryOptions,
    tables: Mutex<HashMap<String, Arc<KamuTable>>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl KamuSchema {
    pub async fn prepare(
        session_context: &SessionContext,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        options: QueryOptions,
    ) -> Result<Self, InternalError> {
        let schema = Self {
            inner: Arc::new(KamuSchemaImpl {
                session_config: Arc::new(session_context.copied_config()),
                table_options: Arc::new(session_context.copied_table_options()),
                dataset_registry,
                dataset_action_authorizer,
                options,
                tables: Mutex::new(HashMap::new()),
            }),
        };

        schema.init_schema_cache().await?;

        Ok(schema)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn init_schema_cache(&self) -> Result<(), InternalError> {
        let mut tables = HashMap::new();

        let name_resolution_enabled = self.inner.options.input_datasets.is_empty();

        if !name_resolution_enabled {
            for (id, opts) in &self.inner.options.input_datasets {
                let hdl = self
                    .inner
                    .dataset_registry
                    .resolve_dataset_handle_by_ref(&id.as_local_ref())
                    .await
                    .int_err()?;

                if !self
                    .inner
                    .dataset_action_authorizer
                    .is_action_allowed(&hdl, auth::DatasetAction::Read)
                    .await?
                {
                    // Ignore this alias and let the query fail with "not found" error
                    continue;
                }

                let resolved_dataset = self.inner.dataset_registry.get_dataset_by_handle(&hdl);

                tables.insert(
                    opts.alias.clone(),
                    Arc::new(KamuTable::new(
                        self.inner.session_config.clone(),
                        self.inner.table_options.clone(),
                        resolved_dataset,
                        opts.block_hash.clone(),
                        opts.hints.clone(),
                    )),
                );
            }
        } else {
            // TODO: Private Datasets: PERF: find a way to narrow down the number of records
            //       to filter, e.g.:
            //       - Anonymous: get all the public
            //       - Logged: all owned datasets and datasets with relations
            // TODO: PERF: Scanning all datasets is not just super expensive - it may not be
            // possible at the public node scale. We need to patch DataFusion to support
            // unbounded catalogs.
            let mut readable_dataset_handles_stream = self
                .inner
                .dataset_action_authorizer
                .filtered_datasets_stream(
                    self.inner.dataset_registry.all_dataset_handles(),
                    DatasetAction::Read,
                );

            while let Some(Ok(hdl)) = readable_dataset_handles_stream.next().await {
                let resolved_dataset = self.inner.dataset_registry.get_dataset_by_handle(&hdl);

                tables.insert(
                    hdl.alias.to_string(),
                    Arc::new(KamuTable::new(
                        self.inner.session_config.clone(),
                        self.inner.table_options.clone(),
                        resolved_dataset,
                        None,
                        None,
                    )),
                );
            }
        }

        let mut guard = self.inner.tables.lock().unwrap();
        *guard = tables;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SchemaProvider for KamuSchema {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let guard = self.inner.tables.lock().unwrap();
        guard.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let guard = self.inner.tables.lock().unwrap();
        guard.contains_key(name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = {
            let guard = self.inner.tables.lock().unwrap();
            guard.get(name).cloned()
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

impl std::fmt::Debug for KamuSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KamuSchema").finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Table
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct KamuTable {
    session_config: Arc<SessionConfig>,
    table_options: Arc<TableOptions>,
    resolved_dataset: ResolvedDataset,
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
        resolved_dataset: ResolvedDataset,
        as_of: Option<Multihash>,
        hints: Option<DatasetQueryHints>,
    ) -> Self {
        Self {
            session_config,
            table_options,
            resolved_dataset,
            as_of,
            hints,
            cache: Mutex::new(TableCache::default()),
        }
    }

    #[tracing::instrument(level="info", skip_all, fields(dataset = ?self.resolved_dataset))]
    async fn init_table_schema(&self) -> Result<SchemaRef, InternalError> {
        let maybe_set_data_schema = self
            .resolved_dataset
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
    #[tracing::instrument(level="info", skip_all, fields(dataset = ?self.resolved_dataset))]
    async fn init_table_provider(
        &self,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableProvider>, InternalError> {
        let files = self.collect_data_file_hashes(self.as_of.as_ref()).await?;

        if files.is_empty() {
            return Ok(Arc::new(EmptyTable::new(schema)));
        }

        let object_repo = self.resolved_dataset.as_data_repo();
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
            self.resolved_dataset
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
            .resolved_dataset
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

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
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

impl std::fmt::Debug for KamuTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KamuTable")
            .field("resolved_dataset", &self.resolved_dataset)
            .finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
