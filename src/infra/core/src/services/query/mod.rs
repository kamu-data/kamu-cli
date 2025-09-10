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
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Catalog
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with "remote catalog" pattern
// See: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/remote_catalog.rs#L78
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
pub(crate) struct KamuSchema {
    // Note: Never include `SessionContext` or `SessionState` into this structure.
    // Be mindful that this will cause a circular reference and thus prevent context
    // from ever being released.
    session_config: Arc<SessionConfig>,
    table_options: Arc<TableOptions>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    options: QueryOptions,
    tables: Mutex<HashMap<String, Arc<KamuTable>>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl KamuSchema {
    pub async fn prepare(
        session_context: &SessionContext,
        dataset_registry: Arc<dyn DatasetRegistry>,
        options: QueryOptions,
    ) -> Result<Self, InternalError> {
        let schema = Self {
            session_config: Arc::new(session_context.copied_config()),
            table_options: Arc::new(session_context.copied_table_options()),
            dataset_registry,
            options,
            tables: Mutex::new(HashMap::new()),
        };

        schema.init_schema_cache().await?;

        Ok(schema)
    }

    #[tracing::instrument(level = "info", name = "KamuSchema::init_schema_cache" skip_all)]
    async fn init_schema_cache(&self) -> Result<(), InternalError> {
        let mut tables = HashMap::new();

        let input_datasets = self
            .options
            .input_datasets
            .as_ref()
            .expect("QueryService should resolve all inputs");

        for (id, opts) in input_datasets {
            let hdl = opts
                .hints
                .handle
                .clone()
                .expect("QueryService should pre-resolve handles");

            assert_eq!(*id, hdl.id);

            let resolved_dataset = self.dataset_registry.get_dataset_by_handle(&hdl).await;

            tables.insert(
                opts.alias.clone(),
                Arc::new(KamuTable::new(
                    self.session_config.clone(),
                    self.table_options.clone(),
                    resolved_dataset,
                    opts.block_hash.clone(),
                    opts.hints.clone(),
                )),
            );
        }

        {
            let mut guard = self.tables.lock().unwrap();
            *guard = tables;
        }

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
        let guard = self.tables.lock().unwrap();
        guard.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let guard = self.tables.lock().unwrap();
        guard.contains_key(name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let table = {
            let guard = self.tables.lock().unwrap();
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
    as_of: Option<odf::Multihash>,
    hints: DatasetQueryHints,
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
        as_of: Option<odf::Multihash>,
        hints: DatasetQueryHints,
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

    #[tracing::instrument(
        level = "info",
        name = "KamuTable::init_table_schema",
        skip_all,
        fields(dataset = ?self.resolved_dataset),
    )]
    async fn init_table_schema(&self) -> Result<SchemaRef, InternalError> {
        use odf::dataset::MetadataChainExt;

        if self.hints.does_not_need_schema {
            return Ok(Arc::new(Schema::empty()));
        }

        let maybe_set_data_schema = self
            .resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetDataSchemaVisitor::new())
            .await
            .int_err()?
            .into_event();

        if let Some(set_data_schema) = maybe_set_data_schema {
            let odf_schema = set_data_schema.upgrade().schema;

            // TODO: Due to stipping of encoding annotations from ODF schema - Arrow schema
            // restored from it may not be accurate or optimal for reading. Possible edge
            // cases include millisecond-encoded `Date`` type and 128/256-bit `Decimal`.
            // It's unclear if Datafusion will fail on mismatch or will try to convert the
            // Parquet type to conform to the specified schema. We should consider carefully
            // what schema to use here.
            let arrow_schema = odf_schema
                .to_arrow(&odf::metadata::ToArrowSettings {
                    // NOTE: Using more efficient View encodings as default
                    // TODO: Make configurable
                    default_buffer_encoding: Some(odf::metadata::ArrowBufferEncoding::View {
                        offset_bit_width: Some(32),
                    }),
                    ..Default::default()
                })
                .int_err()?;

            Ok(Arc::new(arrow_schema))
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
    #[tracing::instrument(
        level = "info",
        name = "KamuTable::init_table_provider",
        skip_all,
        fields(dataset = ?self.resolved_dataset),
    )]
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
            // NOTE: Here we use Arrow schema that was reconstructed from ODF schema in metadata
            // chain to avoid schema inference step. We must be careful that all Parquet files can
            // actually be coerced into this schema and it is the most efficient representation for
            // querying (e.g. uses zero-copy `View` types).
            schema: Some(&schema),
            // TODO: PERF: potential speedup if we specify `offset` and `system_time`?
            file_sort_order: Vec::new(),
            file_extension: "",
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
            file_decryption_properties: None,
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
        as_of: Option<&odf::Multihash>,
    ) -> Result<Vec<odf::Multihash>, InternalError> {
        let hash = if let Some(hash) = as_of {
            hash.clone()
        } else {
            self.resolved_dataset
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
                .int_err()?
        };

        tracing::debug!(?as_of, "Collecting data slices");

        let last_records_to_consider = self.hints.last_records_to_consider;

        type Flag = odf::metadata::MetadataEventTypeFlags;
        type Decision = odf::dataset::MetadataVisitorDecision;

        struct DataSliceCollectorVisitorState {
            files: Vec<odf::Multihash>,
            num_records: u64,
            last_records_to_consider: Option<u64>,
        }

        use odf::dataset::MetadataChainExt;
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
                        odf::MetadataEvent::AddData(e) => e.new_data.as_ref(),
                        odf::MetadataEvent::ExecuteTransform(e) => e.new_data.as_ref(),
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
