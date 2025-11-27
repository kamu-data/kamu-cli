// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Constraints, Statistics};
use datafusion::config::TableOptions;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::ResolvedDataset;

use crate::EngineConfigDatafusionEmbeddedBatchQueryExt;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    async fn init_table_schema(
        &self,
        cfg: &EngineConfigDatafusionEmbeddedBatchQueryExt,
    ) -> Result<SchemaRef, InternalError> {
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
            let settings = if !cfg.use_legacy_arrow_buffer_encoding {
                odf::metadata::ToArrowSettings {
                    // NOTE: Using more efficient View encodings as default
                    // TODO: Improve configurability
                    default_binary_encoding: Some(odf::metadata::ArrowBufferEncoding::View {
                        offset_bit_width: Some(32),
                    }),
                    default_string_encoding: Some(odf::metadata::ArrowBufferEncoding::View {
                        offset_bit_width: Some(32),
                    }),
                    ..Default::default()
                }
            } else {
                odf::metadata::ToArrowSettings {
                    default_binary_encoding: Some(odf::metadata::ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(32),
                    }),
                    default_string_encoding: Some(odf::metadata::ArrowBufferEncoding::Contiguous {
                        offset_bit_width: Some(32),
                    }),
                    ..Default::default()
                }
            };

            let arrow_schema = odf_schema.to_arrow(&settings).int_err()?;

            Ok(Arc::new(arrow_schema))
        } else {
            Ok(Arc::new(Schema::empty()))
        }
    }

    pub(crate) async fn get_table_schema(&self) -> Result<SchemaRef, InternalError> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(schema) = &cache.schema {
                return Ok(Arc::clone(schema));
            }
        }

        let cfg = self
            .session_config
            .get_extension::<EngineConfigDatafusionEmbeddedBatchQueryExt>()
            .unwrap_or_default();

        let schema = self.init_table_schema(&cfg).await?;

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

        use futures::stream::{self, StreamExt};

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

        use datafusion::execution::context::DataFilePaths;
        use datafusion::execution::options::ReadOptions;

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

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
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
