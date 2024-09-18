// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use ::datafusion::error::DataFusionError;
use ::datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use ::datafusion::prelude::*;
use auth::OdfServerAccessTokenResolverNull;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::{Constraints, Statistics};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::datasource::TableType;
use datafusion::execution::context::DataFilePaths as _;
use datafusion::execution::options::ReadOptions as _;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use futures::{stream, StreamExt};
use internal_error::*;
use kamu::domain::*;
use kamu::*;
use opendatafabric::{MetadataEvent, MetadataEventTypeFlags, Multihash};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ipfs_query() {
    let catalog = dill::CatalogBuilder::new()
        .add_value(IpfsGateway {
            url: Url::parse("https://yellow-wrong-raccoon-732.mypinata.cloud/").unwrap(),
            pre_resolve_dnslink: true,
        })
        .add::<OdfServerAccessTokenResolverNull>()
        .add::<ObjectStoreRegistryImpl>()
        .add::<DatasetFactoryImpl>()
        .build();

    let dataset_factory: Arc<dyn DatasetFactory> = catalog.get_one().unwrap();
    //let object_store_registry: Arc<dyn ObjectStoreRegistry> =
    // catalog.get_one().unwrap();

    let mut cfg = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema("kamu", "kamu");

    cfg.options_mut().sql_parser.enable_ident_normalization = false;

    let runtime_config = RuntimeConfig {
        //object_store_registry: object_store_registry.as_datafusion_registry(),
        ..RuntimeConfig::default()
    };
    runtime_config.object_store_registry.register_store(
        &Url::parse("https://yellow-wrong-raccoon-732.mypinata.cloud/").unwrap(),
        Arc::new(ObjectStoreWithTracing::new(
            object_store::http::HttpBuilder::new()
                .with_url("https://yellow-wrong-raccoon-732.mypinata.cloud/")
                .build()
                .unwrap(),
        )),
    );
    let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
    let ctx = SessionContext::new_with_config_rt(cfg, runtime);

    ctx.register_catalog(
        "kamu",
        Arc::new(IpfsCatalog {
            schema: Arc::new(IpfsSchema {
                tables: [
                    (
                        "spy".to_string(),
                        Arc::new(
                            IpfsTable::new(
                                &ctx,
                                dataset_factory
                                    .get_dataset(
                                        &Url::parse("ipns://test-1.ipns.kamu.dev").unwrap(),
                                        false,
                                    )
                                    .await
                                    .unwrap(),
                            )
                            .await
                            .unwrap(),
                        ),
                    ),
                    (
                        "eth".to_string(),
                        Arc::new(
                            IpfsTable::new(
                                &ctx,
                                dataset_factory
                                    .get_dataset(
                                        &Url::parse("ipns://test-2.ipns.kamu.dev").unwrap(),
                                        false,
                                    )
                                    .await
                                    .unwrap(),
                            )
                            .await
                            .unwrap(),
                        ),
                    ),
                ]
                .into_iter()
                .collect(),
            }),
        }),
    );

    // let df = ctx
    //     .sql(
    //         r#"
    //         (
    //             select from_symbol, to_symbol, open, close, high, low
    //             from spy
    //             order by offset desc
    //             limit 1
    //         )
    //         union all
    //         (
    //             select from_symbol, to_symbol, open, close, high, low
    //             from eth
    //             order by offset desc
    //             limit 1
    //         )
    //         "#,
    //     )
    //     .await
    //     .unwrap();
    // df.show().await.unwrap();

    let df = ctx
        .sql(
            r#"
            select from_symbol, to_symbol, open, close, high, low
            from spy
            order by offset
            limit 1
            "#,
        )
        .await
        .unwrap();
    df.show().await.unwrap();

    //////
    // TODO:
    // - dataset metadata caching (eliminate two passess for schema and data)
    /////
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IpfsCatalog {
    schema: Arc<IpfsSchema>,
}

impl CatalogProvider for IpfsCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["kamu".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        assert_eq!(name, "kamu");
        Some(self.schema.clone())
    }
}

struct IpfsSchema {
    tables: BTreeMap<String, Arc<IpfsTable>>,
}

#[async_trait::async_trait]
impl SchemaProvider for IpfsSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().contains(&name.to_string())
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self
            .tables
            .get(name)
            .map(|t| t.clone() as Arc<dyn TableProvider>))
    }
}

struct IpfsTable {
    table_provider: Arc<dyn TableProvider>,
}

#[async_trait::async_trait]
impl TableProvider for IpfsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_provider.schema()
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
        self.table_provider
            .scan(state, projection, filters, limit)
            .await
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

impl IpfsTable {
    async fn new(
        session_context: &SessionContext,
        dataset: Arc<dyn Dataset>,
    ) -> Result<Self, InternalError> {
        let head = dataset
            .as_metadata_chain()
            .try_get_ref(&BlockRef::Head)
            .await
            .int_err()?
            .unwrap();

        let schema = Self::init_table_schema(&dataset, &head).await?;

        let table_provider =
            Self::init_table_provider(session_context, schema, &dataset, &head).await?;

        Ok(Self { table_provider })
    }

    async fn init_table_schema(
        dataset: &Arc<dyn Dataset>,
        hash: &Multihash,
    ) -> Result<SchemaRef, InternalError> {
        let maybe_set_data_schema = dataset
            .as_metadata_chain()
            .accept_one_by_hash(hash, SearchSetDataSchemaVisitor::new())
            .await
            .int_err()?
            .into_event();

        if let Some(set_data_schema) = maybe_set_data_schema {
            set_data_schema.schema_as_arrow().int_err()
        } else {
            Ok(Arc::new(Schema::empty()))
        }
    }

    // TODO: A lot of duplication from `SessionContext::read_parquet` - code is
    // copied as we need table provider and not the `DataFrame`
    async fn init_table_provider(
        session_context: &SessionContext,
        schema: SchemaRef,
        dataset: &Arc<dyn Dataset>,
        hash: &Multihash,
    ) -> Result<Arc<dyn TableProvider>, InternalError> {
        let files = Self::collect_data_file_hashes(dataset, hash).await?;

        if files.is_empty() {
            return Ok(Arc::new(EmptyTable::new(schema)));
        }

        let object_repo = dataset.as_data_repo();
        let file_urls: Vec<String> = stream::iter(files)
            .then(|h| async move { object_repo.get_internal_url(&h).await })
            .map(Into::into)
            .collect()
            .await;

        let options = ParquetReadOptions {
            schema: Some(&schema),
            file_sort_order: vec![vec![col("offset").sort(true, false)]],
            file_extension: "",
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
        };

        let session_config = session_context.copied_config();
        let table_options = session_context.copied_table_options();
        let table_paths = file_urls.to_urls().int_err()?;
        let listing_options = options.to_listing_options(&session_config, table_options);

        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let provider = ListingTable::try_new(config).int_err()?;

        Ok(Arc::new(provider))
    }

    async fn collect_data_file_hashes(
        dataset: &Arc<dyn Dataset>,
        hash: &Multihash,
    ) -> Result<Vec<Multihash>, InternalError> {
        type Flag = MetadataEventTypeFlags;
        type Decision = MetadataVisitorDecision;

        let slices = dataset
            .as_metadata_chain()
            .reduce_by_hash(
                hash,
                Vec::new(),
                Decision::NextOfType(Flag::DATA_BLOCK),
                |state, _hash, block| {
                    let new_data = match &block.event {
                        MetadataEvent::AddData(e) => e.new_data.as_ref(),
                        MetadataEvent::ExecuteTransform(e) => e.new_data.as_ref(),
                        _ => unreachable!(),
                    };

                    if let Some(slice) = new_data {
                        state.push(slice.physical_hash.clone());
                    }

                    Decision::NextOfType(Flag::DATA_BLOCK)
                },
            )
            .await
            .int_err()?;

        tracing::debug!(num_slices = slices.len(), "Slices collected");
        Ok(slices)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
