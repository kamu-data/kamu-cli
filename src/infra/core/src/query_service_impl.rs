// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use dill::*;
use futures::stream::{self, StreamExt, TryStreamExt};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::*;
use opendatafabric::*;

use crate::utils::docker_images;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct QueryServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

#[component(pub)]
#[interface(dyn QueryService)]
impl QueryServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_repo,
            object_store_registry,
            dataset_action_authorizer,
        }
    }

    fn session_context(&self, options: QueryOptions) -> SessionContext {
        let cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("kamu", "kamu");

        let runtime_config = RuntimeConfig {
            object_store_registry: self.object_store_registry.clone().as_datafusion_registry(),
            ..RuntimeConfig::default()
        };
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let session_context = SessionContext::new_with_config_rt(cfg, runtime);

        session_context.register_catalog(
            "kamu",
            Arc::new(KamuCatalog::new(Arc::new(KamuSchema::new(
                session_context.clone(),
                self.dataset_repo.clone(),
                self.dataset_action_authorizer.clone(),
                options,
            )))),
        );
        session_context
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_schema_impl(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<SetDataSchema>, QueryError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Read)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        // TODO: Update to use SetDataSchema event
        let last_data_slice_opt = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<SetDataSchema>())
            .try_first()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Resolving last data slice failed");
                e
            })
            .int_err()?;

        match last_data_slice_opt {
            Some(last_data_slice) => Ok(Some(last_data_slice)),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_schema_parquet_impl(
        &self,
        session_context: &SessionContext,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Type>, QueryError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Read)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let last_data_slice_opt = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.new_data)
            .try_first()
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Resolving last data slice failed");
                e
            })
            .int_err()?;

        match last_data_slice_opt {
            Some(last_data_slice) => {
                // TODO: Avoid boxing url - requires datafusion to fix API
                let data_url = Box::new(
                    dataset
                        .as_data_repo()
                        .get_internal_url(&last_data_slice.physical_hash)
                        .await,
                );

                let object_store = session_context.runtime_env().object_store(&data_url)?;

                tracing::debug!("QueryService::get_schema_impl: obtained object store");

                let data_path =
                    object_store::path::Path::from_url_path(data_url.path()).int_err()?;

                let metadata = read_data_slice_metadata(object_store, &data_path).await?;

                Ok(Some(metadata.file_metadata().schema().clone()))
            }
            None => Ok(None),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl QueryService for QueryServiceImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn create_session(&self) -> Result<SessionContext, CreateSessionError> {
        Ok(self.session_context(QueryOptions::default()))
    }

    #[tracing::instrument(
        level = "info",
        name = "query_service::tail",
        skip_all,
        fields(dataset_ref, num_records)
    )]
    async fn tail(
        &self,
        dataset_ref: &DatasetRef,
        skip: u64,
        limit: u64,
    ) -> Result<DataFrame, QueryError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Read)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let summary = dataset
            .get_summary(GetSummaryOpts::default())
            .await
            .int_err()?;

        if summary.num_records == 0 {
            return Err(DatasetSchemaNotAvailableError {
                dataset_ref: dataset_ref.clone(),
            }
            .into());
        }

        // TODO: PERF: Avoid full scan of metadata
        let vocab: DatasetVocabulary = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<SetVocab>())
            .try_first()
            .await
            .int_err()?
            .unwrap_or_default()
            .into();

        let ctx = self.session_context(QueryOptions {
            datasets: vec![DatasetQueryOptions {
                dataset_ref: dataset_handle.as_local_ref(),
                last_records_to_consider: Some(skip + limit),
            }],
        });

        let df = ctx
            .table(TableReference::bare(dataset_handle.alias.to_string()))
            .await?;

        let df = df
            .sort(vec![col(&vocab.offset_column).sort(false, true)])?
            .limit(
                usize::try_from(skip).unwrap(),
                Some(usize::try_from(limit).unwrap()),
            )?
            .sort(vec![col(&vocab.offset_column).sort(true, false)])?;

        Ok(df)
    }

    #[tracing::instrument(level = "info", skip_all, fields(statement))]
    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<DataFrame, QueryError> {
        let ctx = self.session_context(options);
        Ok(ctx.sql(statement).await?)
    }

    #[tracing::instrument(level = "info", skip_all, fields(dataset_ref))]
    async fn get_schema(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<SetDataSchema>, QueryError> {
        self.get_schema_impl(dataset_ref).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(dataset_ref))]
    async fn get_schema_parquet(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Type>, QueryError> {
        let ctx = self.session_context(QueryOptions::default());
        self.get_schema_parquet_impl(&ctx, dataset_ref).await
    }

    async fn get_known_engines(&self) -> Result<Vec<EngineDesc>, InternalError> {
        Ok(vec![
            EngineDesc {
                name: "Spark".to_string(),
                dialect: QueryDialect::SqlSpark,
                latest_image: docker_images::SPARK.to_string(),
            },
            EngineDesc {
                name: "Flink".to_string(),
                dialect: QueryDialect::SqlFlink,
                latest_image: docker_images::FLINK.to_string(),
            },
            EngineDesc {
                name: "DataFusion".to_string(),
                dialect: QueryDialect::SqlDataFusion,
                latest_image: docker_images::DATAFUSION.to_string(),
            },
        ])
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

// TODO: Performance is poor as it essentially reads all data files in the
// workspace and in some cases (like 'show tables') even twice
#[derive(Clone)]
struct KamuSchema {
    session_context: SessionContext,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    options: QueryOptions,
}

impl KamuSchema {
    fn new(
        session_context: SessionContext,
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        options: QueryOptions,
    ) -> Self {
        Self {
            session_context,
            dataset_repo,
            dataset_action_authorizer,
            options,
        }
    }

    async fn has_data(&self, dataset_handle: &DatasetHandle) -> Result<bool, QueryError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .unwrap();

        let last_records_to_consider = self
            .options_for(dataset_handle)
            .and_then(|o| o.last_records_to_consider);
        let file_hashes = self
            .collect_data_file_hashes(dataset.as_ref(), last_records_to_consider)
            .await?;

        if file_hashes.is_empty() {
            return Ok(false);
        }

        // TODO: Datafusion does not yet support nested types
        // See: https://github.com/apache/arrow-datafusion/issues/2326
        let nested = self
            .is_nested(dataset.as_ref(), file_hashes.first().unwrap())
            .await?;

        Ok(!nested)
    }

    async fn is_nested(
        &self,
        dataset: &dyn Dataset,
        data_file_hash: &Multihash,
    ) -> Result<bool, QueryError> {
        // We have to read file with raw ObjectStore and Parquet to access the metadata
        // TODO: Avoid boxing due invalid API in datafusion
        let data_url = Box::new(
            dataset
                .as_data_repo()
                .get_internal_url(data_file_hash)
                .await,
        );

        let object_store = self.session_context.runtime_env().object_store(&data_url)?;

        let data_path = object_store::path::Path::from_url_path(data_url.path()).int_err()?;

        let metadata = read_data_slice_metadata(object_store, &data_path).await?;
        let schema = metadata.file_metadata().schema();
        Ok(schema.get_fields().iter().any(|f| f.is_group()))
    }

    async fn collect_data_file_hashes(
        &self,
        dataset: &dyn Dataset,
        last_records_to_consider: Option<u64>,
    ) -> Result<Vec<Multihash>, InternalError> {
        let mut files = Vec::new();
        let mut num_records = 0;

        let mut slices = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.new_data);

        while let Some(slice) = slices.try_next().await.int_err()? {
            num_records += slice.num_records();
            files.push(slice.physical_hash);

            if last_records_to_consider.is_some()
                && last_records_to_consider.unwrap() <= num_records
            {
                break;
            }
        }

        Ok(files)
    }

    fn options_for(&self, dataset_handle: &DatasetHandle) -> Option<&DatasetQueryOptions> {
        for opt in &self.options.datasets {
            let same = match &opt.dataset_ref {
                DatasetRef::ID(id) => *id == dataset_handle.id,
                DatasetRef::Alias(alias) => *alias == dataset_handle.alias,
                DatasetRef::Handle(h) => h.id == dataset_handle.id,
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
            let mut dataset_handles = self.dataset_repo.get_all_datasets();

            while let Some(hdl) = dataset_handles.try_next().await.unwrap() {
                if self
                    .dataset_action_authorizer
                    .check_action_allowed(&hdl, auth::DatasetAction::Read)
                    .await
                    .is_ok()
                    && self.has_data(&hdl).await.unwrap()
                {
                    res.push(hdl.alias.to_string());
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
        let Ok(dataset_name) = DatasetName::try_from(name) else {
            return false;
        };

        let maybe_dataset_handle = self
            .dataset_repo
            .try_resolve_dataset_ref(&dataset_name.into())
            .await
            .unwrap();

        if let Some(dh) = maybe_dataset_handle.as_ref() {
            self.dataset_action_authorizer
                .check_action_allowed(dh, auth::DatasetAction::Read)
                .await
                .is_ok()
        } else {
            false
        }
    }
}

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

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let Ok(dataset_alias) = DatasetAlias::try_from(name) else {
            return None;
        };

        let Ok(dataset_handle) = self
            .dataset_repo
            .resolve_dataset_ref(&dataset_alias.as_local_ref())
            .await
        else {
            return None;
        };

        match self
            .dataset_action_authorizer
            .check_action_allowed(&dataset_handle, auth::DatasetAction::Read)
            .await
        {
            Ok(_) => {}
            Err(_) => return None,
        }

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .unwrap();

        let last_records_to_consider = self
            .options_for(&dataset_handle)
            .and_then(|o| o.last_records_to_consider);
        let files = self
            .collect_data_file_hashes(dataset.as_ref(), last_records_to_consider)
            .await
            .unwrap();

        if files.is_empty() {
            return None;
        }

        let object_repo = dataset.as_data_repo();
        let file_urls: Vec<String> = stream::iter(files)
            .then(|h| async move { object_repo.get_internal_url(&h).await })
            .map(Into::into)
            .collect()
            .await;

        let df = self
            .session_context
            .read_parquet(
                file_urls,
                ParquetReadOptions {
                    schema: None,
                    // TODO: PERF: potential speedup if we specify `offset`?
                    file_sort_order: Vec::new(),
                    file_extension: "",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    insert_mode: datafusion::datasource::listing::ListingTableInsertMode::Error,
                },
            )
            .await
            .unwrap();

        Some(df.into_view())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug", skip_all, fields(data_slice_store_path))]
async fn read_data_slice_metadata(
    object_store: Arc<dyn object_store::ObjectStore>,
    data_slice_store_path: &object_store::path::Path,
) -> Result<Arc<ParquetMetaData>, QueryError> {
    let object_meta = object_store
        .head(data_slice_store_path)
        .await
        .map_err(|e| {
            tracing::error!(
                error = ?e,
                "QueryService::read_data_slice_metadata: object store head failed",
            );
            e
        })
        .int_err()?;

    let mut parquet_object_reader = ParquetObjectReader::new(object_store, object_meta);

    use datafusion::parquet::arrow::async_reader::AsyncFileReader;
    let metadata = parquet_object_reader
        .get_metadata()
        .await
        .map_err(|e| {
            tracing::error!(
                error = ?e,
                "QueryService::read_data_slice_metadata: Parquet reader get metadata failed"
            );
            e
        })
        .int_err()?;

    Ok(metadata)
}

/////////////////////////////////////////////////////////////////////////////////////////
