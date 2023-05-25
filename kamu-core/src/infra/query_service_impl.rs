// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::basic::LogicalType;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::*;
use dill::*;
use futures::stream::TryStreamExt;
use opendatafabric::*;

use crate::domain::*;
use crate::infra::utils::datafusion_hacks::ListingTableOfFiles;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct QueryServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
}

#[component(pub)]
impl QueryServiceImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
    ) -> Self {
        Self {
            dataset_repo,
            object_store_registry,
        }
    }

    fn session_context(&self, options: QueryOptions) -> Result<SessionContext, InternalError> {
        let cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("kamu", "kamu");

        let runtime_config = RuntimeConfig {
            object_store_registry: self.object_store_registry.clone().as_datafusion_registry(),
            ..RuntimeConfig::default()
        };
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let session_context = SessionContext::with_config_rt(cfg, runtime);

        session_context.register_catalog(
            "kamu",
            Arc::new(KamuCatalog::new(Arc::new(KamuSchema::new(
                session_context.clone(),
                self.dataset_repo.clone(),
                options,
            )))),
        );
        Ok(session_context)
    }

    async fn get_schema_impl(
        &self,
        session_context: &SessionContext,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Type>, QueryError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let last_data_slice_opt = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.output_data)
            .try_first()
            .await
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
    async fn tail(
        &self,
        dataset_ref: &DatasetRef,
        num_records: u64,
    ) -> Result<DataFrame, QueryError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;
        let dataset = self
            .dataset_repo
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

        let ctx = self
            .session_context(QueryOptions {
                datasets: vec![DatasetQueryOptions {
                    dataset_ref: dataset_handle.as_local_ref(),
                    limit: Some(num_records),
                }],
            })
            .map_err(|e| QueryError::Internal(e))?;

        // TODO: This is a workaround for Arrow not handling timestamps with explicit
        // timezones. We basically have to re-cast all timestamp fields into
        // timestamps after querying. See:
        // - https://github.com/apache/arrow-datafusion/issues/959
        // - https://github.com/apache/arrow-rs/issues/393
        let res_schema = self
            .get_schema_impl(&ctx, &dataset_handle.as_local_ref())
            .await?;
        if let None = res_schema {
            return Err(QueryError::DatasetSchemaNotAvailable(
                DatasetSchemaNotAvailableError {
                    dataset_ref: dataset_handle.as_local_ref(),
                },
            ));
        }

        let fields: Vec<String> = match res_schema.unwrap() {
            Type::GroupType { fields, .. } => fields
                .iter()
                .map(|f| match f.as_ref() {
                    pt @ Type::PrimitiveType { .. } => {
                        if let Some(LogicalType::Timestamp {
                            is_adjusted_to_u_t_c,
                            ..
                        }) = pt.get_basic_info().logical_type()
                        {
                            if is_adjusted_to_u_t_c {
                                return format!(
                                    "CAST(\"{name}\" as TIMESTAMP) as \"{name}\"",
                                    name = pt.get_basic_info().name()
                                );
                            }
                        }
                        format!("\"{}\"", pt.get_basic_info().name())
                    }
                    Type::GroupType { basic_info, .. } => {
                        format!("\"{}\"", basic_info.name())
                    }
                })
                .collect(),
            Type::PrimitiveType { .. } => unreachable!(),
        };

        let query = format!(
            r#"SELECT {fields} FROM "{dataset}" ORDER BY {offset_col} DESC LIMIT {num_records}"#,
            fields = fields.join(", "),
            dataset = dataset_handle.alias,
            offset_col = vocab.offset_column.unwrap_or("offset".to_owned()),
            num_records = num_records
        );

        Ok(ctx.sql(&query).await?)
    }

    #[tracing::instrument(level = "info", skip_all, fields(statement))]
    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<DataFrame, QueryError> {
        let ctx = self
            .session_context(options)
            .map_err(|e| QueryError::Internal(e))?;
        Ok(ctx.sql(statement).await?)
    }

    async fn get_schema(&self, dataset_ref: &DatasetRef) -> Result<Option<Type>, QueryError> {
        let ctx = self
            .session_context(QueryOptions::default())
            .map_err(|e| QueryError::Internal(e))?;
        self.get_schema_impl(&ctx, dataset_ref).await
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
    options: QueryOptions,
}

impl KamuSchema {
    fn new(
        session_context: SessionContext,
        dataset_repo: Arc<dyn DatasetRepository>,
        options: QueryOptions,
    ) -> Self {
        Self {
            session_context,
            dataset_repo,
            options,
        }
    }

    async fn has_data(&self, dataset_handle: &DatasetHandle) -> Result<bool, QueryError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .unwrap();

        let limit = self.options_for(dataset_handle).and_then(|o| o.limit);
        let files = self.collect_data_files(dataset.as_ref(), limit).await?;

        if files.is_empty() {
            return Ok(false);
        }

        // TODO: Datafusion does not yet support nested types
        // See: https://github.com/apache/arrow-datafusion/issues/2326
        let nested = self
            .is_nested(dataset.as_ref(), files.first().unwrap())
            .await?;

        Ok(!nested)
    }

    async fn is_nested(
        &self,
        dataset: &dyn Dataset,
        file: &object_store::path::Path,
    ) -> Result<bool, QueryError> {
        let (_, object_store) = self.object_store_for(dataset).await;
        let metadata = read_data_slice_metadata(object_store, &file).await?;
        let schema = metadata.file_metadata().schema();
        Ok(schema.get_fields().iter().any(|f| f.is_group()))
    }

    async fn collect_data_files(
        &self,
        dataset: &dyn Dataset,
        limit: Option<u64>,
    ) -> Result<Vec<object_store::path::Path>, InternalError> {
        let mut files = Vec::new();
        let mut num_records = 0;

        let mut slices = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.output_data);

        while let Some(slice) = slices.try_next().await.int_err()? {
            num_records += slice.interval.end - slice.interval.start + 1;

            let slice_url = dataset
                .as_data_repo()
                .get_internal_url(&slice.physical_hash)
                .await;

            files.push(object_store::path::Path::from_url_path(slice_url.path()).int_err()?);

            if limit.is_some() && limit.unwrap() <= num_records as u64 {
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
                if self.has_data(&hdl).await.unwrap() {
                    res.push(hdl.alias.to_string())
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

        self.dataset_repo
            .try_resolve_dataset_ref(&dataset_name.into())
            .await
            .unwrap()
            .is_some()
    }

    async fn object_store_for(
        &self,
        dataset: &dyn Dataset,
    ) -> (url::Url, Arc<dyn object_store::ObjectStore>) {
        // TODO: Abusing the fact that we can get Url for a non-existing
        // hash
        let data_url = Box::new(
            dataset
                .as_data_repo()
                .get_internal_url(&Multihash::from_digest_sha3_256(b""))
                .await,
        );

        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&data_url)
            .unwrap();

        let object_store_url = url::Url::parse(&format!(
            "{}://{}",
            data_url.scheme(),
            &data_url[url::Position::BeforeHost..url::Position::AfterPort],
        ))
        .unwrap();

        (object_store_url, object_store)
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
        // TODO: multitencancy
        let dataset_name = match DatasetName::try_from(name) {
            Ok(name) => name,
            Err(_) => return None,
        };

        let dataset_handle = match self
            .dataset_repo
            .resolve_dataset_ref(&dataset_name.as_local_ref())
            .await
        {
            Ok(hdl) => hdl,
            Err(_) => return None,
        };

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await
            .unwrap();

        let limit = self.options_for(&dataset_handle).and_then(|o| o.limit);
        let files = self
            .collect_data_files(dataset.as_ref(), limit)
            .await
            .unwrap();

        if files.is_empty() {
            return None;
        }

        let (object_store_url, object_store) = self.object_store_for(dataset.as_ref()).await;

        let table = ListingTableOfFiles::try_new(
            object_store_url,
            object_store,
            &self.session_context.state(),
            files,
        )
        .await
        .unwrap();

        Some(Arc::new(table))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn read_data_slice_metadata(
    object_store: Arc<dyn object_store::ObjectStore>,
    data_slice_store_path: &object_store::path::Path,
) -> Result<Arc<ParquetMetaData>, QueryError> {
    let object_meta = object_store.head(&data_slice_store_path).await.int_err()?;

    let mut parquet_object_reader = ParquetObjectReader::new(object_store, object_meta);

    use datafusion::parquet::arrow::async_reader::AsyncFileReader;
    let metadata = parquet_object_reader.get_metadata().await.int_err()?;
    Ok(metadata)
}

/////////////////////////////////////////////////////////////////////////////////////////
