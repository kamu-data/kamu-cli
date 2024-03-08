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

use datafusion::arrow;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use dill::*;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::*;
use opendatafabric::*;

use crate::query::*;
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

    async fn single_dataset(
        &self,
        dataset_ref: &DatasetRef,
        last_records_to_consider: Option<u64>,
    ) -> Result<(Arc<dyn Dataset>, DataFrame), QueryError> {
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

        let ctx = self.session_context(QueryOptions {
            datasets: vec![DatasetQueryOptions {
                dataset_ref: dataset_handle.as_local_ref(),
                last_records_to_consider,
            }],
        });

        let df = ctx
            .table(TableReference::bare(dataset_handle.alias.to_string()))
            .await?;

        Ok((dataset, df))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_schema_impl(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<arrow::datatypes::SchemaRef>, QueryError> {
        let dataset_handle = self.dataset_repo.resolve_dataset_ref(dataset_ref).await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Read)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let schema_opt = dataset
            .as_metadata_chain()
            .iter_blocks()
            .filter_map_ok(|(_, b)| b.event.into_variant::<SetDataSchema>())
            .try_first()
            .await
            .int_err()?;

        match schema_opt {
            Some(schema) => Ok(Option::from(schema.schema_as_arrow().unwrap())),
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
        let mut visitor = <SearchDataBlocksVisitor>::next_filled_new_data();

        dataset
            .as_metadata_chain()
            .accept(&mut [&mut visitor])
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "Resolving last data slice failed");
                e
            })?;

        // TODO: Update to use SetDataSchema event
        let last_data_slice_opt = visitor.into_event().and_then(|e| e.new_data);

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
        let (dataset, df) = self.single_dataset(dataset_ref, Some(skip + limit)).await?;

        let mut search_set_vocab_visitor = <SearchSetVocabVisitor>::default();

        dataset
            .as_metadata_chain()
            .accept(&mut [&mut search_set_vocab_visitor])
            .await?;

        let vocab: DatasetVocabulary = search_set_vocab_visitor
            .into_event()
            .unwrap_or_default()
            .into();

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
    ) -> Result<Option<arrow::datatypes::SchemaRef>, QueryError> {
        self.get_schema_impl(dataset_ref).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(dataset_ref))]
    async fn get_schema_parquet_file(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Type>, QueryError> {
        let ctx = self.session_context(QueryOptions::default());
        self.get_schema_parquet_impl(&ctx, dataset_ref).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(dataset_ref))]
    async fn get_data(&self, dataset_ref: &DatasetRef) -> Result<DataFrame, QueryError> {
        // TODO: PERF: Limit push-down opportunity
        let (_dataset, df) = self.single_dataset(dataset_ref, None).await?;
        Ok(df)
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
