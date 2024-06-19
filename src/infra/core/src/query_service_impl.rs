// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
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
                &session_context,
                self.dataset_repo.clone(),
                self.dataset_action_authorizer.clone(),
                options,
            )))),
        );
        session_context
    }

    /// Unless state is already provided in the options this will attempt to
    /// parse the SQL, extract the names of all datasets mentioned in the
    /// query and affix their states in the query options to specific blocks.
    async fn resolve_query_state(
        &self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<QueryOptions, QueryError> {
        use datafusion::sql::parser::Statement;

        // If options already specify state - use it
        if options.as_of_state.is_some() {
            return Ok(options);
        }

        // If options specify aliases - resolve state just for those datasets
        if let Some(aliases) = &options.aliases {
            let mut as_of_state = QueryState {
                inputs: BTreeMap::new(),
            };
            for id in aliases.values() {
                let dataset = self.dataset_repo.get_dataset(&id.into()).await?;

                // TODO: Do we leak any info by not checking read permissions here?
                let hash = dataset
                    .as_metadata_chain()
                    .resolve_ref(&BlockRef::Head)
                    .await
                    .int_err()?;

                as_of_state.inputs.insert(id.clone(), hash);
            }
            return Ok(QueryOptions {
                as_of_state: Some(as_of_state),
                ..options
            });
        }

        // Since neither state nor aliases are present - we have to inspect SQL to
        // understand which datasets the query is using and populate the state and
        // aliases for them
        let mut table_refs = Vec::new();
        for stmt in datafusion::sql::parser::DFParser::parse_sql(sql).int_err()? {
            match stmt {
                Statement::Statement(stmt) => {
                    table_refs.append(&mut extract_table_refs(&stmt)?);
                }
                Statement::CreateExternalTable(_)
                | Statement::CopyTo(_)
                | Statement::Explain(_) => {}
            }
        }

        // Resolve table references into datasets.
        // We simply ignore unresolvable, letting query to fail at the execution stage.
        let mut aliases = BTreeMap::new();
        let mut as_of_state = QueryState {
            inputs: BTreeMap::new(),
        };
        for mut table in table_refs {
            // Strip possible `kamu.kamu.` prefix
            while table.0.len() > 1 && table.0[0].value == "kamu" {
                table.0.remove(0);
            }
            if table.0.len() == 1 {
                let alias = table.0.pop().unwrap().value;
                let Ok(dataset_ref) = DatasetRef::try_from(&alias) else {
                    tracing::warn!(alias, "Ignoring table with invalid alias");
                    continue;
                };
                let Ok(hdl) = self.dataset_repo.resolve_dataset_ref(&dataset_ref).await else {
                    tracing::warn!(?dataset_ref, "Ignoring table with unresolvable alias");
                    continue;
                };
                let dataset = self
                    .dataset_repo
                    .get_dataset(&hdl.as_local_ref())
                    .await
                    .int_err()?;
                let hash = dataset
                    .as_metadata_chain()
                    .resolve_ref(&BlockRef::Head)
                    .await
                    .int_err()?;
                aliases.insert(alias, hdl.id.clone());
                as_of_state.inputs.insert(hdl.id, hash);
            }
        }

        Ok(QueryOptions {
            aliases: Some(aliases),
            as_of_state: Some(as_of_state),
            hints: options.hints,
        })
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
            aliases: Some(BTreeMap::from([(
                dataset_handle.alias.to_string(),
                dataset_handle.id.clone(),
            )])),
            as_of_state: None,
            hints: Some(BTreeMap::from([(
                dataset_handle.id,
                DatasetQueryHints {
                    last_records_to_consider,
                },
            )])),
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

        // TODO: Update to use SetDataSchema event
        let maybe_last_data_slice_hash = dataset
            .as_metadata_chain()
            .last_data_block_with_new_data()
            .await
            .int_err()
            .map_err(|e| {
                tracing::error!(error = ?e, "Resolving last data slice failed");
                e
            })?
            .into_event()
            .and_then(|event| event.new_data)
            .map(|new_data| new_data.physical_hash);

        match maybe_last_data_slice_hash {
            Some(last_data_slice_hash) => {
                // TODO: Avoid boxing url - requires datafusion to fix API
                let data_url = Box::new(
                    dataset
                        .as_data_repo()
                        .get_internal_url(&last_data_slice_hash)
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

        let vocab: DatasetVocabulary = dataset
            .as_metadata_chain()
            .accept_one(SearchSetVocabVisitor::new())
            .await
            .int_err()?
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

    #[tracing::instrument(level = "info", skip_all)]
    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<QueryResponse, QueryError> {
        tracing::info!(statement, "Executing SQL query");

        let resolved_options = self.resolve_query_state(statement, options.clone()).await?;

        tracing::info!(
            original_options = ?options,
            ?resolved_options,
            "Resolved SQL query",
        );

        let state = resolved_options.as_of_state.clone().unwrap();
        let ctx = self.session_context(options);
        let df = ctx.sql(statement).await?;

        Ok(QueryResponse { df, state })
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
            EngineDesc {
                name: "RisingWave".to_string(),
                dialect: QueryDialect::SqlRisingWave,
                latest_image: docker_images::RISINGWAVE.to_string(),
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

// TODO: This is too complex - we should explore better ways to associate a
// query with a certain state
fn extract_table_refs(
    stmt: &datafusion::sql::sqlparser::ast::Statement,
) -> Result<Vec<datafusion::sql::sqlparser::ast::ObjectName>, QueryError> {
    use datafusion::sql::sqlparser::ast::Statement;

    let mut tables = Vec::new();
    if let Statement::Query(query) = stmt {
        extract_table_refs_rec(query, &mut tables)?;
    }

    Ok(tables)
}

fn extract_table_refs_rec(
    query: &datafusion::sql::sqlparser::ast::Query,
    tables: &mut Vec<datafusion::sql::sqlparser::ast::ObjectName>,
) -> Result<(), QueryError> {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_table_refs_rec(&cte.query, tables)?;
        }
    }
    extract_table_refs_rec_set_expr(&query.body, tables)?;

    // Another pass to remove CTEs from table refs
    // TODO: This may fail in some tricky cases like nested CTEs
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            tables.retain(|tn| tn.0.len() != 1 || tn.0[0] != cte.alias.name);
        }
    }

    Ok(())
}

fn extract_table_refs_rec_set_expr(
    expr: &datafusion::sql::sqlparser::ast::SetExpr,
    tables: &mut Vec<datafusion::sql::sqlparser::ast::ObjectName>,
) -> Result<(), QueryError> {
    use datafusion::sql::sqlparser::ast::SetExpr;

    match expr {
        SetExpr::Select(select) => {
            for twj in &select.from {
                extract_table_refs_rec_table_factor(&twj.relation, tables)?;
                for join in &twj.joins {
                    extract_table_refs_rec_table_factor(&join.relation, tables)?;
                }
            }
            Ok(())
        }
        SetExpr::Query(query) => extract_table_refs_rec(query, tables),
        SetExpr::SetOperation { left, right, .. } => {
            extract_table_refs_rec_set_expr(left, tables)?;
            extract_table_refs_rec_set_expr(right, tables)?;
            Ok(())
        }
        SetExpr::Table(_) | SetExpr::Values(_) | SetExpr::Insert(_) | SetExpr::Update(_) => Ok(()),
    }
}

fn extract_table_refs_rec_table_factor(
    expr: &datafusion::sql::sqlparser::ast::TableFactor,
    tables: &mut Vec<datafusion::sql::sqlparser::ast::ObjectName>,
) -> Result<(), QueryError> {
    use datafusion::sql::sqlparser::ast::TableFactor;

    match expr {
        TableFactor::Table { name, .. } => {
            tables.push(name.clone());
            Ok(())
        }
        TableFactor::Derived { subquery, .. } => extract_table_refs_rec(subquery, tables),
        _ => Ok(()),
    }
}
