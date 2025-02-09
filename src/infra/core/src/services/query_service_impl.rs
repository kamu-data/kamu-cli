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
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::*;

use crate::services::query::*;
use crate::utils::docker_images;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct QueryServiceImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

#[component(pub)]
#[interface(dyn QueryService)]
impl QueryServiceImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            object_store_registry,
            dataset_action_authorizer,
        }
    }

    async fn session_context(
        &self,
        options: QueryOptions,
    ) -> Result<SessionContext, InternalError> {
        let mut cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("kamu", "kamu");

        // Forcing case-sensitive identifiers in case-insensitive language seems to
        // be a lesser evil than following DataFusion's default behavior of forcing
        // identifiers to lowercase instead of case-insensitive matching.
        //
        // See: https://github.com/apache/datafusion/issues/7460
        // TODO: Consider externalizing this config (e.g. by allowing custom engine
        // options in transform DTOs)
        cfg.options_mut().sql_parser.enable_ident_normalization = false;

        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_object_store_registry(
                    self.object_store_registry.clone().as_datafusion_registry(),
                )
                .build()
                .unwrap(),
        );
        let session_context = SessionContext::new_with_config_rt(cfg, runtime);

        let schema = KamuSchema::prepare(
            &session_context,
            self.dataset_registry.clone(),
            self.dataset_action_authorizer.clone(),
            options,
        )
        .await?;

        session_context.register_catalog("kamu", Arc::new(KamuCatalog::new(Arc::new(schema))));
        Ok(session_context)
    }

    /// Unless state is already provided in the options this will attempt to
    /// parse the SQL, extract the names of all datasets mentioned in the
    /// query and affix their states in the query options to specific blocks.
    async fn resolve_query_state(
        &self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<QueryState, QueryError> {
        use datafusion::sql::parser::Statement;

        let name_resolution_enabled = options.input_datasets.is_empty();

        if !name_resolution_enabled {
            let mut input_datasets = BTreeMap::new();

            for (id, opts) in options.input_datasets {
                // SECURITY: We expect that access permissions will be validated during
                // the query execution and that we're not leaking information here if the
                // user doesn't have access to this dataset.
                let resolved_dataset = self
                    .dataset_registry
                    .get_dataset_by_ref(&id.as_local_ref())
                    .await?;

                let block_hash = if let Some(block_hash) = opts.block_hash {
                    // Validate that block the user is asking for exists
                    // SECURITY: Are we leaking information here by doing this check before auth?
                    if !resolved_dataset
                        .as_metadata_chain()
                        .contains_block(&block_hash)
                        .await
                        .int_err()?
                    {
                        return Err(DatasetBlockNotFoundError::new(id, block_hash).into());
                    }

                    block_hash
                } else {
                    resolved_dataset
                        .as_metadata_chain()
                        .resolve_ref(&odf::BlockRef::Head)
                        .await
                        .int_err()?
                };

                input_datasets.insert(
                    id,
                    QueryStateDataset {
                        alias: opts.alias,
                        block_hash,
                    },
                );
            }
            Ok(QueryState { input_datasets })
        } else {
            // In the name resolution mode we have to inspect SQL to
            // understand which datasets the query is using and populate the block hashes
            // and aliases for them
            let mut table_refs = Vec::new();

            let statements = datafusion::sql::parser::DFParser::parse_sql(sql)
                .map_err(|e| DataFusionError::SQL(e, None))?;

            for stmt in statements {
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
            let mut input_datasets = BTreeMap::new();

            for mut table in table_refs {
                // Strip possible `kamu.kamu.` prefix
                while table.0.len() > 1 && table.0[0].value == "kamu" {
                    table.0.remove(0);
                }
                if table.0.len() == 1 {
                    let alias = table.0.pop().unwrap().value;
                    let Ok(dataset_ref) = odf::DatasetRef::try_from(&alias) else {
                        tracing::warn!(alias, "Ignoring table with invalid alias");
                        continue;
                    };
                    let Ok(hdl) = self
                        .dataset_registry
                        .resolve_dataset_handle_by_ref(&dataset_ref)
                        .await
                    else {
                        tracing::warn!(?dataset_ref, "Ignoring table with unresolvable alias");
                        continue;
                    };

                    // SECURITY: We expect that access permissions will be validated during
                    // the query execution and that we're not leaking information here if the user
                    // doesn't have access to this dataset.
                    let resolved_dataset = self.dataset_registry.get_dataset_by_handle(&hdl);

                    let block_hash = resolved_dataset
                        .as_metadata_chain()
                        .resolve_ref(&odf::BlockRef::Head)
                        .await
                        .int_err()?;

                    input_datasets.insert(hdl.id.clone(), QueryStateDataset { alias, block_hash });
                }
            }

            Ok(QueryState { input_datasets })
        }
    }

    async fn single_dataset(
        &self,
        dataset_ref: &odf::DatasetRef,
        last_records_to_consider: Option<u64>,
    ) -> Result<(ResolvedDataset, DataFrame), QueryError> {
        let resolved_dataset = self.resolve_dataset(dataset_ref).await?;

        let ctx = self
            .session_context(QueryOptions {
                input_datasets: BTreeMap::from([(
                    resolved_dataset.get_id().clone(),
                    QueryOptionsDataset {
                        alias: resolved_dataset.get_alias().to_string(),
                        block_hash: None,
                        hints: Some(DatasetQueryHints {
                            last_records_to_consider,
                        }),
                    },
                )]),
            })
            .await?;

        let df = ctx
            .table(TableReference::bare(
                resolved_dataset.get_alias().to_string(),
            ))
            .await?;

        Ok((resolved_dataset, df))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_schema_impl(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<arrow::datatypes::SchemaRef>, QueryError> {
        let resolved_dataset = self.resolve_dataset(dataset_ref).await?;

        use odf::dataset::MetadataChainExt;
        let schema = resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetDataSchemaVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(|e| e.schema_as_arrow())
            .transpose()
            .int_err()?;

        Ok(schema)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_schema_parquet_impl(
        &self,
        session_context: &SessionContext,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Type>, QueryError> {
        let resolved_dataset = self.resolve_dataset(dataset_ref).await?;

        // TODO: Update to use SetDataSchema event
        use odf::dataset::MetadataChainExt;
        let maybe_last_data_slice_hash = resolved_dataset
            .as_metadata_chain()
            .last_data_block_with_new_data()
            .await
            .int_err()
            .map_err(|e| {
                tracing::error!(error = ?e, error_msg = %e, "Resolving last data slice failed");
                e
            })?
            .into_event()
            .and_then(|event| event.new_data)
            .map(|new_data| new_data.physical_hash);

        match maybe_last_data_slice_hash {
            Some(last_data_slice_hash) => {
                // TODO: Avoid boxing url - requires datafusion to fix API
                let data_url = Box::new(
                    resolved_dataset
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

    async fn resolve_dataset(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ResolvedDataset, QueryError> {
        let dataset_handle = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle.id, DatasetAction::Read)
            .await?;

        Ok(self.dataset_registry.get_dataset_by_handle(&dataset_handle))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl QueryService for QueryServiceImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn create_session(&self) -> Result<SessionContext, CreateSessionError> {
        Ok(self.session_context(QueryOptions::default()).await?)
    }

    #[tracing::instrument(
        level = "info",
        name = "tail",
        skip_all,
        fields(%dataset_ref, %skip, %limit)
    )]
    async fn tail(
        &self,
        dataset_ref: &odf::DatasetRef,
        skip: u64,
        limit: u64,
    ) -> Result<DataFrame, QueryError> {
        let (resolved_dataset, df) = self.single_dataset(dataset_ref, Some(skip + limit)).await?;

        // Our custom catalog provider resolves schemas lazily, so the dataset will be
        // found even if it's empty and its schema will be empty, but we decide not to
        // propagate this special case to the users and return an error instead
        if df.schema().fields().is_empty() {
            Err(DatasetSchemaNotAvailableError {
                dataset_ref: dataset_ref.clone(),
            })?;
        }

        use odf::dataset::MetadataChainExt;
        let vocab: odf::metadata::DatasetVocabulary = resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetVocabVisitor::new())
            .await
            .int_err()?
            .into_event()
            .unwrap_or_default()
            .into();

        let df = df
            .sort(vec![
                col(Column::from_name(&vocab.offset_column)).sort(false, true)
            ])?
            .limit(
                usize::try_from(skip).unwrap(),
                Some(usize::try_from(limit).unwrap()),
            )?
            .sort(vec![
                col(Column::from_name(&vocab.offset_column)).sort(true, false)
            ])?;

        Ok(df)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<QueryResponse, QueryError> {
        tracing::info!(statement, ?options, "Executing SQL query");

        let state = self.resolve_query_state(statement, options.clone()).await?;

        tracing::info!(?state, "Resolved SQL query state");

        // Map resolved state back to options (including hints) for query planner
        let options = QueryOptions {
            input_datasets: state
                .input_datasets
                .iter()
                .map(|(id, s)| {
                    (
                        id.clone(),
                        QueryOptionsDataset {
                            alias: s.alias.clone(),
                            block_hash: Some(s.block_hash.clone()),
                            hints: options
                                .input_datasets
                                .get(id)
                                .and_then(|opt| opt.hints.clone()),
                        },
                    )
                })
                .collect(),
        };
        let ctx = self.session_context(options).await?;
        let df = ctx.sql(statement).await?;

        Ok(QueryResponse { df, state })
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
    async fn get_schema(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<arrow::datatypes::SchemaRef>, QueryError> {
        self.get_schema_impl(dataset_ref).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
    async fn get_schema_parquet_file(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Type>, QueryError> {
        let ctx = self.session_context(QueryOptions::default()).await?;
        self.get_schema_parquet_impl(&ctx, dataset_ref).await
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
    async fn get_data(&self, dataset_ref: &odf::DatasetRef) -> Result<DataFrame, QueryError> {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug", skip_all, fields(%data_slice_store_path))]
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
                error_msg = %e,
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
                error_msg = %e,
                "QueryService::read_data_slice_metadata: Parquet reader get metadata failed"
            );
            e
        })
        .int_err()?;

    Ok(metadata)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
