// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionAuthorizerExt as _};
use kamu_core::{auth, *};
use odf::utils::data::DataFrameExt;

use crate::EngineConfigDatafusionEmbeddedBatchQuery;
use crate::services::query::*;
use crate::utils::docker_images;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn QueryService)]
pub struct QueryServiceImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    datafusion_engine_config: Arc<EngineConfigDatafusionEmbeddedBatchQuery>,
}

impl QueryServiceImpl {
    async fn session_context(
        &self,
        options: QueryOptions,
    ) -> Result<SessionContext, InternalError> {
        assert!(
            options.input_datasets.is_some(),
            "QueryService should resolve all inputs"
        );
        for opts in options.input_datasets.as_ref().unwrap().values() {
            assert!(
                opts.hints.handle.is_some(),
                "QueryService should pre-resolve handles"
            );
        }

        let config = self.datafusion_engine_config.0.clone();

        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_object_store_registry(
                    self.object_store_registry.clone().as_datafusion_registry(),
                )
                .build()
                .unwrap(),
        );

        #[allow(unused_mut)]
        let mut ctx = SessionContext::new_with_config_rt(config, runtime);

        let schema = KamuSchema::prepare(&ctx, self.dataset_registry.clone(), options).await?;

        ctx.register_catalog("kamu", Arc::new(KamuCatalog::new(Arc::new(schema))));

        cfg_if::cfg_if! {
            if #[cfg(feature = "query-extensions-json")] {
                datafusion_functions_json::register_all(&mut ctx).unwrap();
            }
        }

        Ok(ctx)
    }

    /// Unless state is already provided in the options this will attempt to
    /// parse the SQL, extract the names of all datasets mentioned in the
    /// query and affix their states in the query options to specific blocks.
    #[expect(clippy::single_match)]
    #[tracing::instrument(
        level = "info",
        name = "QueryServiceImpl::resolve_query_state",
        skip_all
    )]
    async fn resolve_query_state(
        &self,
        sql: &str,
        options: QueryOptions,
    ) -> Result<(QueryState, QueryOptions), QueryError> {
        use datafusion::sql::parser::Statement as DfStatement;
        use datafusion::sql::sqlparser::ast::Statement as SqlStatement;

        let mut new_dataset_opts = BTreeMap::new();
        let mut input_dataset_states = BTreeMap::new();

        // If input datasets options are specified we check access and resolve block
        // hash if not already provided.
        //
        // Otherwise, we infer the datasets from the query itself.
        if let Some(input_dataset_opts) = options.input_datasets {
            // Vectorized access check
            let dataset_id_refs = input_dataset_opts
                .keys()
                .map(Cow::Borrowed)
                .collect::<Vec<_>>();
            let by_access = self
                .dataset_action_authorizer
                .classify_dataset_ids_by_allowance(&dataset_id_refs, DatasetAction::Read)
                .await?;

            for (id, _) in by_access.unauthorized_ids_with_errors {
                tracing::warn!(?id, "Restricting access to unauthorized dataset");
            }

            // We skip adding inaccessible datasets to state and options and let the query
            // fail with "not found" error
            for id in by_access.authorized_ids {
                let mut opts = input_dataset_opts.get(&id).unwrap().clone();

                let hdl = if let Some(hdl) = &opts.hints.handle {
                    hdl.clone()
                } else {
                    let hdl = self
                        .dataset_registry
                        .resolve_dataset_handle_by_ref(&id.as_local_ref())
                        .await?;
                    opts.hints.handle = Some(hdl.clone());
                    hdl
                };

                let resolved_dataset = self.dataset_registry.get_dataset_by_handle(&hdl).await;

                let block_hash = if let Some(block_hash) = &opts.block_hash {
                    // Validate that block the user is asking for exists
                    if !resolved_dataset
                        .as_metadata_chain()
                        .contains_block(block_hash)
                        .await
                        .int_err()?
                    {
                        return Err(DatasetBlockNotFoundError::new(id, block_hash.clone()).into());
                    }

                    block_hash.clone()
                } else {
                    let block_hash = resolved_dataset
                        .as_metadata_chain()
                        .resolve_ref(&odf::BlockRef::Head)
                        .await
                        .int_err()?;

                    opts.block_hash = Some(block_hash.clone());
                    block_hash
                };

                input_dataset_states.insert(
                    id.clone(),
                    QueryStateDataset {
                        alias: opts.alias.clone(),
                        block_hash,
                    },
                );

                new_dataset_opts.insert(id, opts);
            }
        } else {
            // We will inspect SQL to understand which datasets the query is using and
            // populate the block hashes and aliases for them
            let mut table_refs = Vec::new();

            // Some queries like `show tables` will not be affixed to a specific set of
            // datasets and for now require populating session context with all datasets
            // accessible to the user. We will not include such datasets in the state, but
            // will propagate them to [`KamuSchema`] via options.
            let mut is_restricted_set = false;
            let mut needs_schema = false;

            // TODO: Replace with "remote catalog" pattern
            // See: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/remote_catalog.rs#L78
            let statements = datafusion::sql::parser::DFParser::parse_sql(sql)?;

            for stmt in statements {
                match stmt {
                    DfStatement::Statement(stmt) => match stmt.as_ref() {
                        SqlStatement::Query(query) => {
                            is_restricted_set = true;
                            needs_schema = true;
                            extract_table_refs_rec(query, &mut table_refs)?;
                        }
                        _ => {}
                    },
                    // TODO: SEC: Restrict the subset of supported statements
                    _ => {}
                }
            }

            tracing::debug!(
                is_restricted_set,
                needs_schema,
                ?table_refs,
                "Extracted table references from query statements",
            );

            if !is_restricted_set {
                // TODO: PERF: SEC: Scanning all datasets is not just super expensive - it may
                // not be possible at the public node scale. We need to restrict
                // the number of datasets that can be scanned.
                // Options:
                // - Fail unrestricted query (e.g. `show tables`) after it touches >N datasets
                // - Require specifying `limit` or `like` filters if number of datasets in the
                //   node is too large
                //
                // TODO: Private Datasets: PERF: find a way to narrow down the number of records
                // to filter, e.g.:
                // - Anonymous: get all the public
                // - Logged: all owned datasets and datasets with relations
                let handles: Vec<odf::DatasetHandle> = self
                    .dataset_action_authorizer
                    .filtered_datasets_stream(
                        self.dataset_registry.all_dataset_handles(),
                        DatasetAction::Read,
                    )
                    .try_collect()
                    .await?;

                // We don't include datasets into the state, but add them to options
                for hdl in handles {
                    new_dataset_opts.insert(
                        hdl.id.clone(),
                        QueryOptionsDataset {
                            alias: hdl.alias.to_string(),
                            block_hash: None,
                            hints: DatasetQueryHints {
                                handle: Some(hdl),
                                last_records_to_consider: None,
                                does_not_need_schema: !needs_schema,
                            },
                        },
                    );
                }
            } else {
                // Resolve table references into datasets.
                // We simply ignore unresolvable, letting query to fail at the execution stage.
                for mut table in table_refs {
                    // Strip possible `kamu.kamu.` prefix
                    while table.0.len() > 1
                        && table.0[0].as_ident().map(|i| i.value.as_ref()) == Some("kamu")
                    {
                        table.0.remove(0);
                    }

                    if table.0.len() != 1 {
                        continue;
                    }

                    let datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(ident) =
                        table.0.pop().unwrap()
                    else {
                        tracing::warn!(?table, "Ignoring identifier");
                        continue;
                    };

                    let alias = ident.value;
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

                    if !self
                        .dataset_action_authorizer
                        .is_action_allowed(&hdl.id, DatasetAction::Read)
                        .await?
                    {
                        // Ignore this alias and let the query fail with "not found" error
                        tracing::warn!(?dataset_ref, "Restricting access to unauthorized dataset");
                        continue;
                    }

                    let resolved_dataset = self.dataset_registry.get_dataset_by_handle(&hdl).await;

                    let block_hash = resolved_dataset
                        .as_metadata_chain()
                        .resolve_ref(&odf::BlockRef::Head)
                        .await
                        .int_err()?;

                    input_dataset_states.insert(
                        hdl.id.clone(),
                        QueryStateDataset {
                            alias: alias.clone(),
                            block_hash: block_hash.clone(),
                        },
                    );

                    new_dataset_opts.insert(
                        hdl.id.clone(),
                        QueryOptionsDataset {
                            alias,
                            block_hash: Some(block_hash),
                            hints: DatasetQueryHints {
                                handle: Some(hdl),
                                last_records_to_consider: None,
                                does_not_need_schema: !needs_schema,
                            },
                        },
                    );
                }
            }
        }

        Ok((
            QueryState {
                input_datasets: input_dataset_states,
            },
            QueryOptions {
                input_datasets: Some(new_dataset_opts),
            },
        ))
    }

    async fn single_dataset(
        &self,
        dataset_ref: &odf::DatasetRef,
        last_records_to_consider: Option<u64>,
        head: Option<odf::Multihash>,
    ) -> Result<(ResolvedDataset, odf::Multihash, Option<DataFrameExt>), QueryError> {
        let resolved_dataset = self.resolve_dataset(dataset_ref).await?;

        let head = if let Some(head) = head {
            head
        } else {
            resolved_dataset
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
                .int_err()?
        };

        let ctx = self
            .session_context(QueryOptions {
                input_datasets: Some(BTreeMap::from([(
                    resolved_dataset.get_id().clone(),
                    QueryOptionsDataset {
                        alias: resolved_dataset.get_alias().to_string(),
                        block_hash: Some(head.clone()),
                        hints: DatasetQueryHints {
                            handle: Some(resolved_dataset.get_handle().clone()),
                            last_records_to_consider,
                            does_not_need_schema: false,
                        },
                    },
                )])),
            })
            .await?;

        let df = ctx
            .table(TableReference::bare(
                resolved_dataset.get_alias().to_string(),
            ))
            .await?;

        if df.schema().fields().is_empty() {
            Ok((resolved_dataset, head, None))
        } else {
            Ok((resolved_dataset, head, Some(df.into())))
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_schema_impl(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Arc<odf::schema::DataSchema>>, QueryError> {
        let resolved_dataset = self.resolve_dataset(dataset_ref).await?;

        use odf::dataset::MetadataChainExt;
        let schema = resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSetDataSchemaVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(|e| Arc::new(e.upgrade().schema));

        Ok(schema)
    }

    async fn resolve_dataset(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ResolvedDataset, QueryError> {
        let resolved_dataset = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_ref(dataset_ref, auth::DatasetAction::Read)
            .await
            .map_err(|e| {
                use kamu_auth_rebac::RebacDatasetRefUnresolvedError as E;
                match e {
                    E::NotFound(e) => QueryError::DatasetNotFound(e),
                    E::Access(e) => QueryError::Access(e),
                    e @ E::Internal(_) => QueryError::Internal(e.int_err()),
                }
            })?;

        Ok(resolved_dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl QueryService for QueryServiceImpl {
    #[tracing::instrument(level = "info", name = QueryServiceImpl_create_session, skip_all)]
    async fn create_session(&self) -> Result<SessionContext, CreateSessionError> {
        // TODO: PERF: Avoid pre-registering all datasets
        // Use DataFusion remote catalog pattern instead:
        // https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/remote_catalog.rs
        let handles: Vec<odf::DatasetHandle> = self
            .dataset_action_authorizer
            .filtered_datasets_stream(
                self.dataset_registry.all_dataset_handles(),
                DatasetAction::Read,
            )
            .try_collect()
            .await?;

        // We don't include datasets into the state, but add them to options
        let input_datasets = handles
            .into_iter()
            .map(|hdl| {
                (
                    hdl.id.clone(),
                    QueryOptionsDataset {
                        alias: hdl.alias.to_string(),
                        block_hash: None,
                        hints: DatasetQueryHints {
                            handle: Some(hdl),
                            last_records_to_consider: None,
                            does_not_need_schema: false,
                        },
                    },
                )
            })
            .collect();

        let ctx = self
            .session_context(QueryOptions {
                input_datasets: Some(input_datasets),
            })
            .await?;

        Ok(ctx)
    }

    #[tracing::instrument(
        level = "info",
        name = QueryServiceImpl_tail,
        skip_all,
        fields(%dataset_ref, %skip, %limit)
    )]
    async fn tail(
        &self,
        dataset_ref: &odf::DatasetRef,
        skip: u64,
        limit: u64,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError> {
        let (resolved_dataset, head, df) = self
            .single_dataset(dataset_ref, Some(skip + limit), options.block_hash)
            .await?;

        // Our custom catalog provider resolves schemas lazily, so the dataset will be
        // found even if it's empty and its schema will be empty, but we decide not to
        // propagate this special case to the users and return `None` instead
        let Some(df) = df else {
            return Ok(GetDataResponse {
                df: None,
                dataset_handle: resolved_dataset.take_handle(),
                block_hash: head,
            });
        };

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
                col(Column::from_name(&vocab.offset_column)).sort(false, true),
            ])?
            .limit(
                usize::try_from(skip).unwrap(),
                Some(usize::try_from(limit).unwrap()),
            )?
            .sort(vec![
                col(Column::from_name(&vocab.offset_column)).sort(true, false),
            ])?;

        Ok(GetDataResponse {
            df: Some(df),
            dataset_handle: resolved_dataset.take_handle(),
            block_hash: head,
        })
    }

    #[tracing::instrument(level = "info", name = QueryServiceImpl_sql_statement, skip_all)]
    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<QueryResponse, QueryError> {
        tracing::info!(statement, ?options, "Executing SQL query");

        let (state, new_options) = self.resolve_query_state(statement, options).await?;
        tracing::info!(?state, ?new_options, "Resolved SQL query state");

        let ctx = self.session_context(new_options).await?;
        let df = ctx.sql(statement).await?;

        Ok(QueryResponse {
            df: df.into(),
            state,
        })
    }

    #[tracing::instrument(level = "info", name = QueryServiceImpl_get_schema, skip_all, fields(%dataset_ref))]
    async fn get_schema(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Arc<odf::schema::DataSchema>>, QueryError> {
        self.get_schema_impl(dataset_ref).await
    }

    #[tracing::instrument(level = "info", skip_all, name = QueryServiceImpl_get_last_data_chunk_schema_arrow, fields(%dataset_ref))]
    async fn get_last_data_chunk_schema_arrow(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<datafusion::arrow::datatypes::SchemaRef>, QueryError> {
        let resolved_dataset = self.resolve_dataset(dataset_ref).await?;

        use odf::dataset::MetadataChainExt;
        let Some(last_data_slice_hash) = resolved_dataset
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
            .map(|new_data| new_data.physical_hash)
        else {
            return Ok(None);
        };

        let data_url = resolved_dataset
            .as_data_repo()
            .get_internal_url(&last_data_slice_hash)
            .await;

        let ctx = self
            .session_context(QueryOptions {
                input_datasets: Some(BTreeMap::new()),
            })
            .await?;

        let df = ctx
            .read_parquet(
                data_url.to_string(),
                ParquetReadOptions {
                    schema: None,
                    file_sort_order: Vec::new(),
                    file_extension: "",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    file_decryption_properties: None,
                },
            )
            .await
            .int_err()?;

        Ok(Some(df.schema().inner().clone()))
    }

    #[tracing::instrument(level = "info", skip_all, name = QueryServiceImpl_get_last_data_chunk_schema_parquet, fields(%dataset_ref))]
    async fn get_last_data_chunk_schema_parquet(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Type>, QueryError> {
        let resolved_dataset = self.resolve_dataset(dataset_ref).await?;

        use odf::dataset::MetadataChainExt;
        let Some(last_data_slice_hash) = resolved_dataset
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
            .map(|new_data| new_data.physical_hash)
        else {
            return Ok(None);
        };

        let data_url = Box::new(
            resolved_dataset
                .as_data_repo()
                .get_internal_url(&last_data_slice_hash)
                .await,
        );

        let ctx = self
            .session_context(QueryOptions {
                input_datasets: Some(BTreeMap::new()),
            })
            .await?;

        let object_store = ctx.runtime_env().object_store(&data_url)?;

        let data_path = object_store::path::Path::from_url_path(data_url.path()).int_err()?;

        let metadata = read_data_slice_metadata(object_store, data_path).await?;

        Ok(Some(metadata.file_metadata().schema().clone()))
    }

    #[tracing::instrument(level = "info", name = QueryServiceImpl_get_data, skip_all, fields(%dataset_ref))]
    async fn get_data(
        &self,
        dataset_ref: &odf::DatasetRef,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError> {
        // TODO: PERF: Limit push-down opportunity
        let (resolved_dataset, head, df) = self
            .single_dataset(dataset_ref, None, options.block_hash)
            .await?;

        Ok(GetDataResponse {
            df,
            dataset_handle: resolved_dataset.take_handle(),
            block_hash: head,
        })
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
    data_slice_store_path: object_store::path::Path,
) -> Result<Arc<ParquetMetaData>, QueryError> {
    let mut parquet_object_reader = ParquetObjectReader::new(object_store, data_slice_store_path);

    use datafusion::parquet::arrow::async_reader::AsyncFileReader;
    let metadata = parquet_object_reader
        .get_metadata(None)
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
            tables.retain(|tn| tn.0.len() != 1 || tn.0[0].as_ident() != Some(&cte.alias.name));
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
        SetExpr::Table(_)
        | SetExpr::Values(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_) => Ok(()),
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
