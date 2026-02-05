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

use datafusion::prelude::*;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_core::*;
use kamu_datasets::{
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionAuthorizerExt as _,
    DatasetRegistry,
    ResolvedDataset,
};
use odf::utils::data::DataFrameExt;

use crate::utils::docker_images;
use crate::{QueryDatasetDataUseCaseImpl, SessionContextBuilder};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn QueryService)]
pub struct QueryServiceImpl {
    session_context_builder: Arc<SessionContextBuilder>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl QueryServiceImpl {
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
                                source_dataset: None, // not pre-resolved yet
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
                                source_dataset: Some(resolved_dataset),
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
        resolved_dataset: &ResolvedDataset,
        last_records_to_consider: Option<u64>,
        head: Option<odf::Multihash>,
    ) -> Result<(odf::Multihash, Option<DataFrameExt>), QueryError> {
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
            .session_context_builder
            .session_context(QueryOptions {
                input_datasets: Some(BTreeMap::from([(
                    resolved_dataset.get_id().clone(),
                    QueryOptionsDataset {
                        alias: resolved_dataset.get_alias().to_string(),
                        block_hash: Some(head.clone()),
                        hints: DatasetQueryHints {
                            handle: Some(resolved_dataset.get_handle().clone()),
                            source_dataset: Some(resolved_dataset.clone()),
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
            Ok((head, None))
        } else {
            Ok((head, Some(df.into())))
        }
    }

    async fn session_context(
        &self,
        options: QueryOptions,
    ) -> Result<SessionContext, InternalError> {
        let ctx = self
            .session_context_builder
            .session_context(options)
            .await?;

        // NOTE: Ugly way to avoid circular dependencies in a DI tree
        let rebac_dataset_registry_facade = Arc::new(RebacDatasetRegistryFacadeImpl::new(
            self.dataset_registry.clone(),
            self.dataset_action_authorizer.clone(),
        ));
        let self_as_arc = Arc::new(Self {
            session_context_builder: self.session_context_builder.clone(),
            dataset_registry: self.dataset_registry.clone(),
            dataset_action_authorizer: self.dataset_action_authorizer.clone(),
        });
        let query_dataset_data_use_case = Arc::new(QueryDatasetDataUseCaseImpl::new(
            self_as_arc,
            self.dataset_registry.clone(),
            rebac_dataset_registry_facade,
        ));

        kamu_datafusion_udf::ToTableUdtf::register(&ctx, query_dataset_data_use_case);

        Ok(ctx)
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
                            source_dataset: None, // not pre-resolved yet
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
        fields(hdl=%source.get_handle(), %skip, %limit)
    )]
    async fn tail(
        &self,
        source: ResolvedDataset,
        skip: u64,
        limit: u64,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError> {
        let (head, df) = self
            .single_dataset(&source, Some(skip + limit), options.block_hash)
            .await?;

        // Our custom catalog provider resolves schemas lazily, so the dataset will be
        // found even if it's empty and its schema will be empty, but we decide not to
        // propagate this special case to the users and return `None` instead
        let Some(df) = df else {
            return Ok(GetDataResponse {
                df: None,
                source,
                block_hash: head,
            });
        };

        use odf::dataset::MetadataChainExt;
        let vocab: odf::metadata::DatasetVocabulary = source
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
            source,
            block_hash: head,
        })
    }

    #[tracing::instrument(level = "info", name = QueryServiceImpl_get_data, skip_all, fields(hdl=%source.get_handle()))]
    async fn get_data(
        &self,
        source: ResolvedDataset,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError> {
        // TODO: PERF: Limit push-down opportunity
        let (head, df) = self
            .single_dataset(&source, None, options.block_hash)
            .await?;

        Ok(GetDataResponse {
            df,
            source,
            block_hash: head,
        })
    }

    #[tracing::instrument(level = "info", name = QueryServiceImpl_get_data_multi, skip_all)]
    async fn get_data_multi(
        &self,
        sources: Vec<ResolvedDataset>,
    ) -> Result<Vec<GetDataResponse>, QueryError> {
        let mut sources_with_head = Vec::new();

        // TODO: consider vectorized head resolution
        for source in sources {
            let head = source
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
                .int_err()?;
            sources_with_head.push((source, head));
        }

        let input_datasets = sources_with_head
            .iter()
            .map(|(ds, head)| {
                (
                    ds.get_id().clone(),
                    QueryOptionsDataset {
                        alias: ds.get_alias().to_string(),
                        block_hash: Some(head.clone()),
                        hints: DatasetQueryHints {
                            handle: Some(ds.get_handle().clone()),
                            source_dataset: Some(ds.clone()),
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

        let mut results = Vec::new();

        for (source, head) in sources_with_head {
            let df = ctx
                .table(TableReference::bare(source.get_alias().to_string()))
                .await?;

            let res = if df.schema().fields().is_empty() {
                GetDataResponse {
                    df: None,
                    source,
                    block_hash: head,
                }
            } else {
                GetDataResponse {
                    df: Some(df.into()),
                    source,
                    block_hash: head,
                }
            };

            results.push(res);
        }

        Ok(results)
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

    #[tracing::instrument(level = "info", name = QueryServiceImpl_get_changelog_projection, skip_all, fields(hdl=%source.get_handle()))]
    async fn get_changelog_projection(
        &self,
        source: ResolvedDataset,
        options: GetChangelogProjectionOptions,
    ) -> Result<GetDataResponse, QueryError> {
        let (head, maybe_df) = self
            .single_dataset(&source, None, options.block_hash)
            .await?;

        let Some(df) = maybe_df else {
            // Dataset has no schema yet.
            return Ok(GetDataResponse {
                df: None,
                source,
                block_hash: head,
            });
        };

        enum PrimaryKeyValue {
            Preresolved(Vec<String>),
            NeedToScan,
        }
        enum DatasetVocabularyValue {
            Preresolved(odf::metadata::DatasetVocabulary),
            NeedToScan,
        }

        let mut set_vocab_visitor = odf::dataset::SearchSetVocabVisitor::new();
        // TODO: Remove these visitors once schema contains primary keys -->
        let mut active_polling_source_visitor =
            odf::dataset::SearchActivePollingSourceVisitor::new(source.get_kind());
        let mut active_push_sources_visitor =
            odf::dataset::SearchActivePushSourcesVisitor::new(source.get_kind());
        // <--

        let mut visitors =
            Vec::<&mut dyn odf::dataset::MetadataChainVisitor<Error = _>>::with_capacity(3);

        let primary_key_value = if let Some(value) = options.hints.primary_key {
            PrimaryKeyValue::Preresolved(value)
        } else {
            visitors.push(&mut active_polling_source_visitor);
            visitors.push(&mut active_push_sources_visitor);
            PrimaryKeyValue::NeedToScan
        };
        let dataset_vocabulary_value = if let Some(value) = options.hints.dataset_vocabulary {
            DatasetVocabularyValue::Preresolved(value)
        } else {
            visitors.push(&mut set_vocab_visitor);
            DatasetVocabularyValue::NeedToScan
        };

        use odf::dataset::MetadataChainExt;

        source
            .as_metadata_chain()
            .accept_by_interval(&mut visitors, Some(&head), None)
            .await
            .int_err()?;

        let primary_key = match primary_key_value {
            PrimaryKeyValue::Preresolved(value) => value,
            PrimaryKeyValue::NeedToScan => {
                // TODO: Revisit once schema contains primary keys
                let merge_strategy = if let Some(event) = active_polling_source_visitor.into_event()
                {
                    // Use the active polling source if any.
                    Some(event.merge)
                } else if let Some(event) = active_push_sources_visitor.into_events().pop() {
                    // Use the (!) last active push source, if any.
                    // It's OK until the schema starts containing primary keys.
                    Some(event.merge)
                } else {
                    None
                };

                let Some(primary_key) =
                    merge_strategy.and_then(odf::metadata::MergeStrategy::primary_key)
                else {
                    return Err(DatasetHasNoPrimaryKeysError {
                        dataset_handle: source.take_handle(),
                    }
                    .int_err()
                    .into());
                };

                primary_key
            }
        };
        let dataset_vocabulary = match dataset_vocabulary_value {
            DatasetVocabularyValue::Preresolved(value) => value,
            DatasetVocabularyValue::NeedToScan => {
                set_vocab_visitor.into_event().unwrap_or_default().into()
            }
        };

        let projection_df =
            odf::utils::data::changelog::project(df, &primary_key, &dataset_vocabulary)?;

        Ok(GetDataResponse {
            df: Some(projection_df),
            source,
            block_hash: head,
        })
    }
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
        | SetExpr::Delete(_)
        | SetExpr::Merge(_) => Ok(()),
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
