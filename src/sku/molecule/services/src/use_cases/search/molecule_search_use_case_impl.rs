// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::PaginationOpts;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::{
    molecule_announcement_search_schema as announcement_schema,
    molecule_data_room_entry_search_schema as dataroom_entry_schema,
    molecule_search_schema_common as molecule_schema,
    *,
};
use kamu_search::*;
use odf::utils::data::DataFrameExt;

use crate::{
    MoleculeAnnouncementsService,
    MoleculeGlobalDataRoomActivitiesService,
    map_molecule_search_filters,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeSearchUseCase)]
pub struct MoleculeSearchUseCaseImpl {
    catalog: dill::Catalog,
    global_data_room_activities_service: Arc<dyn MoleculeGlobalDataRoomActivitiesService>,
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
    search_service: Arc<dyn SearchService>,
    embeddings_provider: Arc<dyn EmbeddingsProvider>,
}

impl MoleculeSearchUseCaseImpl {
    async fn search_via_changelog(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        pagination: PaginationOpts,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        let search_entity_kinds = utils::get_search_entity_kinds(filters.as_ref());

        let (mut data_room_listing, mut announcement_activities_listing) = tokio::try_join!(
            self.get_global_data_room_activities_listing(
                molecule_subject,
                prompt,
                filters.clone(),
                &search_entity_kinds
            ),
            self.get_global_announcement_activities_listing(
                molecule_subject,
                prompt,
                filters,
                &search_entity_kinds
            ),
        )?;

        // Get the total count before pagination
        let total_count =
            data_room_listing.total_count + announcement_activities_listing.total_count;

        // Merge lists
        let mut list = {
            let mut v = Vec::with_capacity(total_count);
            v.append(&mut data_room_listing.list);
            v.append(&mut announcement_activities_listing.list);
            v
        };

        // Sort by event time descending
        list.sort_unstable_by_key(|item| std::cmp::Reverse(item.entity.event_time()));

        // Pagination
        let safe_offset = pagination.offset.min(total_count);
        list.drain(..safe_offset);
        list.truncate(pagination.limit);

        Ok(MoleculeSearchHitsListing { list, total_count })
    }

    fn project_global_data_room_activities(
        ledger: DataFrameExt,
    ) -> Result<DataFrameExt, DataFusionError> {
        // TODO: PERF: Re-assess implementation as it may be sub-optimal
        const RANK_COLUMN: &str = "__rank";

        let vocab = odf::metadata::DatasetVocabulary::default();
        // NOTE: Unlike other places, we set PK not by `path`, but by `ref`.
        let primary_key = ["ipnft_uid", "ref"]
            .into_iter()
            .map(|name| col(Column::from_name(name)))
            .collect::<Vec<_>>();

        let projection = ledger
            .window(vec![
                datafusion::functions_window::row_number::row_number()
                    .partition_by(primary_key)
                    .order_by(vec![
                        col(Column::from_name(&vocab.offset_column)).sort(false, false),
                    ])
                    .build()?
                    .alias(RANK_COLUMN),
            ])?
            .filter(
                col(Column::from_name(RANK_COLUMN))
                    .eq(lit(1))
                    .and(col(Column::from_name("activity_type")).not_eq(lit("removed"))),
            )?
            .without_columns(&[RANK_COLUMN])?;

        Ok(projection)
    }

    async fn get_global_data_room_activities_listing(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        search_entity_kinds: &HashSet<MoleculeSearchEntityKind>,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        if !search_entity_kinds.contains(&MoleculeSearchEntityKind::DataRoomActivity) {
            return Ok(MoleculeSearchHitsListing::empty());
        }

        // Get read access to global activities dataset
        let data_room_activities_reader = match self
            .global_data_room_activities_service
            .reader(&molecule_subject.account_name)
            .await
        {
            Ok(reader) => Ok(reader),

            // No activities dataset yet is fine, just return an empty listing
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) => {
                return Ok(MoleculeSearchHitsListing::empty());
            }

            Err(e) => Err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>(e)),
        }?;

        // Load raw ledger DF
        let maybe_df = data_room_activities_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_df else {
            return Ok(MoleculeSearchHitsListing::empty());
        };

        // Filtering
        let df = if let Some(filters) = filters {
            crate::utils::apply_molecule_filters_to_df(
                df,
                filters.by_ipnft_uids,
                filters.by_tags,
                filters.by_categories,
                filters.by_access_levels,
                filters.by_access_level_rules,
            )?
        } else {
            df
        };

        let df = Self::project_global_data_room_activities(df).int_err()?;

        let pattern = lit(format!("%{prompt}%"));
        let df = df.filter(col("description").ilike(pattern)).int_err()?;

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        // Convert to entities
        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeDataRoomActivity::from_changelog_entry_json(record)?;
                Ok(MoleculeSearchHit {
                    entity: MoleculeSearchHitEntity::DataRoomActivity(entity),
                    score: None,
                    explanation: None,
                })
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeSearchHitsListing { list, total_count })
    }

    async fn get_global_announcement_activities_listing(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        search_entity_kinds: &HashSet<MoleculeSearchEntityKind>,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        if !search_entity_kinds.contains(&MoleculeSearchEntityKind::Announcement) {
            return Ok(MoleculeSearchHitsListing::empty());
        }

        // Get read access to global announcements dataset
        let announcements_reader = match self
            .announcements_service
            .global_reader(&molecule_subject.account_name)
            .await
        {
            Ok(reader) => Ok(reader),

            // No announcements dataset yet is fine, just return empty listing
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) => {
                return Ok(MoleculeSearchHitsListing::empty());
            }

            Err(e) => Err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>(e)),
        }?;

        // Load raw ledger DF
        let maybe_df = announcements_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeSearchError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_df else {
            return Ok(MoleculeSearchHitsListing::empty());
        };

        // Filtering
        let df = if let Some(filters) = filters {
            crate::utils::apply_molecule_filters_to_df(
                df,
                filters.by_ipnft_uids,
                filters.by_tags,
                filters.by_categories,
                filters.by_access_levels,
                filters.by_access_level_rules,
            )?
        } else {
            df
        };

        let pattern = lit(format!("%{prompt}%"));
        let df = df
            .filter(
                col("headline")
                    .ilike(pattern.clone())
                    .or(col("body").ilike(pattern)),
            )
            .int_err()?;

        // Sorting will be done after merge
        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let list = records
            .into_iter()
            .map(|record| {
                let entity = MoleculeGlobalAnnouncement::from_changelog_entry_json(record)?;
                Ok(MoleculeSearchHit {
                    entity: MoleculeSearchHitEntity::Announcement(entity),
                    score: None,
                    explanation: None,
                })
            })
            .collect::<Result<Vec<_>, InternalError>>()?;

        Ok(MoleculeSearchHitsListing { list, total_count })
    }

    async fn search_via_search_index(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        prompt: &str,
        maybe_search_filters: Option<MoleculeSearchFilters>,
        pagination: PaginationOpts,
        enable_explain: bool,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        let prompt = prompt.trim();

        let ctx = SearchContext {
            catalog: &self.catalog,
            security: SearchSecurityContext::Restricted {
                current_principal_ids: vec![molecule_subject.account_id.to_string()],
            },
        };

        let entity_schemas = {
            let mut entity_schemas = vec![];

            let search_entity_kinds = utils::get_search_entity_kinds(maybe_search_filters.as_ref());

            if search_entity_kinds.contains(&MoleculeSearchEntityKind::DataRoomActivity) {
                entity_schemas.push(dataroom_entry_schema::SCHEMA_NAME);
            }

            if search_entity_kinds.contains(&MoleculeSearchEntityKind::Announcement) {
                entity_schemas.push(announcement_schema::SCHEMA_NAME);
            }

            entity_schemas
        };

        let maybe_filter = maybe_search_filters.and_then(|filters| {
            let and_clauses = map_molecule_search_filters(filters);
            if and_clauses.is_empty() {
                None
            } else {
                Some(SearchFilterExpr::and_clauses(and_clauses))
            }
        });

        tracing::debug!(
            empty_prompt = prompt.is_empty(),
            non_zero_offset = pagination.offset > 0,
            "Selecting search strategy"
        );

        let search_results = if prompt.is_empty() {
            self.search_via_listing(ctx, entity_schemas, maybe_filter, pagination)
                .await?
        } else if pagination.offset > 0 {
            self.search_via_text_search(
                ctx,
                prompt,
                entity_schemas,
                maybe_filter,
                pagination,
                enable_explain,
            )
            .await?
        } else {
            self.search_via_hybrid_search(
                ctx,
                prompt,
                entity_schemas,
                maybe_filter,
                pagination,
                enable_explain,
            )
            .await?
        };

        tracing::debug!(
            total_hits = search_results.total_hits,
            hits_count = search_results.hits.len(),
            "Search completed"
        );

        Ok(MoleculeSearchHitsListing {
            total_count: usize::try_from(search_results.total_hits.unwrap_or_default()).unwrap(),
            list: search_results
                .hits
                .into_iter()
                .map(|hit| match hit.schema_name {
                    dataroom_entry_schema::SCHEMA_NAME => {
                        // Extract "ipnft_uid" field from source
                        let ipnft_uid = if let Some(obj) = hit.source.as_object() {
                            obj.get(molecule_schema::fields::IPNFT_UID)
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string)
                                .ok_or_else(|| {
                                    InternalError::new(
                                        "Missing or invalid ipnft_uid field in search hit",
                                    )
                                })?
                        } else {
                            unreachable!()
                        };

                        // Recover data room entry
                        let data_room_entry =
                            MoleculeDataRoomEntry::from_search_index_json(hit.source)?;

                        // Convert it do dummy activity
                        let activity = MoleculeDataRoomActivity::from_data_room_operation(
                            0, /* unknown */
                            MoleculeDataRoomFileActivityType::Added,
                            data_room_entry,
                            ipnft_uid,
                        );

                        // Finally wrap as search hit
                        Ok(MoleculeSearchHit {
                            entity: MoleculeSearchHitEntity::DataRoomActivity(activity),
                            score: hit.score,
                            explanation: hit.explanation,
                        })
                    }
                    announcement_schema::SCHEMA_NAME => {
                        MoleculeGlobalAnnouncement::from_search_index_json(hit.id, hit.source).map(
                            |announcement| MoleculeSearchHit {
                                entity: MoleculeSearchHitEntity::Announcement(announcement),
                                score: hit.score,
                                explanation: hit.explanation,
                            },
                        )
                    }
                    _ => Err(InternalError::new(format!(
                        "Unexpected schema name: {schema_name}",
                        schema_name = hit.schema_name
                    ))),
                })
                .collect::<Result<Vec<_>, InternalError>>()?,
        })
    }

    async fn search_via_listing(
        &self,
        ctx: SearchContext<'_>,
        entity_schemas: Vec<&'static str>,
        maybe_filter: Option<SearchFilterExpr>,
        pagination: PaginationOpts,
    ) -> Result<SearchResponse, InternalError> {
        self.search_service
            .listing_search(
                ctx,
                ListingSearchRequest {
                    entity_schemas,
                    source: SearchRequestSourceSpec::All,
                    filter: maybe_filter,
                    sort: vec![SearchSortSpec::ByField {
                        field: molecule_schema::fields::EVENT_TIME,
                        direction: SearchSortDirection::Descending,
                        nulls_first: false,
                    }],
                    page: Some(pagination).into(),
                },
            )
            .await
            .int_err()
    }

    async fn search_via_text_search(
        &self,
        ctx: SearchContext<'_>,
        prompt: &str,
        entity_schemas: Vec<&'static str>,
        maybe_filter: Option<SearchFilterExpr>,
        pagination: PaginationOpts,
        enable_explain: bool,
    ) -> Result<SearchResponse, InternalError> {
        self.search_service
            .text_search(
                ctx,
                TextSearchRequest {
                    intent: TextSearchIntent::make_full_text(prompt),
                    entity_schemas,
                    source: SearchRequestSourceSpec::All,
                    filter: maybe_filter,
                    secondary_sort: vec![SearchSortSpec::ByField {
                        field: molecule_schema::fields::EVENT_TIME,
                        direction: SearchSortDirection::Descending,
                        nulls_first: false,
                    }],
                    page: Some(pagination).into(),
                    options: TextSearchOptions {
                        enable_explain,
                        ..TextSearchOptions::default()
                    },
                },
            )
            .await
            .int_err()
    }

    async fn search_via_hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        prompt: &str,
        entity_schemas: Vec<&'static str>,
        maybe_filter: Option<SearchFilterExpr>,
        pagination: PaginationOpts,
        enable_explain: bool,
    ) -> Result<SearchResponse, InternalError> {
        // Try to get embeddings for the prompt
        let maybe_prompt_vec = match self
            .embeddings_provider
            .provide_prompt_embeddings(prompt.to_string())
            .await
        {
            // Got embeddings => do hybrid search
            Ok(v) => Ok(Some(v)),

            // Encoder couldn't produce embeddings - degrade to text search
            Err(EmbeddingsProviderError::Unsupported) => Ok(None),

            // Bad error
            Err(e @ EmbeddingsProviderError::Internal(_)) => Err(e.int_err()),
        }?;

        // Hybrid search or degrade to text search
        if let Some(prompt_vec) = maybe_prompt_vec {
            self.search_service
                .hybrid_search(
                    ctx,
                    HybridSearchRequest {
                        prompt: prompt.to_string(),
                        prompt_embedding: prompt_vec,
                        entity_schemas,
                        source: SearchRequestSourceSpec::All,
                        filter: maybe_filter,
                        secondary_sort: vec![SearchSortSpec::ByField {
                            field: molecule_schema::fields::EVENT_TIME,
                            direction: SearchSortDirection::Descending,
                            nulls_first: false,
                        }],
                        limit: pagination.limit,
                        options: HybridSearchOptions {
                            enable_explain,
                            ..HybridSearchOptions::for_limit(pagination.limit)
                        },
                    },
                )
                .await
                .int_err()
        } else {
            self.search_via_text_search(
                ctx,
                prompt,
                entity_schemas,
                maybe_filter,
                pagination,
                enable_explain,
            )
            .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeSearchUseCase for MoleculeSearchUseCaseImpl {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeSearchUseCaseImpl_execute,
        skip_all,
        fields(prompt, ?mode, ?filters, ?pagination ))
    ]
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        mode: MoleculeSearchMode,
        prompt: &str,
        filters: Option<MoleculeSearchFilters>,
        pagination: PaginationOpts,
        enable_explain: bool,
    ) -> Result<MoleculeSearchHitsListing, MoleculeSearchError> {
        match mode {
            MoleculeSearchMode::ViaChangelog => {
                self.search_via_changelog(molecule_subject, prompt, filters, pagination)
                    .await
            }
            MoleculeSearchMode::ViaSearchIndex => {
                self.search_via_search_index(
                    molecule_subject,
                    prompt,
                    filters,
                    pagination,
                    enable_explain,
                )
                .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
