// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::{
    molecule_announcement_search_schema as announcement_schema,
    molecule_search_schema_common as molecule_schema,
    *,
};
use kamu_search::*;

use crate::{MoleculeAnnouncementsService, map_molecule_announcements_filters_to_search, utils};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewGlobalAnnouncementsUseCase)]
pub struct MoleculeViewGlobalAnnouncementsUseCaseImpl {
    catalog: dill::Catalog,
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
    search_service: Arc<dyn SearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeViewGlobalAnnouncementsUseCaseImpl {
    async fn global_announcements_from_source(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalAnnouncementListing, MoleculeViewGlobalAnnouncementsError> {
        // Gain read access to global announcements dataset
        let global_announcements_reader = match self
            .announcements_service
            .global_reader(&molecule_subject.account_name)
            .await
        {
            Ok(reader) => reader,

            // No announcements dataset yet is fine, just return empty listing
            Err(RebacDatasetRefUnresolvedError::NotFound(_)) => {
                return Ok(MoleculeGlobalAnnouncementListing::default());
            }

            Err(e) => {
                return Err(MoleculeDatasetErrorExt::adapt::<
                    MoleculeViewGlobalAnnouncementsError,
                >(e));
            }
        };

        // Obtain raw ledger DF
        let maybe_df = global_announcements_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewGlobalAnnouncementsError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_df else {
            return Ok(MoleculeGlobalAnnouncementListing::default());
        };

        use datafusion::logical_expr::col;

        // Sort the df by offset descending
        // TODO: add col const from snapshot?
        let df = df.sort(vec![col("offset").sort(false, false)]).int_err()?;

        // Apply pagination
        let df = if let Some(pagination) = pagination {
            df.limit(pagination.offset, Some(pagination.limit))
                .int_err()?
        } else {
            df
        };

        let df = if let Some(filters) = filters {
            utils::apply_molecule_filters_to_df(
                df,
                None,
                filters.by_tags,
                filters.by_categories,
                filters.by_access_levels,
                filters.by_access_level_rules,
            )?
        } else {
            df
        };

        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let announcements = records
            .into_iter()
            .map(MoleculeGlobalAnnouncement::from_changelog_entry_json)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        Ok(MoleculeGlobalAnnouncementListing {
            total_count,
            list: announcements,
        })
    }

    async fn global_announcements_from_search(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        maybe_announcements_filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalAnnouncementListing, MoleculeViewGlobalAnnouncementsError> {
        let ctx = SearchContext {
            catalog: &self.catalog,
            security: SearchSecurityContext::Restricted {
                current_principal_ids: vec![molecule_subject.account_id.to_string()],
            },
        };

        let maybe_filter = maybe_announcements_filters.and_then(|filters| {
            let and_clauses = map_molecule_announcements_filters_to_search(filters);
            if and_clauses.is_empty() {
                None
            } else {
                Some(SearchFilterExpr::and_clauses(and_clauses))
            }
        });

        let search_results = self
            .search_service
            .listing_search(
                ctx,
                ListingSearchRequest {
                    entity_schemas: vec![announcement_schema::SCHEMA_NAME],
                    source: SearchRequestSourceSpec::All,
                    filter: maybe_filter,
                    sort: sort!(molecule_schema::fields::SYSTEM_TIME, desc),
                    page: pagination.into(),
                },
            )
            .await
            .int_err()?;

        Ok(MoleculeGlobalAnnouncementListing {
            total_count: usize::try_from(search_results.total_hits.unwrap_or_default()).unwrap(),
            list: search_results
                .hits
                .into_iter()
                .map(|hit| MoleculeGlobalAnnouncement::from_search_index_json(hit.id, hit.source))
                .collect::<Result<Vec<_>, InternalError>>()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewGlobalAnnouncementsUseCase for MoleculeViewGlobalAnnouncementsUseCaseImpl {
    #[tracing::instrument(
        name = MoleculeViewGlobalAnnouncementsUseCaseImpl_execute,
        level = "debug",
        skip_all
    )]
    async fn execute(
        &self,
        molecule_subject: &kamu_accounts::LoggedAccount,
        mode: MoleculeViewGlobalAnnouncementsMode,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalAnnouncementListing, MoleculeViewGlobalAnnouncementsError> {
        match mode {
            MoleculeViewGlobalAnnouncementsMode::LatestSource => {
                self.global_announcements_from_source(molecule_subject, filters, pagination)
                    .await
            }
            MoleculeViewGlobalAnnouncementsMode::LatestProjection => {
                self.global_announcements_from_search(molecule_subject, filters, pagination)
                    .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
