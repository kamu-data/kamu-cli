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
use kamu_molecule_domain::{
    molecule_announcement_search_schema as announcement_schema,
    molecule_search_schema_common as molecule_schema,
    *,
};
use kamu_search::*;

use crate::{MoleculeAnnouncementsService, map_molecule_announcements_filters_to_search};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewProjectAnnouncementsUseCase)]
pub struct MoleculeViewProjectAnnouncementsUseCaseImpl {
    catalog: dill::Catalog,
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
    full_text_search_service: Arc<dyn FullTextSearchService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeViewProjectAnnouncementsUseCaseImpl {
    async fn project_announcements_from_source(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectAnnouncementListing, MoleculeViewProjectAnnouncementsError> {
        // Gain read access to project's announcements dataset
        let projects_announcements_reader = self
            .announcements_service
            .project_reader(molecule_project)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewProjectAnnouncementsError>)?;

        // Obtain raw ledger DF
        let maybe_df = projects_announcements_reader
            .raw_ledger_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewProjectAnnouncementsError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_df else {
            return Ok(MoleculeProjectAnnouncementListing::default());
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

        let maybe_filter = filters.and_then(|f| {
            utils::molecule_fields_filter(None, f.by_tags, f.by_categories, f.by_access_levels)
        });
        let df = if let Some(filters) = maybe_filter {
            kamu_datasets_services::utils::DataFrameExtraDataFieldsFilterApplier::apply(df, filters)
                .int_err()?
        } else {
            df
        };

        let records = df.collect_json_aos().await.int_err()?;
        let total_count = records.len();

        let announcements = records
            .into_iter()
            .map(MoleculeAnnouncement::from_changelog_entry_json)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        Ok(MoleculeProjectAnnouncementListing {
            total_count,
            list: announcements,
        })
    }

    async fn project_announcements_from_search(
        &self,
        molecule_project: &MoleculeProject,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectAnnouncementListing, MoleculeViewProjectAnnouncementsError> {
        let ctx = FullTextSearchContext {
            catalog: &self.catalog,
        };

        let filter = {
            let mut and_clauses = vec![];

            // ipnft_uid equality
            and_clauses.push(field_eq_str(
                molecule_schema::fields::IPNFT_UID,
                &molecule_project.ipnft_uid,
            ));

            // filters by categories, tags, access levels
            if let Some(filters) = filters {
                and_clauses.extend(map_molecule_announcements_filters_to_search(filters));
            }

            FullTextSearchFilterExpr::and_clauses(and_clauses)
        };

        let search_results = self
            .full_text_search_service
            .search(
                ctx,
                FullTextSearchRequest {
                    query: None, // no textual query, just filtering
                    entity_schemas: vec![announcement_schema::SCHEMA_NAME],
                    source: FullTextSearchRequestSourceSpec::All,
                    filter: Some(filter),
                    sort: sort!(molecule_schema::fields::SYSTEM_TIME, desc),
                    page: pagination.into(),
                    options: FullTextSearchOptions::default(),
                },
            )
            .await?;

        Ok(MoleculeProjectAnnouncementListing {
            total_count: usize::try_from(search_results.total_hits).unwrap(),
            list: search_results
                .hits
                .into_iter()
                .map(|hit| MoleculeAnnouncement::from_search_index_json(hit.id, hit.source))
                .collect::<Result<Vec<_>, InternalError>>()?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewProjectAnnouncementsUseCase for MoleculeViewProjectAnnouncementsUseCaseImpl {
    #[tracing::instrument(
        name = MoleculeViewProjectAnnouncementsUseCaseImpl_execute,
        level = "debug",
        skip_all
    )]
    async fn execute(
        &self,
        molecule_project: &MoleculeProject,
        mode: MoleculeViewProjectAnnouncementsMode,
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectAnnouncementListing, MoleculeViewProjectAnnouncementsError> {
        match mode {
            MoleculeViewProjectAnnouncementsMode::LatestSource => {
                self.project_announcements_from_source(molecule_project, filters, pagination)
                    .await
            }

            MoleculeViewProjectAnnouncementsMode::LatestProjection => {
                self.project_announcements_from_search(molecule_project, filters, pagination)
                    .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
