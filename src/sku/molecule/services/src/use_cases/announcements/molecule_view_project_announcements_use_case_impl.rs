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
use internal_error::ResultIntoInternal;
use kamu_molecule_domain::*;

use crate::MoleculeAnnouncementsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewProjectAnnouncementsUseCase)]
pub struct MoleculeViewProjectAnnouncementsUseCaseImpl {
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
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
            kamu_molecule_domain::molecule_extra_data_fields_filter(
                f.by_tags,
                f.by_categories,
                f.by_access_levels,
            )
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
            .map(MoleculeAnnouncementChangelogEntry::from_json)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        Ok(MoleculeProjectAnnouncementListing {
            total_count,
            list: announcements,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
