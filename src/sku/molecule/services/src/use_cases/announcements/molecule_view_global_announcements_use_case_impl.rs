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
use kamu_auth_rebac::RebacDatasetRefUnresolvedError;
use kamu_molecule_domain::*;

use crate::MoleculeAnnouncementsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewGlobalAnnouncementsUseCase)]
pub struct MoleculeViewGlobalAnnouncementsUseCaseImpl {
    announcements_service: Arc<dyn MoleculeAnnouncementsService>,
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
            crate::utils::apply_molecule_filters_to_df(
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
        filters: Option<MoleculeAnnouncementsFilters>,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeGlobalAnnouncementListing, MoleculeViewGlobalAnnouncementsError> {
        self.global_announcements_from_source(molecule_subject, filters, pagination)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
