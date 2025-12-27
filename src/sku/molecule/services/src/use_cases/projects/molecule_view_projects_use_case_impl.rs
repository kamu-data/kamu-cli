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
use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::*;

use crate::MoleculeProjectsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeViewProjectsUseCase)]
pub struct MoleculeViewProjectsUseCaseImpl {
    projects_service: Arc<dyn MoleculeProjectsService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeViewProjectsUseCase for MoleculeViewProjectsUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeViewProjectsUseCaseImpl_execute, skip_all, fields(?pagination))]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        pagination: Option<PaginationOpts>,
    ) -> Result<MoleculeProjectListing, MoleculeViewProjectsError> {
        // Gain read access to projects dataset
        let projects_reader = self
            .projects_service
            .reader(&molecule_subject.account_name)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewProjectsError>)?;

        // Load changelog projection DF
        let maybe_changelog_df = projects_reader
            .changelog_projection_data_frame()
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeViewProjectsError>)?;

        // Empty? Return empty listing
        let Some(df) = maybe_changelog_df else {
            return Ok(MoleculeProjectListing::default());
        };

        // Get total count before pagination
        let total_count = df.clone().count().await.int_err()?;

        // Sort DF by ipnft_symbol
        use datafusion::logical_expr::col;
        let df = df
            .sort(vec![col("ipnft_symbol").sort(true, false)])
            .int_err()?;

        // Apply pagination
        let df = if let Some(pagination) = pagination {
            df.limit(pagination.offset, Some(pagination.limit))
                .int_err()?
        } else {
            df
        };

        // Convert to JSON AoS
        let records = df.collect_json_aos().await.int_err()?;

        // Map to entities
        let projects = records
            .into_iter()
            .map(MoleculeProject::from_json)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        // Return listing
        Ok(MoleculeProjectListing {
            list: projects,
            total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
