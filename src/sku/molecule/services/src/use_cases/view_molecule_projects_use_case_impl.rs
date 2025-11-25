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
use kamu_core::auth::DatasetAction;
use kamu_molecule_domain::MoleculeProjectService;

use crate::domain::{
    MoleculeProjectListing,
    ViewMoleculeProjectsError,
    ViewMoleculeProjectsUseCase,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ViewMoleculeProjectsUseCase)]
pub struct ViewMoleculeProjectsUseCaseImpl {
    project_service: Arc<dyn MoleculeProjectService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ViewMoleculeProjectsUseCaseImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ViewMoleculeProjectsUseCase for ViewMoleculeProjectsUseCaseImpl {
    async fn execute(
        &self,
        molecule_subject: LoggedAccount,
        pagination: PaginationOpts,
    ) -> Result<MoleculeProjectListing, ViewMoleculeProjectsError> {
        use datafusion::logical_expr::col;

        let Some(df) = self
            .project_service
            .get_projects_snapshot(molecule_subject, DatasetAction::Read, false)
            .await
            .map_err(ViewMoleculeProjectsError::from)?
            .1
        else {
            return Ok(MoleculeProjectListing::default());
        };

        let total_count = df.clone().count().await.int_err()?;
        let df = df
            .sort(vec![col("ipnft_symbol").sort(true, false)])
            .int_err()?
            .limit(pagination.offset, Some(pagination.limit))
            .int_err()?;

        let records = df.collect_json_aos().await.int_err()?;

        Ok(MoleculeProjectListing {
            records,
            total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
