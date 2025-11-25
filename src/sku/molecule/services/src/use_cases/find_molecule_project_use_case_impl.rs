// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_accounts::LoggedAccount;
use kamu_core::auth::DatasetAction;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn FindMoleculeProjectUseCase)]
pub struct FindMoleculeProjectUseCaseImpl {
    project_service: Arc<dyn MoleculeProjectService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
#[common_macros::method_names_consts]
impl FindMoleculeProjectUseCase for FindMoleculeProjectUseCaseImpl {
    #[tracing::instrument(level = "debug", name = FindMoleculeProjectUseCaseImpl_execute, skip_all, fields(ipnft_uid))]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectEntity>, FindMoleculeProjectError> {
        use datafusion::logical_expr::{col, lit};

        let Some(df) = self
            .project_service
            .get_projects_snapshot(molecule_subject, DatasetAction::Read, false)
            .await
            .map_err(FindMoleculeProjectError::from)?
            .1
        else {
            return Ok(None);
        };

        let df = df.filter(col("ipnft_uid").eq(lit(ipnft_uid))).int_err()?;

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);
        let record = records.into_iter().next().unwrap();

        let entity = MoleculeProjectEntity::from_json(record).int_err()?;
        Ok(Some(entity))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
