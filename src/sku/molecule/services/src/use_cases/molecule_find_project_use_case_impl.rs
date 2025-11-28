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
#[dill::interface(dyn MoleculeFindProjectUseCase)]
pub struct MoleculeFindProjectUseCaseImpl {
    molecule_dataset_service: Arc<dyn MoleculeDatasetService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
#[common_macros::method_names_consts]
impl MoleculeFindProjectUseCase for MoleculeFindProjectUseCaseImpl {
    #[tracing::instrument(level = "debug", name = MoleculeFindProjectUseCaseImpl_execute, skip_all, fields(ipnft_uid))]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<Option<MoleculeProjectEntity>, MoleculeFindProjectError> {
        use datafusion::logical_expr::{col, lit};

        let Some(df) = self
            .molecule_dataset_service
            .get_projects_changelog_data_frame(molecule_subject, DatasetAction::Read, false)
            .await?
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

        let entity = MoleculeProjectEntity::from_json(record)?;
        Ok(Some(entity))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
