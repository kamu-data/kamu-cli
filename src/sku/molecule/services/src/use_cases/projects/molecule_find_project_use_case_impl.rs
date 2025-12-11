// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::*;

use crate::MoleculeProjectsDatasetService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeFindProjectUseCase)]
pub struct MoleculeFindProjectUseCaseImpl {
    molecule_projects_dataset_service: Arc<dyn MoleculeProjectsDatasetService>,
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
    ) -> Result<Option<MoleculeProject>, MoleculeFindProjectError> {
        let maybe_project_json = self
            .molecule_projects_dataset_service
            .request_read_of_projects_dataset(&molecule_subject.account_name)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeFindProjectError>)?
            .try_get_changelog_projection_entry("account_id", "ipnft_uid", &ipnft_uid)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeFindProjectError>)?;

        Ok(maybe_project_json
            .map(MoleculeProject::from_json)
            .transpose()?)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
