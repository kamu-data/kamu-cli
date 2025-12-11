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
use kamu_core::auth::DatasetAction;
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
        let (_, project) = self
            .molecule_projects_dataset_service
            .get_project_changelog_entry(molecule_subject, DatasetAction::Read, false, &ipnft_uid)
            .await?;

        Ok(project)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
