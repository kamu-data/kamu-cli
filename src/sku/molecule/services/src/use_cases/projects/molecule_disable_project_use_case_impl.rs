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
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::MoleculeProjectsDatasetService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeDisableProjectUseCase)]
pub struct MoleculeDisableProjectUseCaseImpl {
    molecule_projects_dataset_service: Arc<dyn MoleculeProjectsDatasetService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl MoleculeDisableProjectUseCase for MoleculeDisableProjectUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = MoleculeDisableProjectUseCaseImpl_execute,
        skip_all,
        fields(ipnft_uid)
    )]
    async fn execute(
        &self,
        molecule_subject: &LoggedAccount,
        ipnft_uid: String,
    ) -> Result<MoleculeProject, MoleculeDisableProjectError> {
        let now = chrono::Utc::now();

        let projects_write_accessor = self
            .molecule_projects_dataset_service
            .request_write_of_projects_dataset(&molecule_subject.account_name, false)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeDisableProjectError>)?;

        let maybe_project = projects_write_accessor
            .as_read_accessor()
            .try_get_changelog_projection_entry("account_id", "ipnft_uid", &ipnft_uid)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeDisableProjectError>)?
            .map(MoleculeProject::from_json)
            .transpose()?;

        let Some(mut project) = maybe_project else {
            return Err(MoleculeDisableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };
        project.system_time = now;
        project.event_time = now;

        let changelog_record =
            project.as_changelog_record(u8::from(odf::metadata::OperationType::Retract));

        projects_write_accessor
            .push_ndjson_data(changelog_record.to_bytes(), Some(now))
            .await
            .int_err()?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_MOLECULE_PROJECT_SERVICE,
                MoleculeProjectMessage::disabled(
                    now,
                    molecule_subject.account_id.clone(),
                    project.account_id.clone(),
                    project.ipnft_uid.clone(),
                    project.ipnft_symbol.clone(),
                ),
            )
            .await
            .int_err()?;

        Ok(project)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
