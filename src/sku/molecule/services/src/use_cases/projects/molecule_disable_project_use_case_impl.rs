// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::ResultIntoInternal;
use kamu_accounts::LoggedAccount;
use kamu_core::PushIngestResult;
use kamu_molecule_domain::*;
use messaging_outbox::{Outbox, OutboxExt};

use crate::MoleculeProjectsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeDisableProjectUseCase)]
pub struct MoleculeDisableProjectUseCaseImpl {
    projects_service: Arc<dyn MoleculeProjectsService>,
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
        source_event_time: Option<DateTime<Utc>>,
        ipnft_uid: String,
    ) -> Result<MoleculeProject, MoleculeDisableProjectError> {
        // Gain write access to projects dataset
        let projects_writer = self
            .projects_service
            .writer(&molecule_subject.account_name, false)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeDisableProjectError>)?;

        // Try to find the latest project state
        let maybe_last_changelog_entry = projects_writer
            .as_reader()
            .changelog_entry_by_ipnft_uid(&ipnft_uid)
            .await
            .map_err(MoleculeDatasetErrorExt::adapt::<MoleculeDisableProjectError>)?
            .map(MoleculeProjectChangelogEntry::from_json)
            .transpose()
            .int_err()?;

        // Not found? Sorry
        let Some(last_changelog_entry) = maybe_last_changelog_entry else {
            return Err(MoleculeDisableProjectError::ProjectNotFound(
                ProjectNotFoundError {
                    ipnft_uid: ipnft_uid.clone(),
                },
            ));
        };

        // Add retraction record to disable the project
        let new_changelog_record = MoleculeProjectChangelogInsertionRecord {
            op: odf::metadata::OperationType::Retract,
            event_time: source_event_time,
            payload: last_changelog_entry.payload,
        };

        let push_res = projects_writer
            .push_ndjson_data(new_changelog_record.to_bytes(), source_event_time)
            .await
            .int_err()?;

        match push_res {
            PushIngestResult::UpToDate => unreachable!(),
            PushIngestResult::Updated {
                system_time: insertion_system_time,
                ..
            } => {
                // Notify external listeners
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_MOLECULE_PROJECT_SERVICE,
                        MoleculeProjectMessage::disabled(
                            insertion_system_time,
                            molecule_subject.account_id.clone(),
                            new_changelog_record.payload.account_id.clone(),
                            new_changelog_record.payload.ipnft_uid.clone(),
                            new_changelog_record.payload.ipnft_symbol.clone(),
                        ),
                    )
                    .await
                    .int_err()?;

                Ok(MoleculeProject::from_payload(
                    new_changelog_record.payload,
                    insertion_system_time,
                    source_event_time.unwrap_or(insertion_system_time),
                )?)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
